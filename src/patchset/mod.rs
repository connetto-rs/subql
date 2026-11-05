//! Apply client-uploaded SQLite session patchsets against a target
//! database via diesel.
//!
//! This module is the "digest" side of the CDC round-trip for
//! `connetto-rs`-shaped topologies: a client uploads a batch of local
//! mutations as an SQLite session-extension patchset, and the server
//! executes each op against Postgres or MySQL with native diesel binds.
//!
//! SQLite has no boolean type, so a Postgres `BOOLEAN` or MySQL
//! `TINYINT(1)` column round-trips through the wire as
//! `Value::Integer(0 | 1)`. Without dispatch, diesel would bind that
//! integer as `BigInt`, and the target rejects the coerce-free INSERT.
//! Each per-backend adapter carries the catalog and rewrites integer
//! binds for `bool`-shaped columns to native `bool` binds so no
//! `CAST` wrapper is emitted.
//!
//! # Extension
//!
//! [`PgAdapter`] and [`MysqlAdapter`] are plain structs that implement
//! [`sqlite_diff_rs::Adapter`]. For Postgres user-defined types (custom
//! enums, domains, and the like) that [`PgAdapter`] cannot bind on its
//! own, wrap it in a [`pg::CustomTypePgAdapter`] and register a
//! [`pg::PgCustomBinder`] per type, building the native bind with
//! [`pg::bind_as`]. For anything more exotic, wrap either adapter with a
//! bespoke [`sqlite_diff_rs::Adapter`] impl that intercepts the columns
//! it owns and delegates the rest, or roll a whole new adapter.
//!
//! # Scope
//!
//! `INTEGER`, `TEXT`, `REAL`, and `BLOB`/`BYTEA` already work through
//! [`sqlite_diff_rs::DefaultBinder`] on both backends. [`PgAdapter`]
//! additionally dispatches `BOOLEAN`, `UUID`, `NUMERIC`/`DECIMAL`, the
//! temporals (`TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `TIME`), and
//! `JSON`/`JSONB` natively, with [`pg::CustomTypePgAdapter`] adding
//! caller-registered `ENUM` and `DOMAIN` binds. [`MysqlAdapter`]
//! dispatches `BOOLEAN`.

use alloc::string::String;
use alloc::vec::Vec;
use core::hash::Hash;

use diesel::backend::Backend;
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, Binary, Double, HasSqlType, Text};
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{Adapter, ApplyOps, ChangeSet, ColumnNames, PatchSet, SchemaWithPK};

use crate::drift::RefusalView;
use crate::Incompatibility;

// Async execution driver, present only when an async apply feature (which
// enables `sqlite-diff-rs/diesel-async`) is on.
#[cfg(any(
    feature = "apply-patchset-postgres-async",
    feature = "apply-patchset-mysql-async"
))]
use sqlite_diff_rs::ApplyOpsAsync;
#[cfg(feature = "apply-patchset-postgres")]
pub mod pg;
#[cfg(feature = "apply-patchset-postgres")]
pub use pg::{
    bind_array_as, bind_as, CustomTypePgAdapter, PgAdapter, PgArray, PgCustomBinder, PgVector,
    VectorType,
};

#[cfg(feature = "apply-patchset-mysql")]
pub mod mysql;
#[cfg(feature = "apply-patchset-mysql")]
pub use mysql::MysqlAdapter;

#[cfg(feature = "apply-patchset-sqlite")]
pub mod sqlite;
#[cfg(feature = "apply-patchset-sqlite")]
pub use sqlite::SqliteAdapter;

#[cfg(any(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-mysql",
    feature = "apply-patchset-sqlite"
))]
pub(crate) mod columns;

pub(crate) mod catalog_apply;
#[cfg(any(
    feature = "apply-patchset-postgres-async",
    feature = "apply-patchset-mysql-async"
))]
pub use catalog_apply::apply_diffset_bytes_async_with_catalog;
pub use catalog_apply::apply_diffset_bytes_with_catalog;

/// The error an upload apply refuses with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ApplyError {
    /// The upload names a table the refused view has refused, so the
    /// whole upload is refused before any op applies.
    #[error("upload refused: table `{table}` is refused: {reasons:?}")]
    RefusedTable {
        /// The table the upload names.
        table: String,
        /// Every reason the table is refused.
        reasons: Vec<Incompatibility>,
    },
    /// The apply failed at the diesel layer, with the transaction rolled
    /// back.
    #[error(transparent)]
    Diesel(diesel::result::Error),
}

impl From<diesel::result::Error> for ApplyError {
    fn from(error: diesel::result::Error) -> Self {
        Self::Diesel(error)
    }
}

/// Refuse the whole upload when any table it names is refused.
fn check_refusals<'n, R: RefusalView>(
    refusals: &R,
    names: impl Iterator<Item = &'n str>,
) -> Result<(), ApplyError> {
    for name in names {
        if let Some(reasons) = refusals.refusal(name) {
            return Err(ApplyError::RefusedTable {
                table: String::from(name),
                reasons,
            });
        }
    }
    Ok(())
}

// Engine entry point (generic over any diesel Backend)

impl<E, I, DB> crate::SubscriptionEngine<E, I, DB>
where
    E: crate::backend::CdcEvent,
    E::Backend: crate::compiler::literals::SqlLiteralParse,
    I: crate::IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Apply a client-uploaded SQLite session patchset against `conn`
    /// using `adapter` for native diesel bind dispatch. The batch runs
    /// inside one diesel transaction: either every op commits or none
    /// do. Returns the summed affected-row count.
    ///
    /// Generic over any diesel [`Backend`]. Concrete adapters shipped
    /// with subql are [`PgAdapter`] and [`MysqlAdapter`].
    ///
    /// `refusals` is consulted before any op applies: a batch naming a
    /// table it refuses is refused as a whole with
    /// [`ApplyError::RefusedTable`].
    ///
    /// # Errors
    ///
    /// [`ApplyError::RefusedTable`] when a table the batch names is
    /// refused. [`ApplyError::Diesel`] wrapping the first
    /// [`diesel::result::Error`] any op produces (with the transaction
    /// rolled back).
    pub fn apply_patchset<DBend, Conn, T, S, B, A, R>(
        &self,
        patchset: &PatchSet<T, S, B>,
        refusals: &R,
        conn: &mut Conn,
        adapter: &A,
    ) -> Result<usize, ApplyError>
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel::Connection<Backend = DBend>,
        A: Adapter<DBend, S, B> + Send + Sync,
        R: RefusalView,
        T: SchemaWithPK + ColumnNames + Sync,
        S: AsRef<str> + Clone + Hash + Eq + Sync,
        B: AsRef<[u8]> + Clone + Hash + Eq + Sync,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        check_refusals(refusals, patchset.iter().map(|op| op.table().name()))?;
        patchset
            .iter()
            .map(|op| op.with_adapter::<DBend, _>(adapter))
            .apply_transactional(conn)
            .map_err(ApplyError::from)
    }

    /// Apply a client-uploaded SQLite session changeset against `conn`
    /// using `adapter`. The changeset counterpart to
    /// [`Self::apply_patchset`].
    ///
    /// The changeset format carries the old and new value of every
    /// changed column, so this path can apply a primary-key-changing
    /// UPDATE (`SET pk = new WHERE pk = old`) that a patchset cannot
    /// represent. The batch runs in one transaction.
    ///
    /// `refusals` is consulted before any op applies: a batch naming a
    /// table it refuses is refused as a whole with
    /// [`ApplyError::RefusedTable`].
    ///
    /// # Errors
    ///
    /// [`ApplyError::RefusedTable`] when a table the batch names is
    /// refused. [`ApplyError::Diesel`] wrapping the first
    /// [`diesel::result::Error`] any op produces, with the transaction
    /// rolled back.
    pub fn apply_changeset<DBend, Conn, T, S, B, A, R>(
        &self,
        changeset: &ChangeSet<T, S, B>,
        refusals: &R,
        conn: &mut Conn,
        adapter: &A,
    ) -> Result<usize, ApplyError>
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel::Connection<Backend = DBend>,
        A: Adapter<DBend, S, B> + Send + Sync,
        R: RefusalView,
        T: SchemaWithPK + ColumnNames + Sync,
        S: AsRef<str> + Clone + Hash + Eq + core::fmt::Debug + Sync,
        B: AsRef<[u8]> + Clone + Hash + Eq + core::fmt::Debug + Sync,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        check_refusals(refusals, changeset.iter().map(|op| op.table().name()))?;
        changeset
            .iter()
            .map(|op| op.with_adapter::<DBend, _>(adapter))
            .apply_transactional(conn)
            .map_err(ApplyError::from)
    }

    /// Apply a client-uploaded SQLite session diffset, still in its raw
    /// wire bytes, against `conn` using `adapter`. This is the production
    /// inbound entry point for the CDC round trip: a client uploads the
    /// bytes the SQLite session extension emitted, and subql parses them,
    /// reconstructs the batch, and applies it in one transaction with
    /// native diesel binds.
    ///
    /// Accepts either a patchset (marker `P`) or a changeset (marker `T`)
    /// and dispatches on the marker. A changeset carries the old and new
    /// value of every changed column, so a changeset upload can apply a
    /// primary-key-changing UPDATE (`SET pk = new WHERE pk = old`) that a
    /// patchset cannot represent. It is the byte-level counterpart to
    /// [`Self::apply_patchset`] and [`Self::apply_changeset`], which take
    /// an already reconstructed diffset.
    ///
    /// For either format the table shape (column order and primary-key
    /// indices) is resolved from subql's catalog by the table name each op
    /// carries, not from the flags embedded in the uploaded bytes, so a
    /// client cannot steer the server's WHERE clause.
    ///
    /// `refusals` is consulted before any op applies: an upload naming a
    /// table it refuses is refused as a whole with
    /// [`ApplyError::RefusedTable`].
    ///
    /// # Errors
    ///
    /// [`ApplyError::RefusedTable`] when an op names a refused table.
    /// [`ApplyError::Diesel`] wrapping a
    /// [`QueryBuilderError`](diesel::result::Error::QueryBuilderError)
    /// when the bytes fail to parse or name a table absent from the
    /// catalog, or the first bind or execution error any op produces,
    /// with the whole transaction rolled back.
    pub fn apply_diffset_bytes<DBend, Conn, A, R>(
        &self,
        bytes: &[u8],
        refusals: &R,
        conn: &mut Conn,
        adapter: &A,
    ) -> Result<usize, ApplyError>
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel::Connection<Backend = DBend>,
        A: Adapter<DBend, String, Vec<u8>> + Send + Sync,
        R: RefusalView,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        apply_diffset_bytes_with_catalog(self.database(), bytes, refusals, conn, adapter)
    }

    /// Async peer of [`Self::apply_patchset`], driven by
    /// [`diesel_async`]. Same contract (one transaction, summed
    /// affected-row count), bounded on an
    /// [`AsyncConnection`](diesel_async::AsyncConnection). Reuses the
    /// shipped [`PgAdapter`] and [`MysqlAdapter`] unchanged.
    ///
    /// `refusals` is consulted before any op applies: a batch naming a
    /// table it refuses is refused as a whole with
    /// [`ApplyError::RefusedTable`].
    ///
    /// # Errors
    ///
    /// [`ApplyError::RefusedTable`] when a table the batch names is
    /// refused. [`ApplyError::Diesel`] wrapping the first
    /// [`diesel::result::Error`] any op produces (with the transaction
    /// rolled back).
    #[cfg(any(
        feature = "apply-patchset-postgres-async",
        feature = "apply-patchset-mysql-async"
    ))]
    pub fn apply_patchset_async<'a, DBend, Conn, T, S, B, A, R>(
        &self,
        patchset: &'a PatchSet<T, S, B>,
        refusals: &R,
        conn: &'a mut Conn,
        adapter: &'a A,
    ) -> impl core::future::Future<Output = Result<usize, ApplyError>> + Send + 'a
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel_async::AsyncConnection<Backend = DBend>,
        A: Adapter<DBend, S, B> + Send + Sync + 'a,
        R: RefusalView,
        T: SchemaWithPK + ColumnNames + Send + Sync + 'a,
        S: AsRef<str> + Clone + Hash + Eq + Send + Sync + 'a,
        B: AsRef<[u8]> + Clone + Hash + Eq + Send + Sync + 'a,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        // The check and the `Send` proof run up front, so the returned
        // future borrows neither the non-`Sync` engine nor the view.
        let refused = check_refusals(refusals, patchset.iter().map(|op| op.table().name()));
        let apply = patchset
            .iter()
            .map(|op| op.with_adapter::<DBend, _>(adapter))
            .apply_transactional_async(conn);
        async move {
            refused?;
            apply.await.map_err(ApplyError::from)
        }
    }

    /// Async peer of [`Self::apply_changeset`], driven by
    /// [`diesel_async`]. The changeset format carries the old and new
    /// value of every column, so this path can apply a
    /// primary-key-changing UPDATE a patchset cannot represent. One
    /// transaction.
    ///
    /// `refusals` is consulted before any op applies: a batch naming a
    /// table it refuses is refused as a whole with
    /// [`ApplyError::RefusedTable`].
    ///
    /// # Errors
    ///
    /// [`ApplyError::RefusedTable`] when a table the batch names is
    /// refused. [`ApplyError::Diesel`] wrapping the first
    /// [`diesel::result::Error`] any op produces, with the transaction
    /// rolled back.
    #[cfg(any(
        feature = "apply-patchset-postgres-async",
        feature = "apply-patchset-mysql-async"
    ))]
    pub fn apply_changeset_async<'a, DBend, Conn, T, S, B, A, R>(
        &self,
        changeset: &'a ChangeSet<T, S, B>,
        refusals: &R,
        conn: &'a mut Conn,
        adapter: &'a A,
    ) -> impl core::future::Future<Output = Result<usize, ApplyError>> + Send + 'a
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel_async::AsyncConnection<Backend = DBend>,
        A: Adapter<DBend, S, B> + Send + Sync + 'a,
        R: RefusalView,
        T: SchemaWithPK + ColumnNames + Send + Sync + 'a,
        S: AsRef<str> + Clone + Hash + Eq + core::fmt::Debug + Send + Sync + 'a,
        B: AsRef<[u8]> + Clone + Hash + Eq + core::fmt::Debug + Send + Sync + 'a,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        let refused = check_refusals(refusals, changeset.iter().map(|op| op.table().name()));
        let apply = changeset
            .iter()
            .map(|op| op.with_adapter::<DBend, _>(adapter))
            .apply_transactional_async(conn);
        async move {
            refused?;
            apply.await.map_err(ApplyError::from)
        }
    }

    /// Async peer of [`Self::apply_diffset_bytes`], driven by
    /// [`diesel_async`]. Parses raw uploaded diffset bytes, reconstructs
    /// the batch against subql's catalog, and applies it in one
    /// transaction. Dispatches on the format marker (patchset or
    /// changeset) exactly like the sync entry point.
    ///
    /// `refusals` is consulted before any op applies: an upload naming a
    /// table it refuses is refused as a whole with
    /// [`ApplyError::RefusedTable`].
    ///
    /// # Errors
    ///
    /// [`ApplyError::RefusedTable`] when an op names a refused table.
    /// [`ApplyError::Diesel`] wrapping a
    /// [`QueryBuilderError`](diesel::result::Error::QueryBuilderError)
    /// when the bytes fail to parse or name a table absent from the
    /// catalog, or the first bind or execution error any op produces,
    /// with the whole transaction rolled back.
    #[cfg(any(
        feature = "apply-patchset-postgres-async",
        feature = "apply-patchset-mysql-async"
    ))]
    pub fn apply_diffset_bytes_async<'a, DBend, Conn, A, R>(
        &self,
        bytes: &[u8],
        refusals: &R,
        conn: &'a mut Conn,
        adapter: &'a A,
    ) -> impl core::future::Future<Output = Result<usize, ApplyError>> + Send + 'a
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel_async::AsyncConnection<Backend = DBend>,
        A: Adapter<DBend, String, Vec<u8>> + Send + Sync + 'a,
        R: RefusalView,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        apply_diffset_bytes_async_with_catalog(self.database(), bytes, refusals, conn, adapter)
    }
}

#[cfg(all(test, feature = "apply-patchset-sqlite"))]
#[allow(clippy::unwrap_used)]
mod tests {
    use diesel::{sql_query, Connection, QueryableByName, RunQueryDsl, SqliteConnection};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{
        ChangeDelete, ChangeSet, ChangesetFormat, DiffOps, Insert, PatchDelete, PatchSet,
        PatchsetFormat, SimpleTable, Update, Value,
    };
    use sqlparser::dialect::SQLiteDialect;

    use super::{ApplyError, SqliteAdapter};
    use crate::backend::SQLite as SqliteBackend;
    use crate::drift::{Incompatibility, NoRefusals};
    use crate::testing::TestEvent;
    use crate::{DefaultIds, SubscriptionEngine};

    const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);";

    type Engine = SubscriptionEngine<TestEvent<SqliteBackend>, DefaultIds, ParserDB>;

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Item {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        qty: i64,
    }

    fn items(conn: &mut SqliteConnection) -> Vec<Item> {
        sql_query("SELECT id, name, qty FROM items ORDER BY id")
            .load::<Item>(conn)
            .unwrap()
    }

    fn table() -> SimpleTable {
        SimpleTable::new("items", &[("id", ""), ("name", ""), ("qty", "")], &[0])
    }

    /// Craft the raw patchset bytes the SQLite session extension would
    /// emit for a full-row insert.
    fn insert_bytes(id: i64, name: &str, qty: i64) -> Vec<u8> {
        let insert = Insert::<_, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(id))
            .unwrap()
            .set(1, Value::Text(name.into()))
            .unwrap()
            .set(2, Value::Integer(qty))
            .unwrap();
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build()
    }

    /// Craft an update that carries the primary key plus a new `qty`,
    /// leaving `name` unset (unchanged), as a patchset update does.
    fn update_qty_bytes(id: i64, qty: i64) -> Vec<u8> {
        let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(id))
            .unwrap()
            .set(2, Value::Integer(qty))
            .unwrap();
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .update(update)
            .build()
    }

    /// Craft a delete carrying only the primary key.
    fn delete_bytes(id: i64) -> Vec<u8> {
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .delete(PatchDelete::new(table(), vec![Value::Integer(id)]))
            .build()
    }

    fn fixture() -> (Engine, SqliteConnection) {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(DDL).execute(&mut conn).unwrap();
        let catalog = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
        let engine = Engine::new(catalog, SQLiteDialect {});
        (engine, conn)
    }

    #[test]
    fn apply_diffset_bytes_inserts_updates_deletes() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");

        let inserted = engine
            .apply_diffset_bytes(&insert_bytes(1, "a", 10), &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(inserted, 1, "one row inserted");
        engine
            .apply_diffset_bytes(&insert_bytes(2, "b", 20), &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![
                Item {
                    id: 1,
                    name: "a".into(),
                    qty: 10,
                },
                Item {
                    id: 2,
                    name: "b".into(),
                    qty: 20,
                },
            ],
            "both rows present after inserts"
        );

        engine
            .apply_diffset_bytes(&update_qty_bytes(1, 99), &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![
                Item {
                    id: 1,
                    name: "a".into(),
                    qty: 99,
                },
                Item {
                    id: 2,
                    name: "b".into(),
                    qty: 20,
                },
            ],
            "row 1 qty updated, name unchanged, row 2 untouched"
        );

        engine
            .apply_diffset_bytes(&delete_bytes(2), &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 1,
                name: "a".into(),
                qty: 99,
            }],
            "row 2 deleted, matched on its primary key"
        );
    }

    #[test]
    fn apply_diffset_bytes_rejects_unparseable_bytes() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        let err = engine
            .apply_diffset_bytes(b"not a diffset", &NoRefusals, &mut conn, &adapter)
            .unwrap_err();
        assert!(
            err.to_string().contains("failed to parse"),
            "error explains the parse failure, got: {err}"
        );
        assert!(items(&mut conn).is_empty(), "nothing applied on rejection");
    }

    #[test]
    fn apply_diffset_bytes_rejects_unknown_table() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");

        let ghost = SimpleTable::new("ghosts", &[("id", "")], &[0]);
        let insert = Insert::<_, String, Vec<u8>>::from(ghost)
            .set(0, Value::Integer(1))
            .unwrap();
        let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build();

        let err = engine
            .apply_diffset_bytes(&bytes, &NoRefusals, &mut conn, &adapter)
            .unwrap_err();
        assert!(
            err.to_string().contains("ghosts"),
            "error names the unknown table, got: {err}"
        );
    }

    const COMPOSITE_DDL: &str =
        "CREATE TABLE pairs (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b));";

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Pair {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        a: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        b: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        v: String,
    }

    fn pairs(conn: &mut SqliteConnection) -> Vec<Pair> {
        sql_query("SELECT a, b, v FROM pairs ORDER BY a, b")
            .load::<Pair>(conn)
            .unwrap()
    }

    fn composite_table() -> SimpleTable {
        SimpleTable::new("pairs", &[("a", ""), ("b", ""), ("v", "")], &[0, 1])
    }

    fn composite_fixture() -> (Engine, SqliteConnection) {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(COMPOSITE_DDL).execute(&mut conn).unwrap();
        let catalog = ParserDB::parse::<SQLiteDialect>(COMPOSITE_DDL).unwrap();
        let engine = Engine::new(catalog, SQLiteDialect {});
        (engine, conn)
    }

    /// A composite primary key must match on every key column, in both
    /// the update WHERE and the delete WHERE. The reconstruction resolves
    /// the multi-column pk from the catalog and zips it against the parsed
    /// op's pk values.
    #[test]
    fn apply_diffset_bytes_composite_pk_matches_all_key_columns() {
        let (engine, mut conn) = composite_fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");

        // Two rows share the first key column but differ in the second, so
        // a WHERE that matched only column `a` would corrupt the sibling.
        let insert = |a: i64, b: i64, v: &str| {
            Insert::<_, String, Vec<u8>>::from(composite_table())
                .set(0, Value::Integer(a))
                .unwrap()
                .set(1, Value::Integer(b))
                .unwrap()
                .set(2, Value::Text(v.into()))
                .unwrap()
        };
        for ins in [insert(1, 1, "x"), insert(1, 2, "y")] {
            let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
                .insert(ins)
                .build();
            engine
                .apply_diffset_bytes(&bytes, &NoRefusals, &mut conn, &adapter)
                .unwrap();
        }

        // Update only (1, 2).
        let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(composite_table())
            .set(0, Value::Integer(1))
            .unwrap()
            .set(1, Value::Integer(2))
            .unwrap()
            .set(2, Value::Text("y2".into()))
            .unwrap();
        let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .update(update)
            .build();
        let touched = engine
            .apply_diffset_bytes(&bytes, &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(touched, 1, "composite update matches exactly one row");
        assert_eq!(
            pairs(&mut conn),
            vec![
                Pair {
                    a: 1,
                    b: 1,
                    v: "x".into(),
                },
                Pair {
                    a: 1,
                    b: 2,
                    v: "y2".into(),
                },
            ],
            "only (1, 2) updated, its (1, 1) sibling untouched"
        );

        // Delete (1, 1).
        let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .delete(PatchDelete::new(
                composite_table(),
                vec![Value::Integer(1), Value::Integer(1)],
            ))
            .build();
        let touched = engine
            .apply_diffset_bytes(&bytes, &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(touched, 1, "composite delete matches exactly one row");
        assert_eq!(
            pairs(&mut conn),
            vec![Pair {
                a: 1,
                b: 2,
                v: "y2".into(),
            }],
            "(1, 1) deleted, (1, 2) survives"
        );
    }

    /// A patchset stores one value per primary-key column, used as the
    /// WHERE key, so it cannot express a primary-key-changing UPDATE (no
    /// slot carries the old key). Applying a patchset update that names a
    /// new key matches on that new key rather than relocating an existing
    /// row. This pins the documented limitation from the apply side.
    #[test]
    fn apply_diffset_bytes_patchset_cannot_relocate_a_primary_key() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        engine
            .apply_diffset_bytes(&insert_bytes(1, "a", 10), &NoRefusals, &mut conn, &adapter)
            .unwrap();

        // A patchset update carrying pk = 2 (a would-be new key) plus a new
        // name. There is no old key to match, so it touches no row.
        let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(2))
            .unwrap()
            .set(1, Value::Text("b".into()))
            .unwrap();
        let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .update(update)
            .build();
        let touched = engine
            .apply_diffset_bytes(&bytes, &NoRefusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(touched, 0, "no row carries the new key, so none is touched");
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 1,
                name: "a".into(),
                qty: 10,
            }],
            "the original row keeps its key and value, unrelocated"
        );
    }

    /// Craft a changeset UPDATE that relocates the primary key from
    /// `old_id` to `new_id` and renames, leaving `qty` unchanged. Only a
    /// changeset can carry this, since it stores the old and new value of
    /// each changed column.
    fn changeset_relocate_pk_bytes(
        old_id: i64,
        new_id: i64,
        old_name: &str,
        new_name: &str,
    ) -> Vec<u8> {
        let update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(old_id), Value::Integer(new_id))
            .unwrap()
            .set(
                1,
                Value::Text(old_name.into()),
                Value::Text(new_name.into()),
            )
            .unwrap();
        ChangeSet::<SimpleTable, String, Vec<u8>>::new()
            .update(update)
            .build()
    }

    /// The changeset path applies a primary-key-changing UPDATE, matching
    /// the old key in the WHERE and writing the new key in the SET, which
    /// the patchset path cannot do.
    #[test]
    fn apply_diffset_bytes_changeset_relocates_a_primary_key() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        engine
            .apply_diffset_bytes(&insert_bytes(1, "a", 10), &NoRefusals, &mut conn, &adapter)
            .unwrap();

        let touched = engine
            .apply_diffset_bytes(
                &changeset_relocate_pk_bytes(1, 2, "a", "b"),
                &NoRefusals,
                &mut conn,
                &adapter,
            )
            .unwrap();
        assert_eq!(
            touched, 1,
            "the primary-key-changing update touches one row"
        );
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 2,
                name: "b".into(),
                qty: 10,
            }],
            "row relocated from id 1 to id 2 with qty preserved"
        );
    }

    /// A changeset carrying an insert, a non-key update, and a delete on
    /// three distinct rows, so `apply_diffset_bytes` exercises the
    /// changeset arm's insert, update, and delete reconstruction branches
    /// in one batch.
    fn changeset_insert_update_delete_bytes() -> Vec<u8> {
        let insert = Insert::<_, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(3))
            .unwrap()
            .set(1, Value::Text("c".into()))
            .unwrap()
            .set(2, Value::Integer(30))
            .unwrap();
        // Non-key update: the primary key is unchanged (old equal to new)
        // and only `qty` moves, so the render keeps `id` in the WHERE and
        // out of the SET.
        let update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(1), Value::Integer(1))
            .unwrap()
            .set(2, Value::Integer(10), Value::Integer(99))
            .unwrap();
        let delete = ChangeDelete::from(table())
            .set(0, Value::Integer(2))
            .unwrap()
            .set(1, Value::Text("b".into()))
            .unwrap()
            .set(2, Value::Integer(20))
            .unwrap();
        ChangeSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .update(update)
            .delete(delete)
            .build()
    }

    /// The changeset arm reconstructs and applies insert, non-key update,
    /// and delete ops, not just the pk-relocating update.
    #[test]
    fn apply_diffset_bytes_changeset_insert_update_delete() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        engine
            .apply_diffset_bytes(&insert_bytes(1, "a", 10), &NoRefusals, &mut conn, &adapter)
            .unwrap();
        engine
            .apply_diffset_bytes(&insert_bytes(2, "b", 20), &NoRefusals, &mut conn, &adapter)
            .unwrap();

        let touched = engine
            .apply_diffset_bytes(
                &changeset_insert_update_delete_bytes(),
                &NoRefusals,
                &mut conn,
                &adapter,
            )
            .unwrap();
        assert_eq!(touched, 3, "insert, update, and delete each apply");
        assert_eq!(
            items(&mut conn),
            vec![
                Item {
                    id: 1,
                    name: "a".into(),
                    qty: 99,
                },
                Item {
                    id: 3,
                    name: "c".into(),
                    qty: 30,
                },
            ],
            "row 3 inserted, row 1 qty updated, row 2 deleted"
        );
    }

    /// Build a fresh `(ParserDB, SqliteConnection)` pair with no
    /// `SubscriptionEngine` in scope, so a test can apply through the
    /// catalog-only entry point holding only the catalog.
    fn catalog_fixture() -> (ParserDB, SqliteConnection) {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(DDL).execute(&mut conn).unwrap();
        let catalog = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
        (catalog, conn)
    }

    /// `apply_diffset_bytes_with_catalog` applies a patchset given only a
    /// `&ParserDB` catalog, with no `SubscriptionEngine` allocated. Covers
    /// the patchset dispatch arm of the catalog-only entry point.
    #[test]
    fn apply_diffset_bytes_with_catalog_applies_a_patchset() {
        let (catalog, mut conn) = catalog_fixture();
        let adapter = SqliteAdapter::new(&catalog).expect("the catalog indexes");

        let inserted = super::apply_diffset_bytes_with_catalog(
            &catalog,
            &insert_bytes(1, "a", 10),
            &NoRefusals,
            &mut conn,
            &adapter,
        )
        .unwrap();
        assert_eq!(
            inserted, 1,
            "one row inserted through the catalog entry point"
        );

        super::apply_diffset_bytes_with_catalog(
            &catalog,
            &update_qty_bytes(1, 42),
            &NoRefusals,
            &mut conn,
            &adapter,
        )
        .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 1,
                name: "a".into(),
                qty: 42,
            }],
            "row 1 qty updated via the catalog entry point, name unchanged"
        );
    }

    /// `apply_diffset_bytes_with_catalog` applies a primary-key-changing
    /// changeset update (the case `apply_diffset_bytes` documents a
    /// patchset cannot represent): the old key matches in the WHERE and
    /// the new key writes in the SET. Covers the changeset dispatch arm of
    /// the catalog-only entry point, still with no engine in scope.
    #[test]
    fn apply_diffset_bytes_with_catalog_relocates_a_primary_key() {
        let (catalog, mut conn) = catalog_fixture();
        let adapter = SqliteAdapter::new(&catalog).expect("the catalog indexes");
        super::apply_diffset_bytes_with_catalog(
            &catalog,
            &insert_bytes(1, "a", 10),
            &NoRefusals,
            &mut conn,
            &adapter,
        )
        .unwrap();

        let touched = super::apply_diffset_bytes_with_catalog(
            &catalog,
            &changeset_relocate_pk_bytes(1, 2, "a", "b"),
            &NoRefusals,
            &mut conn,
            &adapter,
        )
        .unwrap();
        assert_eq!(
            touched, 1,
            "the primary-key-changing update touches one row"
        );
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 2,
                name: "b".into(),
                qty: 10,
            }],
            "row relocated from id 1 to id 2 with qty preserved"
        );
    }

    const TWO_TABLE_DDL: &str =
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER); \
                                 CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);";

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Note {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        body: String,
    }

    fn notes(conn: &mut SqliteConnection) -> Vec<Note> {
        sql_query("SELECT id, body FROM notes ORDER BY id")
            .load::<Note>(conn)
            .unwrap()
    }

    fn two_table_fixture() -> (Engine, SqliteConnection) {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
            .execute(&mut conn)
            .unwrap();
        sql_query("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
            .execute(&mut conn)
            .unwrap();
        let catalog = ParserDB::parse::<SQLiteDialect>(TWO_TABLE_DDL).unwrap();
        let engine = Engine::new(catalog, SQLiteDialect {});
        (engine, conn)
    }

    fn notes_table() -> SimpleTable {
        SimpleTable::new("notes", &[("id", ""), ("body", "")], &[0])
    }

    fn notes_insert_bytes(id: i64, body: &str) -> Vec<u8> {
        let insert = Insert::<_, String, Vec<u8>>::from(notes_table())
            .set(0, Value::Integer(id))
            .unwrap()
            .set(1, Value::Text(body.into()))
            .unwrap();
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build()
    }

    /// A refusal view that refuses exactly the tables it is given, each
    /// with one dropped-column reason, so a test passes a named refusal
    /// without a `DriftGuard` standing behind it.
    struct NamedRefusals {
        /// The refused table names, each with its reasons.
        refused: std::collections::BTreeMap<String, Vec<Incompatibility>>,
    }

    impl crate::drift::RefusalView for NamedRefusals {
        fn refusal(&self, table: &str) -> Option<Vec<Incompatibility>> {
            self.refused.get(table).cloned()
        }
    }

    fn refusals_naming(table: &str) -> NamedRefusals {
        NamedRefusals {
            refused: std::collections::BTreeMap::from([(
                table.to_string(),
                vec![Incompatibility::ColumnDropped {
                    column: "amount".into(),
                }],
            )]),
        }
    }

    /// An upload naming a refused table is refused as a whole with the
    /// typed reason, before any op applies: every table of the target
    /// database stays unchanged.
    #[test]
    fn apply_diffset_bytes_refuses_an_upload_touching_a_refused_table() {
        let (engine, mut conn) = two_table_fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        let refusals = refusals_naming("notes");

        let err = engine
            .apply_diffset_bytes(&notes_insert_bytes(1, "b"), &refusals, &mut conn, &adapter)
            .unwrap_err();
        match err {
            ApplyError::RefusedTable { table, reasons } => {
                assert_eq!(table, "notes");
                assert_eq!(
                    reasons,
                    vec![Incompatibility::ColumnDropped {
                        column: "amount".into()
                    }]
                );
            }
            other => panic!("the refusal carries the typed reason, got {other:?}"),
        }
        assert!(
            items(&mut conn).is_empty(),
            "the sibling table stays untouched"
        );
        assert!(
            notes(&mut conn).is_empty(),
            "the refused table's op never applies"
        );
    }

    /// The same refusal view leaves an upload that names only unrefused
    /// tables to apply as before.
    #[test]
    fn apply_diffset_bytes_applies_an_upload_naming_only_unrefused_tables() {
        let (engine, mut conn) = two_table_fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        let refusals = refusals_naming("notes");

        let inserted = engine
            .apply_diffset_bytes(&insert_bytes(1, "a", 10), &refusals, &mut conn, &adapter)
            .unwrap();
        assert_eq!(inserted, 1, "the unrefused table applies as before");
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 1,
                name: "a".into(),
                qty: 10,
            }]
        );
        assert!(
            notes(&mut conn).is_empty(),
            "the refused table is untouched by this upload"
        );
    }

    /// A changeset upload naming a refused table is refused the same way,
    /// covering the changeset arm of the byte entry point's check.
    #[test]
    fn apply_diffset_bytes_refuses_a_changeset_touching_a_refused_table() {
        let (engine, mut conn) = two_table_fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        let refusals = refusals_naming("notes");

        let insert = Insert::<_, String, Vec<u8>>::from(notes_table())
            .set(0, Value::Integer(1))
            .unwrap()
            .set(1, Value::Text("b".into()))
            .unwrap();
        let bytes = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build();

        let err = engine
            .apply_diffset_bytes(&bytes, &refusals, &mut conn, &adapter)
            .unwrap_err();
        match err {
            ApplyError::RefusedTable { table, .. } => {
                assert_eq!(table, "notes");
            }
            other => panic!("the changeset upload is refused with the typed reason, got {other:?}"),
        }
        assert!(notes(&mut conn).is_empty(), "nothing applies on refusal");
    }

    /// A pre-built batch naming a refused table is refused before its
    /// first op, through the same view the byte entry points take.
    #[test]
    fn apply_patchset_refuses_a_batch_touching_a_refused_table() {
        let (engine, mut conn) = two_table_fixture();
        let adapter = SqliteAdapter::new(engine.database()).expect("the catalog indexes");
        let refusals = refusals_naming("notes");
        let batch = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(
            Insert::<_, String, Vec<u8>>::from(notes_table())
                .set(0, Value::Integer(1))
                .unwrap()
                .set(1, Value::Text("b".into()))
                .unwrap(),
        );

        let err = engine
            .apply_patchset(&batch, &refusals, &mut conn, &adapter)
            .unwrap_err();
        match err {
            ApplyError::RefusedTable { table, .. } => {
                assert_eq!(table, "notes");
            }
            other => panic!("the batch is refused with the typed reason, got {other:?}"),
        }
        assert!(notes(&mut conn).is_empty(), "nothing applies on refusal");
    }
}
