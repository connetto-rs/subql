//! Live-connection SQLite CDC source (gated behind `sqlite-cdc`).
//!
//! Wraps a diesel [`SqliteConnection`] with the SQLite session extension
//! attached via [`diesel_sqlite_session`]. One session per catalog table,
//! attached by name, so a schema change on one table never poisons the
//! others' changes. [`SqliteCdcSource::poll_next_item`] drains the
//! sessions as changesets, maps each op's positions to catalog columns by
//! name through the drain's `sqlite_schema` read, and yields
//! [`SqliteChangesetEvent`] instances one at a time, with a
//! [`SqliteSchemaSignal`] relation item whenever a tracked table's
//! `CREATE` text moved or its undrained changes were lost.
//!
//! Analogue of [`crate::PgStreamingCdcSource`]. `impl CdcSource` rides on
//! the upstream `unsafe impl Send for Session` added in
//! `diesel-sqlite-session`.

use alloc::collections::BTreeMap;
use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use diesel::QueryableByName;
use diesel::SqliteConnection;
use diesel::{sql_query, RunQueryDsl};
use diesel_sqlite_session::{Session, SessionError, SqliteErrorCode, SqliteSessionExt};
use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;

use super::event::SqliteChangesetEvent;
use super::parser::SqliteChangesetParser;
use crate::types::TableId;
use crate::wal::{CdcSource, SourceItem, SourceItemOf, WalParseError};

/// One drain's live maps: each catalog table's `CREATE` text and live
/// column names, read from the database's `sqlite_schema`.
type LiveMaps = (
    BTreeMap<String, Option<String>>,
    BTreeMap<String, Arc<[String]>>,
);

/// Errors surfaced by [`SqliteCdcSource`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteCdcError {
    /// Underlying `diesel-sqlite-session` failure (session creation,
    /// attach, or changeset export).
    #[error("diesel-sqlite-session: {0}")]
    Session(#[from] SessionError),
    /// The changeset parser could not decode the bytes surfaced by the
    /// session extension.
    #[error("changeset parse: {0}")]
    Parse(#[from] WalParseError),
    /// Underlying diesel connection error (statement execution).
    #[error("diesel: {0}")]
    Diesel(#[from] diesel::result::Error),
}

/// A schema message from a live SQLite source.
///
/// The bare database table name, which a consumer holding a catalog
/// resolves to its [`TableId`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SqliteSchemaSignal {
    /// The table's `CREATE` text moved between drains.
    SchemaChanged {
        /// The table's bare name in the database.
        table: String,
        /// The table's new `CREATE` text, or `None` when the table no
        /// longer exists.
        create_sql: Option<String>,
    },
    /// The table's undrained changes were lost.
    ChangesLost {
        /// The table's bare name in the database.
        table: String,
    },
}

/// One session and the catalog identity of the table it tracks.
struct SqliteTableSession {
    name: String,
    table: TableId,
    session: Session,
}

/// One `sqlite_schema` row.
#[derive(QueryableByName, Debug)]
struct SchemaRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    sql: Option<String>,
}

/// Live SQLite CDC source.
pub struct SqliteCdcSource<DB: DatabaseLike> {
    // `sessions` MUST stay declared above `connection`: fields drop in
    // declaration order, and a session's drop reads the connection's
    // `sqlite3` handle.
    sessions: Vec<SqliteTableSession>,
    connection: SqliteConnection,
    catalog: DB,
    /// Items buffered from the last drain, consumed FIFO.
    pending:
        VecDeque<SourceItem<SqliteChangesetEvent, core::convert::Infallible, SqliteSchemaSignal>>,
    /// `PRAGMA schema_version` as read at the last drain.
    schema_version: i64,
    /// Each catalog table's `CREATE` text, or `None` when the database
    /// no longer has the table, as of the last drain.
    create_texts: BTreeMap<String, Option<String>>,
    /// Each catalog table's live column names in wire order, as of the
    /// last drain.
    live_names: BTreeMap<String, Arc<[String]>>,
}

impl<DB: DatabaseLike> SqliteCdcSource<DB> {
    /// Construct a source over `connection`, opening one session per
    /// catalog table, attached by name.
    ///
    /// The session extension silently ignores changes to a table that
    /// declares no primary key, so a catalog table without one is
    /// attached but never yields an event.
    ///
    /// # Errors
    ///
    /// [`SqliteCdcError::Session`] when session creation or attachment
    /// fails, [`SqliteCdcError::Diesel`] when the `sqlite_schema` read
    /// fails.
    pub fn new(mut connection: SqliteConnection, catalog: DB) -> Result<Self, SqliteCdcError> {
        let mut sessions = Vec::new();
        for table in catalog.tables() {
            let Some(id) = catalog.table_id(table) else {
                continue;
            };
            let Some(id) = u32::try_from(id).ok() else {
                continue;
            };
            let name = table.table_name().to_string();
            let mut session = connection.create_session()?;
            session.attach_by_name(&name)?;
            sessions.push(SqliteTableSession {
                name,
                table: id,
                session,
            });
        }
        let (create_texts, live_names) = read_live_maps(&mut connection, &catalog)?;
        let schema_version = read_schema_version(&mut connection)?;
        Ok(Self {
            sessions,
            connection,
            catalog,
            pending: VecDeque::new(),
            schema_version,
            create_texts,
            live_names,
        })
    }

    /// Drain the next item from the source.
    ///
    /// Returns `Ok(None)` when every session is empty and the buffer
    /// holds nothing. A drain yields its schema signals before the
    /// events they precede.
    ///
    /// # Errors
    ///
    /// [`SqliteCdcError::Session`] on a session failure other than a
    /// `SQLITE_SCHEMA` changeset, [`SqliteCdcError::Parse`] on
    /// undecodable changeset bytes, [`SqliteCdcError::Diesel`] on a
    /// `sqlite_schema` or `PRAGMA` read failure.
    pub fn poll_next_item(&mut self) -> Result<Option<SourceItemOf<Self>>, SqliteCdcError> {
        if self.pending.is_empty() {
            self.drain()?;
        }
        Ok(self.pending.pop_front())
    }

    /// Mutable access to the underlying diesel [`SqliteConnection`].
    ///
    /// The caller uses this to drive DML that the sessions then observe.
    /// Writes performed through any *other* connection (a sibling handle
    /// to the same database file, for example) bypass the sessions and
    /// will not surface as events.
    pub const fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.connection
    }

    /// Immutable access to the catalog the source was built with.
    pub const fn catalog(&self) -> &DB {
        &self.catalog
    }

    fn drain(&mut self) -> Result<(), SqliteCdcError> {
        let Self {
            sessions,
            connection,
            catalog,
            pending,
            schema_version,
            create_texts,
            live_names,
        } = self;
        let v_before = read_schema_version(connection)?;
        let moved = v_before != *schema_version;
        let mut final_maps: Option<LiveMaps> = None;
        let (texts_used, names_used) = if moved {
            // The schema moved, so the stored maps are stale and the
            // signals must precede this drain's events.
            let maps = read_live_maps(connection, catalog)?;
            for ts in sessions.iter() {
                let stored = create_texts.get(&ts.name).cloned();
                let stored = stored.as_ref().and_then(|text| text.as_ref());
                let live = maps.0.get(&ts.name).cloned().flatten();
                if stored != live.as_ref() {
                    pending.push_back(SourceItem::Relation(SqliteSchemaSignal::SchemaChanged {
                        table: ts.name.clone(),
                        create_sql: live,
                    }));
                }
            }
            final_maps = Some(maps);
            let (texts, names) = final_maps.as_ref().expect("the maps were just read");
            (texts, names)
        } else {
            (&*create_texts, &*live_names)
        };
        for ts in sessions.iter_mut() {
            if ts.session.is_empty() {
                continue;
            }
            match ts.session.changeset() {
                Ok(bytes) => {
                    let events = SqliteChangesetParser
                        .parse_with_live_shapes(&bytes, catalog, names_used)?;
                    pending.extend(events.into_iter().map(SourceItem::Event));
                    replace_session(ts, connection)?;
                }
                Err(SessionError::ChangesetFailed(SqliteErrorCode::Schema)) => {
                    // The table's schema moved under its recorded changes,
                    // so those changes are gone and no further drain can
                    // export them.
                    pending.push_back(SourceItem::Relation(SqliteSchemaSignal::ChangesLost {
                        table: ts.name.clone(),
                    }));
                    replace_session(ts, connection)?;
                }
                Err(err) => return Err(err.into()),
            }
        }
        let v_after = read_schema_version(connection)?;
        if v_after != v_before {
            // The schema moved mid-drain, so this drain's events for a
            // table whose `CREATE` text no longer matches the shape they
            // were mapped under are mapped under the wrong names.
            let maps = read_live_maps(connection, catalog)?;
            for ts in sessions.iter() {
                let used = texts_used.get(&ts.name).cloned().unwrap_or_default();
                let after = maps.0.get(&ts.name).cloned().unwrap_or_default();
                if used != after {
                    pending.retain(
                        |item| !matches!(item, SourceItem::Event(ev) if ev.table_id == ts.table),
                    );
                    pending.push_back(SourceItem::Relation(SqliteSchemaSignal::ChangesLost {
                        table: ts.name.clone(),
                    }));
                }
            }
            final_maps = Some(maps);
        }
        if let Some((texts, names)) = final_maps {
            // `attach_by_name` does not check that the table still exists,
            // so a vanished table is dropped from tracking here, against
            // the drain's own schema read.
            sessions.retain(|ts| matches!(texts.get(&ts.name), Some(Some(_))));
            *create_texts = texts;
            *live_names = names;
        }
        *schema_version = v_after;
        Ok(())
    }
}

/// Replace `ts`'s session with a fresh one attached to the same table, so
/// the next drain only sees changes made after this one.
fn replace_session(
    ts: &mut SqliteTableSession,
    connection: &mut SqliteConnection,
) -> Result<(), SqliteCdcError> {
    let mut fresh = connection.create_session()?;
    fresh.attach_by_name(&ts.name)?;
    ts.session = fresh;
    Ok(())
}

/// One `PRAGMA schema_version` row.
#[derive(QueryableByName, Debug)]
struct VersionRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    schema_version: i64,
}

/// `PRAGMA schema_version` as a single value. Vendor-side metadata the
/// typed DSL has no schema for.
fn read_schema_version(connection: &mut SqliteConnection) -> Result<i64, SqliteCdcError> {
    let rows: Vec<VersionRow> = sql_query("PRAGMA schema_version").load(connection)?;
    Ok(rows.into_iter().next().map_or(0, |row| row.schema_version))
}

/// Each catalog table's `CREATE` text and live column names, from one
/// `sqlite_schema` read. The catalog table the typed DSL has no schema
/// for, so the read is raw.
fn read_live_maps<DB: DatabaseLike>(
    connection: &mut SqliteConnection,
    catalog: &DB,
) -> Result<LiveMaps, SqliteCdcError> {
    let rows: Vec<SchemaRow> =
        sql_query("SELECT name, sql FROM sqlite_schema WHERE type = 'table'").load(connection)?;
    let mut create_texts = BTreeMap::new();
    let mut live_names = BTreeMap::new();
    for table in catalog.tables() {
        let name = table.table_name().to_string();
        let ddl = rows
            .iter()
            .find(|row| row.name == name)
            .and_then(|row| row.sql.as_ref())
            .cloned();
        live_names.insert(name.clone(), live_column_names(ddl.as_deref()));
        create_texts.insert(name, ddl);
    }
    Ok((create_texts, live_names))
}

/// The DDL's column names in declared order, or empty when the DDL is
/// missing or unparseable, which degrades the table to its old
/// positional behaviour.
fn live_column_names(ddl: Option<&str>) -> Arc<[String]> {
    let Some(ddl) = ddl else {
        return Arc::from(Vec::<String>::new());
    };
    let Ok(database) = ParserDB::parse::<SQLiteDialect>(ddl) else {
        return Arc::from(Vec::<String>::new());
    };
    let mut names = Vec::new();
    if let Some(table) = database.tables().next() {
        if let Ok(columns) = table.columns(&database) {
            for column in columns {
                names.push(column.column_name().to_string());
            }
        }
    }
    Arc::from(names)
}

impl<DB: DatabaseLike> CdcSource for SqliteCdcSource<DB> {
    type Event = SqliteChangesetEvent;
    type Commit = core::convert::Infallible;
    type Relation = SqliteSchemaSignal;
    type Error = SqliteCdcError;

    fn next_item(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send
    {
        core::future::ready(self.poll_next_item())
    }

    fn ack(
        &mut self,
        _upto: <Self::Event as crate::backend::CdcEvent>::Checkpoint,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        // Changeset events carry `NoCheckpoint`. The session extension
        // has no upstream to acknowledge against, so `ack` is a no-op.
        core::future::ready(Ok(()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::{CdcEvent, RowKind, Value};
    use diesel::Connection;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::SQLiteDialect;

    // The session extension silently ignores changes to a table that
    // declares no primary key, so both tables key on `id`.
    fn open_orders_db() -> (SqliteConnection, ParserDB) {
        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE _pad (id INTEGER PRIMARY KEY);\n\
             CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT);",
        )
        .expect("orders DDL parses");
        let mut conn = SqliteConnection::establish(":memory:").expect("open in-memory sqlite");
        sql_query("CREATE TABLE _pad (id INTEGER PRIMARY KEY)")
            .execute(&mut conn)
            .expect("pad DDL");
        sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)")
            .execute(&mut conn)
            .expect("orders DDL");
        (conn, catalog)
    }

    fn event_of(source: &mut SqliteCdcSource<ParserDB>) -> Option<SqliteChangesetEvent> {
        source
            .poll_next_item()
            .expect("poll succeeds")
            .and_then(SourceItem::into_event)
    }

    #[test]
    fn typed_sqlite_source_yields_insert_event() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        assert!(
            source.poll_next_item().expect("empty poll").is_none(),
            "no changes yet -> None"
        );
        sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
            .execute(source.connection())
            .expect("insert");
        let event = source
            .poll_next_item()
            .expect("poll succeeds")
            .and_then(SourceItem::into_event)
            .expect("one event pending");
        assert_eq!(event.kind(), crate::EventKind::Insert);
        assert_eq!(event.pk_columns(source.catalog()), &[0u16]);
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 1).unwrap(),
            Value::Int(250)
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );
        assert!(source.poll_next_item().expect("post-drain poll").is_none());
    }

    #[test]
    fn typed_sqlite_source_yields_multiple_events() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        for (id, amt) in [(1, 10), (2, 20), (3, 30)] {
            sql_query(alloc::format!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {amt}, 's')"
            ))
            .execute(source.connection())
            .expect("insert");
        }

        // What the parser will make of the accumulated sessions, read
        // before the source drains them. `changeset()` snapshots rather
        // than consumes, so this does not disturb the poll below.
        // `orders` is the only session with changes.
        let expected: Vec<Value<crate::backend::SQLite>> = {
            let session = source
                .sessions
                .iter_mut()
                .find(|ts| !ts.session.is_empty())
                .expect("orders session has changes");
            let bytes = session.session.changeset().expect("snapshot the session");
            let events = crate::wal::WalParser::parse_wal_message(
                &SqliteChangesetParser,
                &bytes,
                source.catalog(),
            )
            .expect("the snapshot parses");
            events
                .iter()
                .map(|ev| ev.value_at(source.catalog(), RowKind::New, 0).unwrap())
                .collect()
        };
        assert_eq!(expected.len(), 3, "three inserts, three events");

        let mut drained = Vec::new();
        while let Some(item) = source.poll_next_item().expect("poll") {
            drained.push(item.into_event().expect("drained items are events"));
        }
        assert_eq!(drained.len(), 3);
        for ev in &drained {
            assert_eq!(ev.kind(), crate::EventKind::Insert);
        }
        // Front first, in the parser's order. A consumer applying these
        // downstream depends on it.
        let ids: Vec<Value<crate::backend::SQLite>> = drained
            .iter()
            .map(|ev| ev.value_at(source.catalog(), RowKind::New, 0).unwrap())
            .collect();
        assert_eq!(ids, expected, "the buffer delivers in the parser's order");
        assert!(source.poll_next_item().expect("post-drain poll").is_none());
    }

    #[test]
    fn typed_sqlite_source_delete_and_update() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (5, 100, 'pending')")
            .execute(source.connection())
            .expect("insert");
        let _ = event_of(&mut source);
        assert!(source.poll_next_item().expect("post-insert poll").is_none());

        sql_query("UPDATE orders SET status = 'shipped' WHERE id = 5")
            .execute(source.connection())
            .expect("update");
        let update = event_of(&mut source).expect("one update event");
        assert_eq!(update.kind(), crate::EventKind::Update);
        // Bytes from a real session, which writes each key column of an
        // UPDATE as an old-only pair: the row's identity, not a change.
        let mut changed = update.changed_columns(source.catalog());
        changed.sort_unstable();
        assert_eq!(
            changed,
            vec![2u16],
            "an UPDATE lists the columns it touched, and the key column is not one of them"
        );
        assert_eq!(
            update.value_at(source.catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(5)
        );

        sql_query("DELETE FROM orders WHERE id = 5")
            .execute(source.connection())
            .expect("delete");
        let delete = event_of(&mut source).expect("one delete event");
        assert_eq!(delete.kind(), crate::EventKind::Delete);
        assert_eq!(
            delete.value_at(source.catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(5)
        );
    }

    #[test]
    fn an_added_column_is_signalled_and_projected() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
            .execute(source.connection())
            .expect("insert");
        sql_query("ALTER TABLE orders ADD COLUMN extra TEXT")
            .execute(source.connection())
            .expect("column added");

        let first = source
            .poll_next_item()
            .expect("poll")
            .expect("a signal precedes the drain's events");
        let SourceItem::Relation(SqliteSchemaSignal::SchemaChanged {
            table,
            create_sql: Some(_),
        }) = &first
        else {
            panic!("the moved table is signalled, got {first:?}");
        };
        assert_eq!(table, "orders");
        let event = event_of(&mut source).expect("the insert still arrives");
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(7),
            "id lands in the catalog's id column"
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 1).unwrap(),
            Value::Int(250),
            "amount is not shifted by the extra column"
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 2).unwrap(),
            Value::String("paid".into()),
            "status is not shifted by the extra column"
        );
        assert!(source.poll_next_item().expect("post-drain poll").is_none());
    }

    #[test]
    fn a_dropped_column_loses_orders_but_keeps_the_other_table() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
            .execute(source.connection())
            .expect("orders insert");
        sql_query("INSERT INTO _pad (id) VALUES (1)")
            .execute(source.connection())
            .expect("pad insert");
        sql_query("ALTER TABLE orders DROP COLUMN amount")
            .execute(source.connection())
            .expect("column dropped");

        let mut items = Vec::new();
        while let Some(item) = source.poll_next_item().expect("poll") {
            items.push(item);
        }
        let signals: Vec<&SqliteSchemaSignal> = items
            .iter()
            .filter_map(|item| match item {
                SourceItem::Relation(signal) => Some(signal),
                _ => None,
            })
            .collect();
        assert!(
            signals.iter().any(|signal| {
                matches!(
                    signal,
                    SqliteSchemaSignal::SchemaChanged { table, create_sql: Some(_) }
                        if table == "orders"
                )
            }),
            "orders' moved CREATE text is signalled, got {signals:?}"
        );
        let events: Vec<SqliteChangesetEvent> = items
            .into_iter()
            .filter_map(SourceItem::into_event)
            .collect();
        // The drop lost orders' undrained change, but `_pad`'s insert
        // must survive the same drain.
        assert_eq!(events.len(), 1, "only _pad's insert survives");
        let pad_table = crate::catalog_helpers::table_id::<crate::backend::SQLite, ParserDB>(
            source.catalog(),
            "_pad",
        )
        .expect("pad is in the catalog");
        assert_eq!(events[0].table_id(source.catalog()), pad_table);
        assert_eq!(
            events[0]
                .value_at(source.catalog(), RowKind::New, 0)
                .unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn a_vanished_table_loses_its_changes_and_stops_being_tracked() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
            .execute(source.connection())
            .expect("insert");
        sql_query("DROP TABLE orders")
            .execute(source.connection())
            .expect("table dropped");

        let mut items = Vec::new();
        while let Some(item) = source.poll_next_item().expect("poll") {
            items.push(item);
        }
        assert!(
            items.iter().any(|item| {
                matches!(
                    item,
                    SourceItem::Relation(SqliteSchemaSignal::ChangesLost { table })
                        if table == "orders"
                )
            }),
            "the lost change is signalled, got {items:?}"
        );
        // The dropped table's session is gone, so further drains stay
        // quiet instead of re-signalling a table that no longer exists.
        assert!(source.poll_next_item().expect("quiet poll").is_none());
        assert!(
            source.sessions.iter().all(|ts| ts.name != "orders"),
            "orders is no longer tracked"
        );
    }

    fn file_orders_db() -> (
        tempfile::TempDir,
        SqliteConnection,
        SqliteConnection,
        ParserDB,
    ) {
        use diesel::connection::SimpleConnection;

        let ddl = "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)";
        let catalog = ParserDB::parse::<SQLiteDialect>(ddl).expect("catalog");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("source.sqlite");
        let url = path.to_str().expect("UTF-8 path");
        let mut connection = SqliteConnection::establish(url).expect("source connection");
        connection.batch_execute(ddl).expect("orders table");
        connection
            .batch_execute("PRAGMA busy_timeout = 0")
            .expect("disable lock waiting");
        let writer = SqliteConnection::establish(url).expect("writer connection");
        (dir, connection, writer, catalog)
    }

    #[test]
    fn construction_reports_a_locked_catalog() {
        use diesel::connection::SimpleConnection;

        let (_dir, connection, mut writer, catalog) = file_orders_db();
        writer
            .batch_execute("BEGIN EXCLUSIVE")
            .expect("lock database");
        assert!(matches!(
            SqliteCdcSource::new(connection, catalog),
            Err(SqliteCdcError::Diesel(
                diesel::result::Error::DatabaseError(_, _)
            ))
        ));
        writer.batch_execute("ROLLBACK").expect("release lock");
    }

    #[test]
    fn a_poll_reports_a_database_lock_and_recovers_without_losing_changes() {
        use diesel::connection::SimpleConnection;
        use diesel::ExpressionMethods;

        diesel::table! {
            orders (id) {
                id -> Integer,
                amount -> Nullable<Integer>,
                status -> Nullable<Text>,
            }
        }

        let (_dir, connection, mut writer, catalog) = file_orders_db();
        let mut source = SqliteCdcSource::new(connection, catalog).expect("source");
        diesel::insert_into(orders::table)
            .values((
                orders::id.eq(7),
                orders::amount.eq(250),
                orders::status.eq("paid"),
            ))
            .execute(source.connection())
            .expect("insert");
        writer
            .batch_execute("BEGIN EXCLUSIVE")
            .expect("lock database");
        assert!(matches!(
            source.poll_next_item(),
            Err(SqliteCdcError::Diesel(
                diesel::result::Error::DatabaseError(_, _)
            ))
        ));
        writer.batch_execute("ROLLBACK").expect("release lock");
        let event = event_of(&mut source).expect("recorded insert survives");
        assert_eq!(event.kind(), crate::EventKind::Insert);
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 1).unwrap(),
            Value::Int(250)
        );
        assert!(source.poll_next_item().expect("drained").is_none());
    }

    #[test]
    fn unsupported_virtual_tables_do_not_block_tracked_regular_tables() {
        use diesel::connection::SimpleConnection;
        use diesel::ExpressionMethods;

        diesel::table! {
            _pad (id) {
                id -> Integer,
            }
        }

        for ddl in [
            "CREATE VIRTUAL TABLE orders USING fts5(status)",
            "CREATE VIRTUAL TABLE orders USING fts5(status, tokenize = 'porter')",
        ] {
            let (mut connection, catalog) = open_orders_db();
            connection
                .batch_execute("DROP TABLE orders")
                .expect("drop orders");
            connection.batch_execute(ddl).expect("virtual table");
            let mut source = SqliteCdcSource::new(connection, catalog).expect("source");
            diesel::insert_into(_pad::table)
                .values(_pad::id.eq(7))
                .execute(source.connection())
                .expect("insert into tracked table");
            let event = event_of(&mut source).expect("regular table insert");
            let pad = crate::catalog_helpers::table_id::<crate::backend::SQLite, _>(
                source.catalog(),
                "_pad",
            )
            .expect("tracked regular table");
            assert_eq!(event.table_id(source.catalog()), pad);
            assert_eq!(
                event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
                Value::Int(7)
            );
            assert!(source.poll_next_item().expect("drained").is_none());
        }
    }

    #[test]
    fn live_session_updates_and_deletes_preserve_catalog_column_identity() {
        use diesel::connection::SimpleConnection;
        use diesel::ExpressionMethods;

        diesel::table! {
            orders (id) {
                extra -> Text,
                status -> Text,
                id -> Integer,
            }
        }

        let (mut connection, catalog) = open_orders_db();
        connection
            .batch_execute(
                "DROP TABLE orders;
                 CREATE TABLE orders (extra TEXT, status TEXT, id INTEGER PRIMARY KEY)",
            )
            .expect("live table has reordered and replaced columns");
        diesel::insert_into(orders::table)
            .values((
                orders::extra.eq("x"),
                orders::status.eq("pending"),
                orders::id.eq(7),
            ))
            .execute(&mut connection)
            .expect("seed row");
        let mut source = SqliteCdcSource::new(connection, catalog).expect("source");
        diesel::update(orders::table)
            .set((orders::status.eq("paid"), orders::extra.eq("y")))
            .execute(source.connection())
            .expect("update live columns");
        let update = event_of(&mut source).expect("update");
        assert_eq!(update.kind(), crate::EventKind::Update);
        assert_eq!(update.pk_columns(source.catalog()), &[0u16]);
        assert_eq!(update.changed_columns(source.catalog()), vec![2u16]);
        assert_eq!(
            update.value_at(source.catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            update.value_at(source.catalog(), RowKind::New, 1).unwrap(),
            Value::Missing
        );
        assert_eq!(
            update.value_at(source.catalog(), RowKind::Old, 2).unwrap(),
            Value::String("pending".into())
        );
        assert_eq!(
            update.value_at(source.catalog(), RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );

        diesel::delete(orders::table)
            .execute(source.connection())
            .expect("delete live row");
        let delete = event_of(&mut source).expect("delete");
        assert_eq!(delete.kind(), crate::EventKind::Delete);
        assert_eq!(delete.pk_columns(source.catalog()), &[0u16]);
        assert_eq!(delete.changed_columns(source.catalog()), [] as [u16; 0]);
        assert_eq!(
            delete.value_at(source.catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            delete.value_at(source.catalog(), RowKind::Old, 1).unwrap(),
            Value::Missing
        );
        assert_eq!(
            delete.value_at(source.catalog(), RowKind::Old, 2).unwrap(),
            Value::String("paid".into())
        );
        assert!(source.poll_next_item().expect("drained").is_none());
    }

    #[test]
    fn typed_sqlite_source_impls_cdc_source() {
        use crate::testing::block_on;

        fn assert_cdc_source_send<S: crate::CdcSource + Send>() {}
        assert_cdc_source_send::<SqliteCdcSource<ParserDB>>();

        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (1, 42, 'ok')")
            .execute(source.connection())
            .expect("insert");

        let item = <SqliteCdcSource<ParserDB> as crate::CdcSource>::next_item(&mut source);
        let event = block_on(item)
            .expect("next_item succeeds")
            .and_then(SourceItem::into_event)
            .expect("one event pending");
        assert_eq!(event.kind(), crate::EventKind::Insert);
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(1)
        );

        let ack =
            <SqliteCdcSource<ParserDB> as crate::CdcSource>::ack(&mut source, crate::NoCheckpoint);
        block_on(ack).expect("ack succeeds");
    }
}
