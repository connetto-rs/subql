//! The PostgreSQL live catalog, behind the `postgres-14` to `postgres-18`
//! features.
//!
//! A [`PgLiveCatalog`] loads the named tables through pg_diesel, so the
//! guard judges each one as PostgreSQL holds it now. Its shape adds two
//! facts pg_diesel's `DatabaseLike` does not model, the replica identity
//! `pg_class.relreplident` records and what the source's publication
//! streams of the table, read from `pg_publication` and
//! `pg_publication_tables`. Each `postgres-N` feature compiles the catalog
//! of one server major version, and every read takes the catalog of the
//! server it reaches.

use diesel::sql_types::Bool;
use diesel::{ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl};
use pg_diesel::database::{
    CompiledVersions, PgDatabaseBuildError, PgDieselDatabaseBuilder, PgDieselProfile,
    ServerVersion, VersionCatalog,
};
use pg_diesel::dialect::PostgresDialect;
use pg_diesel::model_metadata::PgTable;
use pg_diesel::schema::pg_catalog::pg_namespace::pg_namespace;
use pg_diesel::PgDieselDatabase;
use sql_traits::errors::LookupError;
use sql_traits::prelude::{DatabaseLike, TableLike};
use sql_traits::structs::generic_db::GenericDBBuilder;
use sql_traits::structs::TargetName;

use super::live::LiveCatalog;
use super::shape::{Publication, PublishedChange, ReplicaIdentity, TableShape};
use crate::backend::{Backend, Postgres};

diesel::define_sql_function! {
    /// The database the session is connected to.
    fn current_database() -> Text
}

diesel::define_sql_function! {
    /// The schemas the session's search path names that exist, with the
    /// implicitly searched ones when `include_implicit` holds.
    fn current_schemas(include_implicit: Bool) -> Array<Text>
}

/// Errors a PostgreSQL live catalog read can surface.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgShapeError {
    /// pg_diesel could not load the live catalog, or the server runs a
    /// major version no `postgres-N` feature compiled.
    #[error("live catalog load: {0}")]
    Load(#[from] PgDatabaseBuildError),
    /// A name, column, key or check lookup on the loaded catalog failed.
    #[error("live table lookup: {0}")]
    Lookup(#[from] LookupError),
    /// A read against the live database failed.
    #[error("live table read: {0}")]
    Diesel(#[from] diesel::result::Error),
    /// `pg_class.relreplident` holds a code PostgreSQL does not document.
    #[error("replica identity code `{0}` is none of PostgreSQL's")]
    UnknownReplicaIdentity(String),
}

/// A connection to a live PostgreSQL database and the publication its
/// source streams, which a [`DriftGuard`](super::DriftGuard) reads tables
/// through.
pub struct PgLiveCatalog {
    connection: PgConnection,
    publication: String,
}

impl PgLiveCatalog {
    /// A live catalog over `connection` whose source streams `publication`.
    #[must_use]
    pub fn new(connection: PgConnection, publication: impl Into<String>) -> Self {
        Self {
            connection,
            publication: publication.into(),
        }
    }

    /// The connection the catalog reads through.
    pub const fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }
}

impl LiveCatalog for PgLiveCatalog {
    type Backend = Postgres;
    type Database = PgDieselDatabase;
    type Error = PgShapeError;

    /// Load the named tables with pg_diesel. A name with a schema is looked
    /// up in that schema, and one without in every schema of the session's
    /// search path, where [`find_table`](Self::find_table) resolves it as
    /// PostgreSQL does.
    fn load_tables(&mut self, tables: &[TargetName<'_>]) -> Result<PgDieselDatabase, PgShapeError> {
        let case = <Postgres as Backend>::WRITTEN_TABLE_NAME_CASE;
        let search_path: Vec<String> = if tables.iter().any(|table| table.schema().is_none()) {
            diesel::select(current_schemas(false)).get_result(&mut self.connection)?
        } else {
            Vec::new()
        };
        let mut selected = Vec::new();
        for table in tables {
            let name = case.compared_form(table.name(), table.name_is_quoted());
            match table.schema() {
                Some(schema) => {
                    selected.push((case.compared_form(schema, table.schema_is_quoted()), name));
                }
                None => selected.extend(
                    search_path
                        .iter()
                        .map(|schema| (schema.as_str().into(), name.clone())),
                ),
            }
        }
        let catalog: String =
            diesel::select(current_database()).get_result(&mut self.connection)?;
        if selected.is_empty() {
            // pg_diesel loads at least one schema, and no name reaches one.
            return Ok(
                GenericDBBuilder::<PgDieselProfile>::new(catalog, PostgresDialect)
                    .into_database(None),
            );
        }
        Ok(PgDieselDatabase::try_from(
            PgDieselDatabaseBuilder::default()
                .connection(&mut self.connection)
                .catalog(catalog)
                .schemas(selected.iter().map(|(schema, _)| schema))
                .tables(selected.iter().map(|(schema, name)| (schema, name))),
        )?)
    }

    /// The loaded table the name resolves to, through the session's search
    /// path when it names no schema.
    fn find_table<'d>(
        database: &'d PgDieselDatabase,
        table: TargetName<'_>,
    ) -> Result<Option<&'d PgTable>, PgShapeError> {
        Ok(database.resolve_target_table(table, <Postgres as Backend>::WRITTEN_TABLE_NAME_CASE)?)
    }

    /// The shape sql-traits reads from the loaded table, with the replica
    /// identity the table records and what the publication streams of it.
    fn table_shape(
        &mut self,
        database: &PgDieselDatabase,
        table: &PgTable,
    ) -> Result<TableShape, PgShapeError> {
        let shape = TableShape::from_table::<Postgres, _>(table, database)?;
        let facts = read_facts(
            &mut self.connection,
            &self.publication,
            table.schema(),
            table.table_name(),
        )?;
        Ok(shape
            .with_replica_identity(replica_identity(&facts.replica_identity)?)
            .with_publication(facts.publication()))
    }
}

/// What the catalog of one server major version says of a table beyond its
/// shape.
struct LiveFacts {
    /// `pg_class.relreplident`.
    replica_identity: String,
    /// The publication's `pubinsert`, `pubupdate`, `pubdelete` and
    /// `pubtruncate` in that order, or `None` when no publication has its
    /// name.
    operations: Option<[bool; 4]>,
    /// The table's `pg_publication_tables` row as its column list and row
    /// filter, or `None` when the publication does not carry the table.
    member: Option<(Option<Vec<String>>, Option<String>)>,
}

impl LiveFacts {
    fn publication(self) -> Publication {
        let (Some(operations), Some((columns, row_filter))) = (self.operations, self.member) else {
            return Publication::Unpublished;
        };
        Publication::Published {
            operations: operations
                .into_iter()
                .zip(PublishedChange::ALL)
                .filter_map(|(published, change)| published.then_some(change))
                .collect(),
            columns,
            row_filter,
        }
    }
}

/// Read [`LiveFacts`] under the catalog module `$version`. Publication
/// column lists and row filters start in PostgreSQL 15, so a catalog
/// `without_lists` reads membership alone.
macro_rules! facts_under {
    (@member without_lists, $query:expr, $connection:expr) => {
        $query
            .select(pg_publication_tables::pubname)
            .first::<Option<String>>($connection)
            .optional()?
            .map(|_| (None, None))
    };
    (@member with_lists, $query:expr, $connection:expr) => {
        $query
            .select((
                pg_publication_tables::attnames,
                pg_publication_tables::rowfilter,
            ))
            .first::<(Option<Vec<String>>, Option<String>)>($connection)
            .optional()?
    };
    ($version:ident, $lists:ident, $connection:expr, $namespace:expr, $publication:expr, $schema:expr, $table:expr) => {{
        use pg_diesel::$version::schema::pg_catalog::{
            pg_class::pg_class, pg_publication::pg_publication,
            pg_publication_tables::pg_publication_tables,
        };
        let replica_identity = pg_class::table
            .filter(pg_class::relnamespace.eq($namespace))
            .filter(pg_class::relname.eq($table))
            .select(pg_class::relreplident)
            .first::<String>($connection)?;
        let operations = pg_publication::table
            .filter(pg_publication::pubname.eq($publication))
            .select((
                pg_publication::pubinsert,
                pg_publication::pubupdate,
                pg_publication::pubdelete,
                pg_publication::pubtruncate,
            ))
            .first::<(bool, bool, bool, bool)>($connection)
            .optional()?
            .map(<[bool; 4]>::from);
        let member = facts_under!(
            @member $lists,
            pg_publication_tables::table
                .filter(pg_publication_tables::pubname.eq($publication))
                .filter(pg_publication_tables::schemaname.eq($schema))
                .filter(pg_publication_tables::tablename.eq($table)),
            $connection
        );
        LiveFacts {
            replica_identity,
            operations,
            member,
        }
    }};
}

/// Read what the server's catalog says of `schema`.`table` beyond its
/// shape, under the catalog of the server's major version.
fn read_facts(
    connection: &mut PgConnection,
    publication: &str,
    schema: &str,
    table: &str,
) -> Result<LiveFacts, PgShapeError> {
    let namespace: u32 = pg_namespace::table
        .filter(pg_namespace::nspname.eq(schema))
        .select(pg_namespace::oid)
        .first(connection)?;
    Ok(match ServerVersion::load(connection)?.major() {
        #[cfg(feature = "postgres-14")]
        pg_diesel::v14::MAJOR => facts_under!(
            v14,
            without_lists,
            connection,
            namespace,
            publication,
            schema,
            table
        ),
        #[cfg(feature = "postgres-15")]
        pg_diesel::v15::MAJOR => facts_under!(
            v15,
            with_lists,
            connection,
            namespace,
            publication,
            schema,
            table
        ),
        #[cfg(feature = "postgres-16")]
        pg_diesel::v16::MAJOR => facts_under!(
            v16,
            with_lists,
            connection,
            namespace,
            publication,
            schema,
            table
        ),
        #[cfg(feature = "postgres-17")]
        pg_diesel::v17::MAJOR => facts_under!(
            v17,
            with_lists,
            connection,
            namespace,
            publication,
            schema,
            table
        ),
        #[cfg(feature = "postgres-18")]
        pg_diesel::v18::MAJOR => facts_under!(
            v18,
            with_lists,
            connection,
            namespace,
            publication,
            schema,
            table
        ),
        server => {
            return Err(PgDatabaseBuildError::UnsupportedServerVersion {
                server,
                compiled: CompiledVersions::MAJORS,
            }
            .into())
        }
    })
}

/// The identity a `pg_class.relreplident` code names.
fn replica_identity(code: &str) -> Result<ReplicaIdentity, PgShapeError> {
    match code {
        "d" => Ok(ReplicaIdentity::Default),
        "n" => Ok(ReplicaIdentity::Nothing),
        "i" => Ok(ReplicaIdentity::Index),
        "f" => Ok(ReplicaIdentity::Full),
        other => Err(PgShapeError::UnknownReplicaIdentity(other.to_owned())),
    }
}
