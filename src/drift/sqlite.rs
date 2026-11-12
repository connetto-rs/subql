//! The SQLite live shape reader, behind `sqlite-cdc`.
//!
//! Reads `sqlite_schema` on the reader's own [`SqliteConnection`] and
//! parses each table's `CREATE` text with [`ParserDB`], so the guard
//! judges the table the way the database has it now rather than the way
//! the stream's ordinals suggest. The stream carries only names and
//! positions; the type, nullability, default and `CHECK` facts a
//! judgment needs come from the DDL text.

use diesel::SqliteConnection;
use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;

use super::guard::{DriftTableSignal, LiveShapeReader};
use super::shape::TableShape;
use crate::backend::SQLite;
use crate::sqlite_cdc::SqliteSchemaSignal;
use crate::types::TableId;

/// Errors a [`SqliteLiveReader`] read can surface.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteShapeError {
    /// The table's `CREATE` text did not parse.
    #[error("live table DDL parse: {0}")]
    Parse(#[from] sql_traits::errors::Error),
    /// A column, key or check lookup on the parsed table failed.
    #[error("live table lookup: {0}")]
    Lookup(#[from] sql_traits::errors::LookupError),
    /// The read against the live database failed.
    #[error("live table read: {0}")]
    Diesel(#[from] diesel::result::Error),
}

/// The live reader for a SQLite database.
///
/// The reader owns its own connection to the live database. Writes
/// through any other connection bypass the source's session and its
/// changes, but never this read, which sees the database as committed.
pub struct SqliteLiveReader {
    connection: SqliteConnection,
}

impl SqliteLiveReader {
    /// Construct a reader over a connection to the live database.
    #[must_use]
    pub const fn new(connection: SqliteConnection) -> Self {
        Self { connection }
    }
}

impl LiveShapeReader for SqliteLiveReader {
    type Error = SqliteShapeError;

    fn shape<'r>(
        &'r mut self,
        table: &'r str,
    ) -> impl core::future::Future<Output = Result<Option<TableShape>, Self::Error>> + Send + 'r
    {
        let connection = &mut self.connection;
        core::future::ready(read_shape(connection, table))
    }
}

/// One row of the `sqlite_schema` read.
#[derive(QueryableByName, Debug)]
struct SchemaRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    sql: Option<String>,
}

/// The live shape of `table`, or `None` when the database has no such
/// table.
fn read_shape(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<Option<TableShape>, SqliteShapeError> {
    // `sqlite_schema` is the SQLite catalog table the typed DSL has no
    // schema for, so the read is raw, the way the source's own
    // `sqlite_schema` reads are.
    let rows: Vec<SchemaRow> =
        sql_query("SELECT name, sql FROM sqlite_schema WHERE type = 'table'").load(connection)?;
    let Some(ddl) = rows
        .iter()
        .find(|row| row.name == table)
        .and_then(|row| row.sql.as_deref())
    else {
        // A table the database lacks, or one whose row carries no
        // stored DDL, reads as missing.
        return Ok(None);
    };
    let database = ParserDB::parse::<SQLiteDialect>(ddl)?;
    let missing = sql_traits::errors::LookupError::TableNotFound {
        object_name: table.to_string(),
    };
    let live_table = database.tables().next().ok_or(missing)?;
    Ok(Some(TableShape::from_table(live_table, &database)?))
}

/// The SQLite source's schema signal names its table by the bare
/// attached name, which the catalog answers with its id.
impl DriftTableSignal for SqliteSchemaSignal {
    fn table<DB: DatabaseLike>(&self, catalog: &DB) -> Option<TableId> {
        let name = match self {
            Self::SchemaChanged { table, .. } | Self::ChangesLost { table } => table,
        };
        crate::catalog_helpers::table_id::<SQLite, DB>(catalog, name)
    }

    fn changes_lost(&self) -> bool {
        matches!(self, Self::ChangesLost { .. })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use diesel::{sql_query as raw, Connection};

    fn live_db(statements: &[&str]) -> (tempfile::TempDir, SqliteConnection) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("live.sqlite");
        let mut connection = SqliteConnection::establish(path.to_str().expect("path is UTF-8"))
            .expect("open the live database");
        for statement in statements {
            raw(*statement)
                .execute(&mut connection)
                .expect("DDL applies");
        }
        (dir, connection)
    }

    #[tokio::test]
    async fn a_reader_reads_the_shape_the_database_declares() {
        let (_dir, connection) = live_db(&[
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0), note TEXT)",
        ]);
        let mut reader = SqliteLiveReader::new(connection);
        let shape = reader
            .shape("t1")
            .await
            .expect("the read succeeds")
            .expect("t1 exists");
        let names: Vec<String> = shape
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        assert_eq!(
            names,
            vec!["id".to_string(), "v".to_string(), "note".to_string()]
        );
        assert_eq!(shape.primary_key(), &["id".to_string()]);
        assert_eq!(shape.checks(), &["v > 0".to_string()]);
        assert!(shape.columns()[1].nullable(), "a plain column is nullable");
    }

    #[tokio::test]
    async fn a_table_the_database_lacks_reads_as_missing() {
        let (_dir, connection) = live_db(&["CREATE TABLE t1 (id INTEGER)"]);
        let mut reader = SqliteLiveReader::new(connection);
        assert_eq!(reader.shape("t2").await.expect("the read succeeds"), None);
    }

    #[tokio::test]
    async fn a_reader_sees_an_added_column_on_its_next_read() {
        let (_dir, connection) = live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)"]);
        let mut reader = SqliteLiveReader::new(connection);
        assert_eq!(
            reader
                .shape("t1")
                .await
                .expect("read")
                .expect("t1")
                .columns()
                .len(),
            2
        );
        raw("ALTER TABLE t1 ADD COLUMN extra TEXT")
            .execute(&mut reader.connection)
            .expect("the column is added");
        let shape = reader.shape("t1").await.expect("read").expect("t1");
        assert_eq!(shape.columns().len(), 3);
        assert_eq!(shape.columns()[2].name(), "extra");
        assert!(shape.columns()[2].nullable());
    }

    #[tokio::test]
    async fn a_reader_sees_a_dropped_column_on_its_next_read() {
        let (_dir, connection) =
            live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)"]);
        let mut reader = SqliteLiveReader::new(connection);
        raw("ALTER TABLE t1 DROP COLUMN v")
            .execute(&mut reader.connection)
            .expect("the column is dropped");
        let shape = reader.shape("t1").await.expect("read").expect("t1");
        let names: Vec<String> = shape
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        assert_eq!(names, vec!["id".to_string(), "note".to_string()]);
    }

    #[tokio::test]
    async fn a_reader_sees_a_renamed_table_under_its_new_name() {
        let (_dir, connection) = live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY)"]);
        let mut reader = SqliteLiveReader::new(connection);
        raw("ALTER TABLE t1 RENAME TO t2")
            .execute(&mut reader.connection)
            .expect("the table is renamed");
        assert_eq!(reader.shape("t1").await.expect("read"), None);
        assert!(reader.shape("t2").await.expect("read").is_some());
    }

    #[tokio::test]
    async fn a_reader_sees_a_changed_check_after_a_rebuild() {
        let statements = [
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0))",
            "CREATE TABLE t1_new (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > -5))",
            "INSERT INTO t1_new SELECT id, v FROM t1",
            "DROP TABLE t1",
            "ALTER TABLE t1_new RENAME TO t1",
        ];
        let (_dir, connection) = live_db(&statements);
        let mut reader = SqliteLiveReader::new(connection);
        let shape = reader.shape("t1").await.expect("read").expect("t1");
        assert_eq!(shape.checks(), &["v > -5".to_string()]);
    }

    #[tokio::test]
    async fn a_reader_reports_a_locked_database_and_recovers_after_rollback() {
        let (dir, mut connection) = live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY)"]);
        raw("PRAGMA busy_timeout = 0")
            .execute(&mut connection)
            .expect("disable lock waiting");
        let path = dir.path().join("live.sqlite");
        let mut writer =
            SqliteConnection::establish(path.to_str().expect("UTF-8 path")).expect("open writer");
        raw("BEGIN EXCLUSIVE")
            .execute(&mut writer)
            .expect("hold the database lock");
        let mut reader = SqliteLiveReader::new(connection);
        assert!(matches!(
            reader.shape("t1").await,
            Err(SqliteShapeError::Diesel(
                diesel::result::Error::DatabaseError(_, _)
            ))
        ));
        raw("ROLLBACK")
            .execute(&mut writer)
            .expect("release the database lock");
        let shape = reader.shape("t1").await.expect("read resumes").expect("t1");
        assert_eq!(shape.primary_key(), &["id".to_string()]);
    }

    #[tokio::test]
    async fn a_virtual_table_without_a_catalog_shape_reports_a_lookup_error() {
        let (_dir, connection) = live_db(&["CREATE VIRTUAL TABLE t1 USING fts5(body)"]);
        let mut reader = SqliteLiveReader::new(connection);
        assert!(matches!(
            reader.shape("t1").await,
            Err(SqliteShapeError::Lookup(
                sql_traits::errors::LookupError::TableNotFound { object_name }
            )) if object_name == "t1"
        ));
    }

    #[tokio::test]
    async fn unsupported_virtual_table_arguments_report_a_parse_error() {
        let (_dir, connection) =
            live_db(&["CREATE VIRTUAL TABLE t1 USING fts5(body, tokenize = 'porter')"]);
        let mut reader = SqliteLiveReader::new(connection);
        assert!(matches!(
            reader.shape("t1").await,
            Err(SqliteShapeError::Parse(_))
        ));
    }

    #[test]
    fn schema_signals_resolve_tracked_tables_and_distinguish_lost_changes() {
        let catalog = ParserDB::parse::<SQLiteDialect>("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
            .expect("catalog");
        let id =
            crate::catalog_helpers::table_id::<SQLite, _>(&catalog, "t1").expect("tracked table");
        for (name, expected) in [("t1", Some(id)), ("untracked", None)] {
            let changed = SqliteSchemaSignal::SchemaChanged {
                table: name.to_string(),
                create_sql: None,
            };
            let lost = SqliteSchemaSignal::ChangesLost {
                table: name.to_string(),
            };
            assert_eq!(changed.table(&catalog), expected);
            assert_eq!(lost.table(&catalog), expected);
            assert!(!changed.changes_lost());
            assert!(lost.changes_lost());
        }
    }
}
