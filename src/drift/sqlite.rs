//! The SQLite live catalog, behind `sqlite-cdc`.
//!
//! A [`SqliteConnection`] loads its own catalog for the guard's live
//! reads: it reads the named tables' `CREATE` text from `sqlite_schema`
//! and parses it with [`ParserDB`], so the guard judges a table the way
//! the database has it now rather than the way the stream's ordinals
//! suggest. The stream carries only names and positions; the type,
//! nullability, default and `CHECK` facts a judgment needs come from the
//! DDL text. [`InlineLiveReader`](super::InlineLiveReader) serves it to the
//! guard on any executor.

use diesel::{
    ExpressionMethods, NullableExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection,
};
use sql_traits::errors::LookupError;
use sql_traits::prelude::{DatabaseLike, TableLike};
use sql_traits::structs::{ParserDB, TargetName};
use sqlparser::dialect::SQLiteDialect;

use super::guard::DriftTableSignal;
use super::live::LiveCatalog;
use crate::backend::SQLite;
use crate::sqlite_cdc::SqliteSchemaSignal;
use crate::types::TableId;

mod catalog {
    diesel::table! {
        /// SQLite's catalog of the schema's tables, indexes, views and triggers.
        sqlite_schema (name) {
            #[sql_name = "type"]
            kind -> Text,
            name -> Text,
            sql -> Nullable<Text>,
        }
    }
}
use catalog::sqlite_schema;

/// Errors a SQLite live catalog load can surface.
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

impl LiveCatalog for SqliteConnection {
    type Database = ParserDB;
    type Error = SqliteShapeError;

    /// Parse the stored `CREATE` text of each named table, by name alone,
    /// since `sqlite_schema` lists the tables of one database. A table whose
    /// row stores no text reads as absent, and one whose text declares no
    /// table sql-traits models, such as an FTS5 virtual table, fails.
    fn load_tables(&mut self, tables: &[TargetName<'_>]) -> Result<ParserDB, SqliteShapeError> {
        let names: Vec<&str> = tables.iter().map(TargetName::name).collect();
        let stored: Vec<(String, String)> = sqlite_schema::table
            .filter(sqlite_schema::kind.eq("table"))
            .filter(sqlite_schema::name.eq_any(names))
            .filter(sqlite_schema::sql.is_not_null())
            .select((sqlite_schema::name, sqlite_schema::sql.assume_not_null()))
            .load(self)?;
        let ddl: Vec<&str> = stored.iter().map(|(_, sql)| sql.as_str()).collect();
        let database = ParserDB::parse::<SQLiteDialect>(&ddl.join(";\n"))?;
        if let Some((name, _)) = stored.iter().find(|(name, _)| {
            !database
                .tables()
                .any(|table| table.table_name() == name.as_str())
        }) {
            return Err(LookupError::TableNotFound {
                object_name: name.clone(),
            }
            .into());
        }
        Ok(database)
    }

    /// The loaded table stored under the name, whatever schema it names.
    fn find_table<'d>(
        database: &'d ParserDB,
        table: TargetName<'_>,
    ) -> Result<Option<&'d <ParserDB as DatabaseLike>::Table>, SqliteShapeError> {
        Ok(database
            .tables()
            .find(|candidate| candidate.table_name() == table.name()))
    }
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

    use crate::drift::{InlineLiveReader, LiveShapeReader};

    /// An unqualified, unquoted name.
    fn named(table: &str) -> TargetName<'_> {
        TargetName::new(table, false)
    }

    fn table_names(database: &ParserDB) -> Vec<&str> {
        database.tables().map(TableLike::table_name).collect()
    }

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
        let mut reader = InlineLiveReader::new(connection);
        let shape = reader
            .shape(named("t1"))
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
    async fn a_read_ignores_the_schema_of_the_name() {
        let (_dir, connection) = live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY)"]);
        let mut reader = InlineLiveReader::new(connection);
        for schema in ["main", "sales"] {
            let shape = reader
                .shape(named("t1").with_schema(schema, false))
                .await
                .expect("the read succeeds");
            assert!(shape.is_some(), "t1 reads under the schema {schema}");
        }
    }

    #[tokio::test]
    async fn a_table_the_database_lacks_reads_as_missing() {
        let (_dir, connection) = live_db(&["CREATE TABLE t1 (id INTEGER)"]);
        let mut reader = InlineLiveReader::new(connection);
        assert_eq!(
            reader.shape(named("t2")).await.expect("the read succeeds"),
            None
        );
    }

    #[test]
    fn a_load_parses_only_the_named_tables_the_database_has() {
        let (_dir, mut connection) = live_db(&[
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY)",
            "CREATE TABLE t2 (id INTEGER PRIMARY KEY)",
            "CREATE TABLE t3 (id INTEGER PRIMARY KEY)",
            "CREATE INDEX t1_id ON t1 (id)",
        ]);
        let database = connection
            .load_tables(&["t1", "t3", "t1_id", "absent"].map(named))
            .expect("the load succeeds");
        assert_eq!(table_names(&database), ["t1", "t3"]);
        let empty = connection
            .load_tables(&[named("absent")])
            .expect("the load succeeds");
        assert!(empty.tables().next().is_none(), "no named table exists");
    }

    #[tokio::test]
    async fn a_reader_sees_an_added_column_on_its_next_read() {
        let (_dir, connection) = live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)"]);
        let mut reader = InlineLiveReader::new(connection);
        assert_eq!(
            reader
                .shape(named("t1"))
                .await
                .expect("read")
                .expect("t1")
                .columns()
                .len(),
            2
        );
        raw("ALTER TABLE t1 ADD COLUMN extra TEXT")
            .execute(reader.connection())
            .expect("the column is added");
        let shape = reader.shape(named("t1")).await.expect("read").expect("t1");
        assert_eq!(shape.columns().len(), 3);
        assert_eq!(shape.columns()[2].name(), "extra");
        assert!(shape.columns()[2].nullable());
    }

    #[tokio::test]
    async fn a_reader_sees_a_dropped_column_on_its_next_read() {
        let (_dir, connection) =
            live_db(&["CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)"]);
        let mut reader = InlineLiveReader::new(connection);
        raw("ALTER TABLE t1 DROP COLUMN v")
            .execute(reader.connection())
            .expect("the column is dropped");
        let shape = reader.shape(named("t1")).await.expect("read").expect("t1");
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
        let mut reader = InlineLiveReader::new(connection);
        raw("ALTER TABLE t1 RENAME TO t2")
            .execute(reader.connection())
            .expect("the table is renamed");
        assert_eq!(reader.shape(named("t1")).await.expect("read"), None);
        assert!(reader.shape(named("t2")).await.expect("read").is_some());
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
        let mut reader = InlineLiveReader::new(connection);
        let shape = reader.shape(named("t1")).await.expect("read").expect("t1");
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
        let mut reader = InlineLiveReader::new(connection);
        assert!(matches!(
            reader.shape(named("t1")).await,
            Err(SqliteShapeError::Diesel(
                diesel::result::Error::DatabaseError(_, _)
            ))
        ));
        raw("ROLLBACK")
            .execute(&mut writer)
            .expect("release the database lock");
        let shape = reader
            .shape(named("t1"))
            .await
            .expect("read resumes")
            .expect("t1");
        assert_eq!(shape.primary_key(), &["id".to_string()]);
    }

    #[tokio::test]
    async fn a_virtual_table_without_a_catalog_shape_reports_a_lookup_error() {
        let (_dir, connection) = live_db(&["CREATE VIRTUAL TABLE t1 USING fts5(body)"]);
        let mut reader = InlineLiveReader::new(connection);
        assert!(matches!(
            reader.shape(named("t1")).await,
            Err(SqliteShapeError::Lookup(
                sql_traits::errors::LookupError::TableNotFound { object_name }
            )) if object_name == "t1"
        ));
    }

    #[tokio::test]
    async fn unsupported_virtual_table_arguments_report_a_parse_error() {
        let (_dir, connection) =
            live_db(&["CREATE VIRTUAL TABLE t1 USING fts5(body, tokenize = 'porter')"]);
        let mut reader = InlineLiveReader::new(connection);
        assert!(matches!(
            reader.shape(named("t1")).await,
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
