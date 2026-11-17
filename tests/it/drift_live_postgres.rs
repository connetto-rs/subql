//! The PostgreSQL live catalog against stock PostgreSQL servers, 18 for the
//! judgments and each other major version for the catalog it is read under.
//!
//! Every test creates tables on a fresh database, reads them through an
//! `InlineLiveReader` over a `PgLiveCatalog`, and judges each live shape
//! against the shape of the same DDL parsed as the consumer's catalog.
#![allow(clippy::unwrap_used)]

use std::fmt::Write as _;

use diesel::connection::SimpleConnection;
use pg_diesel::database::PgDatabaseBuildError;
use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::{ParserDB, TargetName};
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::drift::{
    judge, Incompatibility, InlineLiveReader, LiveCatalog, LiveShapeReader, PgLiveCatalog,
    PgShapeError, PublishedChange, ReplicaIdentity, TableShape, Verdict,
};
use subql::testing::block_on;

use crate::common::{assert_docker_available, stock_pg_database, PgDatabase};

/// The publication every reader is built with.
const PUBLICATION: &str = "subql";

/// Run `statements` on a fresh connection to `database`.
fn run(database: &PgDatabase, statements: &str) {
    // Fixture DDL, which the query DSL does not express.
    database.connect().batch_execute(statements).unwrap();
}

/// A reader over a fresh connection to `database` that has run `setup`.
fn reader(database: &PgDatabase, setup: &str) -> InlineLiveReader<PgLiveCatalog> {
    let mut connection = database.connect();
    // Fixture DDL and session settings, which the query DSL does not express.
    connection.batch_execute(setup).unwrap();
    InlineLiveReader::new(PgLiveCatalog::new(connection, PUBLICATION))
}

/// The catalog shape of the one table `ddl` declares.
fn catalog_shape(ddl: &str) -> TableShape {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap();
    let table = catalog.tables().next().unwrap();
    TableShape::from_table::<Postgres, _>(table, &catalog).unwrap()
}

/// The live shape of the table `name` names.
fn live_shape(reader: &mut InlineLiveReader<PgLiveCatalog>, name: &str) -> Option<TableShape> {
    block_on(reader.shape(TargetName::parse(name).unwrap())).unwrap()
}

/// End every session on `database` but the one this call opens, waiting
/// until each has ended.
fn terminate_other_sessions(database: &PgDatabase) {
    run(
        database,
        "SELECT pg_terminate_backend(pid, 10000) FROM pg_stat_activity
         WHERE datname = current_database() AND pid <> pg_backend_pid()",
    );
}

/// The tables of the PostgreSQL 18 parity probe, each name with its DDL.
const PARITY: &[(&str, &str)] = &[
    ("p_lower", "CREATE TABLE p_lower (id INT PRIMARY KEY, amount INT)"),
    ("p_unquoted_mixed", "CREATE TABLE p_unquoted_mixed (id INT PRIMARY KEY, Amount INT)"),
    ("p_quoted_mixed", "CREATE TABLE p_quoted_mixed (id INT PRIMARY KEY, \"Amount\" INT)"),
    ("p_mixed_key", "CREATE TABLE p_mixed_key (ID INT PRIMARY KEY, v INT)"),
    ("p_check_plain", "CREATE TABLE p_check_plain (id INT PRIMARY KEY, v INT CHECK (v > 0))"),
    ("p_check_and", "CREATE TABLE p_check_and (id INT PRIMARY KEY, v INT CHECK (v > 0 AND v < 10))"),
    ("p_check_mixed", "CREATE TABLE p_check_mixed (id INT PRIMARY KEY, v INT CHECK (V > 0))"),
    ("p_check_text", "CREATE TABLE p_check_text (id INT PRIMARY KEY, s TEXT CHECK (s <> ''))"),
    ("p_check_in", "CREATE TABLE p_check_in (id INT PRIMARY KEY, s TEXT CHECK (s IN ('a', 'b')))"),
    ("p_check_table", "CREATE TABLE p_check_table (id INT PRIMARY KEY, lo INT, hi INT, CHECK (lo <= hi))"),
    ("p_defaults", "CREATE TABLE p_defaults (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 0, t TIMESTAMPTZ NOT NULL DEFAULT now())"),
    ("p_ints", "CREATE TABLE p_ints (id BIGINT PRIMARY KEY, a SMALLINT, b INTEGER, c INT4, d INT8, e SERIAL, f BIGSERIAL)"),
    ("p_floats", "CREATE TABLE p_floats (id INT PRIMARY KEY, a REAL, b DOUBLE PRECISION, c FLOAT, d FLOAT4, e FLOAT8)"),
    ("p_text", "CREATE TABLE p_text (id INT PRIMARY KEY, a TEXT, b VARCHAR(20), c CHARACTER VARYING(20), d CHAR(3), e CHARACTER(3), f VARCHAR)"),
    ("p_numeric", "CREATE TABLE p_numeric (id INT PRIMARY KEY, a NUMERIC(12,4), b DECIMAL(10,2), c NUMERIC)"),
    ("p_time", "CREATE TABLE p_time (id INT PRIMARY KEY, a DATE, b TIME, c TIMESTAMP, d TIMESTAMPTZ, e TIMESTAMP WITH TIME ZONE, f TIMESTAMP(3), g INTERVAL)"),
    ("p_misc", "CREATE TABLE p_misc (id INT PRIMARY KEY, a UUID, b JSONB, c JSON, d BYTEA, e BOOLEAN, f BOOL)"),
    ("p_arrays", "CREATE TABLE p_arrays (id INT PRIMARY KEY, a INT[], b TEXT[], c INTEGER ARRAY, d VARCHAR(5)[])"),
    ("p_composite_key", "CREATE TABLE p_composite_key (a INT, b TEXT, c INT, PRIMARY KEY (b, a))"),
    ("p_unique", "CREATE TABLE p_unique (id INT PRIMARY KEY, e TEXT UNIQUE NOT NULL)"),
];

#[test]
fn every_parity_table_reads_as_its_catalog_declares_it() {
    assert_docker_available();
    let database = stock_pg_database(18);
    let mut setup = String::new();
    for (name, ddl) in PARITY {
        write!(setup, "{ddl}; ALTER TABLE {name} REPLICA IDENTITY FULL;").unwrap();
    }
    write!(setup, "CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES;").unwrap();
    let mut reader = reader(&database, &setup);
    let mut drifted = Vec::new();
    for (name, ddl) in PARITY {
        let verdict = judge(&catalog_shape(ddl), live_shape(&mut reader, name).as_ref());
        if verdict != Verdict::Unchanged {
            drifted.push(format!("{name}: {verdict:?}"));
        }
    }
    assert!(drifted.is_empty(), "{drifted:#?}");
}

#[test]
fn each_identity_below_full_weakens_the_table() {
    assert_docker_available();
    let database = stock_pg_database(18);
    let ddl = "CREATE TABLE t1 (id INT PRIMARY KEY, v INT)";
    let mut reader = reader(
        &database,
        &format!("{ddl}; CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES;"),
    );
    for (clause, identity) in [
        ("DEFAULT", Some(ReplicaIdentity::Default)),
        ("NOTHING", Some(ReplicaIdentity::Nothing)),
        ("USING INDEX t1_pkey", Some(ReplicaIdentity::Index)),
        ("FULL", None),
    ] {
        run(
            &database,
            &format!("ALTER TABLE t1 REPLICA IDENTITY {clause}"),
        );
        let verdict = judge(&catalog_shape(ddl), live_shape(&mut reader, "t1").as_ref());
        assert_eq!(
            verdict,
            identity.map_or(Verdict::Unchanged, Verdict::IdentityWeakened),
            "REPLICA IDENTITY {clause}"
        );
    }
}

#[test]
fn a_table_the_publication_does_not_carry_is_unpublished() {
    assert_docker_available();
    let database = stock_pg_database(18);
    let ddl = "CREATE TABLE t2 (id INT PRIMARY KEY)";
    let mut reader = reader(
        &database,
        &format!(
            "CREATE TABLE t1 (id INT PRIMARY KEY); {ddl};
             ALTER TABLE t2 REPLICA IDENTITY FULL;"
        ),
    );
    assert_eq!(
        judge(&catalog_shape(ddl), live_shape(&mut reader, "t2").as_ref()),
        Verdict::Unpublished,
        "no publication has the reader's name"
    );
    run(
        &database,
        &format!("CREATE PUBLICATION {PUBLICATION} FOR TABLE t1"),
    );
    assert_eq!(
        judge(&catalog_shape(ddl), live_shape(&mut reader, "t2").as_ref()),
        Verdict::Unpublished,
        "the publication carries another table"
    );
}

/// Judge a table whose publication streams only its inserts, read from a
/// stock PostgreSQL `major` server. Column lists and row filters start in
/// PostgreSQL 15, so from that version on the publication also leaves out
/// a column and filters rows.
fn assert_publication_read_from(major: u32) {
    assert_docker_available();
    let database = stock_pg_database(major);
    let ddl = "CREATE TABLE t1 (id INT PRIMARY KEY, v INT, note TEXT)";
    let lists = major >= 15;
    let member = if lists {
        "t1 (id, v) WHERE (v > 0)"
    } else {
        "t1"
    };
    let mut reader = reader(
        &database,
        &format!(
            "{ddl}; ALTER TABLE t1 REPLICA IDENTITY FULL;
             CREATE PUBLICATION {PUBLICATION} FOR TABLE {member} WITH (publish = 'insert');"
        ),
    );
    let skipped = Incompatibility::OperationsUnpublished {
        operations: vec![
            PublishedChange::Update,
            PublishedChange::Delete,
            PublishedChange::Truncate,
        ],
    };
    let reasons = if lists {
        vec![
            Incompatibility::ColumnUnpublished {
                column: "note".into(),
            },
            skipped,
            Incompatibility::RowFiltered {
                filter: "(v > 0)".into(),
            },
        ]
    } else {
        vec![skipped]
    };
    assert_eq!(
        judge(&catalog_shape(ddl), live_shape(&mut reader, "t1").as_ref()),
        Verdict::Incompatible { reasons },
        "PostgreSQL {major}"
    );
}

#[cfg(feature = "postgres-14")]
#[test]
fn a_postgres_14_publication_reports_its_skipped_changes() {
    assert_publication_read_from(14);
}

#[cfg(feature = "postgres-15")]
#[test]
fn a_postgres_15_publication_reports_its_skipped_changes_column_list_and_row_filter() {
    assert_publication_read_from(15);
}

#[cfg(feature = "postgres-16")]
#[test]
fn a_postgres_16_publication_reports_its_skipped_changes_column_list_and_row_filter() {
    assert_publication_read_from(16);
}

#[cfg(feature = "postgres-17")]
#[test]
fn a_postgres_17_publication_reports_its_skipped_changes_column_list_and_row_filter() {
    assert_publication_read_from(17);
}

#[test]
fn a_postgres_18_publication_reports_its_skipped_changes_column_list_and_row_filter() {
    assert_publication_read_from(18);
}

#[test]
fn quoted_mixed_case_names_read_as_stored() {
    assert_docker_available();
    let database = stock_pg_database(18);
    let ddl = "CREATE SCHEMA \"Sales\";
               CREATE TABLE \"Sales\".\"Orders\" (id INT PRIMARY KEY, \"Amount\" INT, Note TEXT);";
    let mut reader = reader(
        &database,
        &format!(
            "{ddl}
             ALTER TABLE \"Sales\".\"Orders\" REPLICA IDENTITY FULL;
             CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES;"
        ),
    );
    assert_eq!(
        judge(
            &catalog_shape(ddl),
            live_shape(&mut reader, "\"Sales\".\"Orders\"").as_ref()
        ),
        Verdict::Unchanged
    );
    assert_eq!(live_shape(&mut reader, "Sales.Orders"), None);
}

#[test]
fn an_unqualified_name_resolves_through_the_search_path() {
    assert_docker_available();
    let database = stock_pg_database(18);
    let mut default_path = reader(
        &database,
        "CREATE SCHEMA app;
         CREATE TABLE app.t1 (id INT PRIMARY KEY, v INT);
         CREATE TABLE public.t1 (id INT PRIMARY KEY);",
    );
    let width = |shape: Option<TableShape>| shape.map(|shape| shape.columns().len());
    assert_eq!(width(live_shape(&mut default_path, "t1")), Some(1));
    let mut app_first = reader(&database, "SET search_path TO app, public");
    assert_eq!(width(live_shape(&mut app_first, "t1")), Some(2));
    let mut no_path = reader(&database, "SET search_path TO ''");
    assert_eq!(live_shape(&mut no_path, "t1"), None);
    assert_eq!(width(live_shape(&mut no_path, "public.t1")), Some(1));
}

#[test]
fn a_table_the_database_lacks_reads_as_missing() {
    assert_docker_available();
    let database = stock_pg_database(18);
    let mut reader = reader(&database, "CREATE TABLE t1 (id INT PRIMARY KEY);");
    assert_eq!(live_shape(&mut reader, "t2"), None);
    assert_eq!(live_shape(&mut reader, "public.t2"), None);
}

/// Whether `error` refuses a PostgreSQL 13 server, which no `postgres-N`
/// feature compiles a catalog for.
const fn refuses_postgres_13(error: &PgShapeError) -> bool {
    matches!(
        error,
        PgShapeError::Load(PgDatabaseBuildError::UnsupportedServerVersion { server: 13, .. })
    )
}

#[test]
fn a_server_no_feature_compiled_is_refused_at_load_and_at_shape() {
    assert_docker_available();
    let old = stock_pg_database(13);
    let current = stock_pg_database(18);
    let ddl = "CREATE TABLE t1 (id INT PRIMARY KEY)";
    let load = block_on(reader(&old, ddl).shape(TargetName::parse("t1").unwrap())).unwrap_err();
    assert!(refuses_postgres_13(&load), "{load:?}");
    // A table loaded from a compiled server, shaped over a connection to
    // PostgreSQL 13.
    run(&current, ddl);
    let database = PgLiveCatalog::new(current.connect(), PUBLICATION)
        .load_tables(&[TargetName::parse("t1").unwrap()])
        .unwrap();
    let table = PgLiveCatalog::find_table(&database, TargetName::parse("t1").unwrap())
        .unwrap()
        .unwrap();
    let shape = PgLiveCatalog::new(old.connect(), PUBLICATION)
        .table_shape(&database, table)
        .unwrap_err();
    assert!(refuses_postgres_13(&shape), "{shape:?}");
}

#[test]
fn a_read_over_an_ended_session_reports_the_database_error() {
    assert_docker_available();
    let database = stock_pg_database(18);
    for name in ["t1", "public.t1"] {
        let mut reader = reader(
            &database,
            "CREATE TABLE IF NOT EXISTS t1 (id INT PRIMARY KEY)",
        );
        terminate_other_sessions(&database);
        let error = block_on(reader.shape(TargetName::parse(name).unwrap())).unwrap_err();
        assert!(
            matches!(error, PgShapeError::Diesel(_)),
            "{name}: {error:?}"
        );
    }
    // The session ends between the load and the read of the table's facts.
    let mut catalog = PgLiveCatalog::new(database.connect(), PUBLICATION);
    let loaded = catalog
        .load_tables(&[TargetName::parse("t1").unwrap()])
        .unwrap();
    let table = PgLiveCatalog::find_table(&loaded, TargetName::parse("t1").unwrap())
        .unwrap()
        .unwrap();
    terminate_other_sessions(&database);
    let error = catalog.table_shape(&loaded, table).unwrap_err();
    assert!(matches!(error, PgShapeError::Diesel(_)), "{error:?}");
}
