//! The PostgreSQL live catalog against a PostgreSQL 18 server.
//!
//! Every test creates tables on a fresh database, reads them through an
//! `InlineLiveReader` over a `PgLiveCatalog`, and judges each live shape
//! against the shape of the same DDL parsed as the consumer's catalog.
#![allow(clippy::unwrap_used)]

use std::fmt::Write as _;

use diesel::connection::SimpleConnection;
use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::{ParserDB, TargetName};
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::drift::{
    judge, Incompatibility, InlineLiveReader, LiveShapeReader, PgLiveCatalog, PublishedChange,
    ReplicaIdentity, TableShape, Verdict,
};
use subql::testing::block_on;

use crate::common::{assert_docker_available, pg18_database, PgDatabase};

/// The publication every reader is built with.
const PUBLICATION: &str = "subql";

/// A reader over a fresh connection to `database` that has run `setup`.
fn reader(database: &PgDatabase, setup: &str) -> InlineLiveReader<PgLiveCatalog> {
    let mut connection = database.connect();
    // Fixture DDL, which the query DSL does not express.
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

/// Run `statement` on the reader's own connection.
fn execute(reader: &mut InlineLiveReader<PgLiveCatalog>, statement: &str) {
    reader
        .connection()
        .connection()
        .batch_execute(statement)
        .unwrap();
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
    let database = pg18_database();
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
    let database = pg18_database();
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
        execute(
            &mut reader,
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
    let database = pg18_database();
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
    execute(
        &mut reader,
        &format!("CREATE PUBLICATION {PUBLICATION} FOR TABLE t1"),
    );
    assert_eq!(
        judge(&catalog_shape(ddl), live_shape(&mut reader, "t2").as_ref()),
        Verdict::Unpublished,
        "the publication carries another table"
    );
}

#[test]
fn a_publication_reports_its_skipped_changes_column_list_and_row_filter() {
    assert_docker_available();
    let database = pg18_database();
    let ddl = "CREATE TABLE t1 (id INT PRIMARY KEY, v INT, note TEXT)";
    let mut reader = reader(
        &database,
        &format!(
            "{ddl}; ALTER TABLE t1 REPLICA IDENTITY FULL;
             CREATE PUBLICATION {PUBLICATION} FOR TABLE t1 (id, v) WHERE (v > 0)
                 WITH (publish = 'insert');"
        ),
    );
    assert_eq!(
        judge(&catalog_shape(ddl), live_shape(&mut reader, "t1").as_ref()),
        Verdict::Incompatible {
            reasons: vec![
                Incompatibility::ColumnUnpublished {
                    column: "note".into()
                },
                Incompatibility::OperationsUnpublished {
                    operations: vec![
                        PublishedChange::Update,
                        PublishedChange::Delete,
                        PublishedChange::Truncate
                    ]
                },
                Incompatibility::RowFiltered {
                    filter: "(v > 0)".into()
                },
            ]
        }
    );
}

#[test]
fn quoted_mixed_case_names_read_as_stored() {
    assert_docker_available();
    let database = pg18_database();
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
    let database = pg18_database();
    let mut reader = reader(
        &database,
        "CREATE SCHEMA app;
         CREATE TABLE app.t1 (id INT PRIMARY KEY, v INT);
         CREATE TABLE public.t1 (id INT PRIMARY KEY);",
    );
    let width = |shape: Option<TableShape>| shape.map(|shape| shape.columns().len());
    assert_eq!(width(live_shape(&mut reader, "t1")), Some(1));
    execute(&mut reader, "SET search_path TO app, public");
    assert_eq!(width(live_shape(&mut reader, "t1")), Some(2));
    execute(&mut reader, "SET search_path TO ''");
    assert_eq!(live_shape(&mut reader, "t1"), None);
    assert_eq!(width(live_shape(&mut reader, "public.t1")), Some(1));
}

#[test]
fn a_table_the_database_lacks_reads_as_missing() {
    assert_docker_available();
    let database = pg18_database();
    let mut reader = reader(&database, "CREATE TABLE t1 (id INT PRIMARY KEY);");
    assert_eq!(live_shape(&mut reader, "t2"), None);
    assert_eq!(live_shape(&mut reader, "public.t2"), None);
}
