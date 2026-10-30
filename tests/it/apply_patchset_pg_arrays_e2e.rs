//! Docker-backed E2E test for the [`PgAdapter`](subql::patchset::PgAdapter)
//! array and pgvector binds.
//!
//! Each column arrives as a pg2sqlite replica stores it, an array as JSON
//! array text and a vector as a float32 BLOB, and must land as the value
//! PostgreSQL itself prints.

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::pg::Pg;
use diesel::result::QueryResult;
use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{Binder, DiffOps, Insert, PatchSet, SimpleTable, Value};
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::{bind_array_as, CustomTypePgAdapter, PgArray, PgCustomBinder};
use subql::{DefaultIds, PgChangeEvent, SubscriptionEngine};

const CATALOG: &str = "CREATE EXTENSION vector; \
    CREATE TYPE mood AS ENUM ('happy', 'sad'); \
    CREATE TABLE arrays (id INT PRIMARY KEY, small INT2[], grid INT4[][], ratios FLOAT4[], \
    labels VARCHAR(5)[], prices NUMERIC[], flags BOOLEAN[], days DATE[], stamps TIMESTAMPTZ[], \
    docs JSONB[], moods mood[], emb vector(3), spans INTERVAL[]);";

const PG_SETUP: [&str; 3] = [
    "CREATE EXTENSION vector",
    "CREATE TYPE mood AS ENUM ('happy', 'sad')",
    "CREATE TABLE arrays (id INT PRIMARY KEY, small INT2[], grid INT4[][], ratios FLOAT4[], \
     labels VARCHAR(5)[], prices NUMERIC[], flags BOOLEAN[], days DATE[], stamps TIMESTAMPTZ[], \
     docs JSONB[], moods mood[], emb vector(3), spans INTERVAL[])",
];

const COLUMNS: [(&str, &str); 13] = [
    ("id", ""),
    ("small", ""),
    ("grid", ""),
    ("ratios", ""),
    ("labels", ""),
    ("prices", ""),
    ("flags", ""),
    ("days", ""),
    ("stamps", ""),
    ("docs", ""),
    ("moods", ""),
    ("emb", ""),
    ("spans", ""),
];

#[derive(diesel::sql_types::SqlType)]
#[diesel(postgres_type(name = "mood"))]
struct MoodType;

/// An enum's binary send format is its label, so an element encodes as its text.
struct MoodArrays;

impl PgCustomBinder<String, Vec<u8>> for MoodArrays {
    fn bind<'a>(
        &self,
        _: &'a Value<String, Vec<u8>>,
    ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
        Err(diesel::result::Error::QueryBuilderError(
            "scalar mood is not under test".into(),
        ))
    }

    fn bind_array(&self, array: &PgArray) -> QueryResult<Box<dyn Binder<Pg> + Send>> {
        bind_array_as::<MoodType>(array, |label| {
            let label = label.as_str()?;
            matches!(label, "happy" | "sad").then(|| label.as_bytes().to_vec())
        })
        .ok_or_else(|| diesel::result::Error::QueryBuilderError("not a mood".into()))
    }
}

#[derive(QueryableByName, Debug, PartialEq)]
struct Printed {
    #[diesel(sql_type = diesel::sql_types::Text)]
    row: String,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn arrays_and_vectors_bind_natively() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut conn = db.connect();
    for statement in PG_SETUP {
        sql_query(statement)
            .execute(&mut conn)
            .expect("set up the schema");
    }

    let catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG).expect("parse the catalog");
    let engine: SubscriptionEngine<PgChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let emb: Vec<u8> = [1.0_f32, -2.5, 3.0]
        .iter()
        .flat_map(|x| x.to_le_bytes())
        .collect();
    let row = Insert::from(SimpleTable::new("arrays", &COLUMNS, &[0]))
        .set(0, 1_i64)
        .unwrap()
        .set(1, "[1,null,-3]")
        .unwrap()
        .set(2, "[[1,2],[3,4]]")
        .unwrap()
        .set(3, "[1.5,0.25]")
        .unwrap()
        .set(4, "[\"ab\",\"c\"]")
        .unwrap()
        .set(5, "[\"12.50\",\"-0.001\"]")
        .unwrap()
        .set(6, "[1,0]")
        .unwrap()
        .set(7, "[\"2024-03-04\"]")
        .unwrap()
        .set(8, "[\"2024-03-04 05:06:07.891000+00:00\"]")
        .unwrap()
        .set(9, "[\"{\\\"b\\\":1,\\\"a\\\":2}\"]")
        .unwrap()
        .set(10, "[[\"happy\"],[\"sad\"]]")
        .unwrap()
        .set(11, emb)
        .unwrap()
        .set(12, "[\"1 day 02:00:00\",null,\"-00:15:00\"]")
        .unwrap();
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(row);

    let adapter = CustomTypePgAdapter::new(engine.database())
        .expect("the catalog indexes")
        .register("mood", MoodArrays);
    sql_query("SET TIME ZONE 'UTC'").execute(&mut conn).unwrap();
    let n = engine
        .apply_patchset(&inserts, &mut conn, &adapter)
        .expect("apply the insert");
    assert_eq!(n, 1, "one row inserted");

    let printed: Printed = sql_query(
        "SELECT concat_ws(' | ', small, grid, ratios, labels, prices, flags, days, stamps, docs, \
         moods, emb, spans) AS row FROM arrays WHERE id = 1",
    )
    .get_result(&mut conn)
    .expect("load");
    assert_eq!(
        printed.row,
        "{1,NULL,-3} | {{1,2},{3,4}} | {1.5,0.25} | {ab,c} | {12.50,-0.001} | {t,f} | \
         {2024-03-04} | {\"2024-03-04 05:06:07.891+00\"} | {\"{\\\"a\\\": 2, \\\"b\\\": 1}\"} | \
         {{happy},{sad}} | [1,-2.5,3] | {\"1 day 02:00:00\",NULL,-00:15:00}"
    );
}

/// A ragged array, a vector of the wrong length and a label outside the enum
/// are each refused before anything reaches the table.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn malformed_arrays_and_vectors_are_refused() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut conn = db.connect();
    for statement in PG_SETUP {
        sql_query(statement)
            .execute(&mut conn)
            .expect("set up the schema");
    }
    let catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG).expect("parse the catalog");
    let engine: SubscriptionEngine<PgChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let adapter = CustomTypePgAdapter::new(engine.database())
        .expect("the catalog indexes")
        .register("mood", MoodArrays);

    let short_vector = 1.0_f32.to_le_bytes().to_vec();
    for (column, value) in [
        (2, Value::Text(String::from("[[1],[2,3]]"))),
        (11, Value::Blob(short_vector)),
        (10, Value::Text(String::from("[\"angry\"]"))),
    ] {
        let row = Insert::from(SimpleTable::new("arrays", &COLUMNS, &[0]))
            .set(0, 1_i64)
            .unwrap()
            .set(column, value)
            .unwrap();
        let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(row);
        assert!(
            engine
                .apply_patchset(&inserts, &mut conn, &adapter)
                .is_err(),
            "column {column} must be refused"
        );
    }
    let count: Printed = sql_query("SELECT count(*)::text AS row FROM arrays")
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(count.row, "0");
}
