//! Integration test for [`DieselConnector`]: drives a full `MIN(price)`
//! subscription through the [`AutoResolvingEngine`] against an in-memory
//! SQLite database. Verifies that:
//!
//! 1. Deleting the current extreme triggers a re-execution that goes
//!    through the Diesel connector, decodes the new MIN, installs it, and
//!    surfaces a [`ScalarUpdate`] (no triggers under the auto-resolving
//!    engine).
//! 2. A `MIN` over an empty result set decodes as [`Value::Null`].
//! 3. A failed read is classified by SQLite's result code, so a missing
//!    table ends its subscription and a busy database keeps the read queued.
//!
//! The lib's unit tests cover the "no DB call on insert above the
//! extreme" optimization with a `MockConnector`. Here we only assert the
//! Diesel side of the round-trip. Each test seeds SQLite with whatever
//! state the connector is expected to read at re-execution time, then
//! dispatches a CDC event that forces the engine to call the connector.
//!
//! Gated behind the `executor-diesel` feature so default `cargo test`
//! does not try to compile it.
#![allow(clippy::unwrap_used)]

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{AutoResolvingEngine, DieselConnector, ReExecError, SyncMode};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

type Engine = AutoResolvingEngine<
    TestEvent<Postgres>,
    DefaultIds,
    ParserDB,
    SyncMode<DieselConnector<SqliteConnection, Postgres>>,
>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn orders_id(database: &ParserDB) -> TableId {
    catalog_helpers::table_id::<subql::backend::Postgres, _>(database, "orders")
        .expect("orders table")
}

fn sqlite_with(rows: &[(i64, f64)]) -> SqliteConnection {
    sqlite_at(":memory:", rows)
}

fn sqlite_at(url: &str, rows: &[(i64, f64)]) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(url).unwrap();
    sql_query(
        "CREATE TABLE orders (\
            id INTEGER PRIMARY KEY, \
            price REAL, \
            quantity INTEGER, \
            status TEXT)",
    )
    .execute(&mut conn)
    .unwrap();
    for (id, price) in rows {
        sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(&mut conn)
        .unwrap();
    }
    conn
}

fn delete_event(tid: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::delete(
        tid,
        vec![
            Value::Int(id),
            Value::Float(price),
            Value::Int(1),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
}

fn build_engine(conn: SqliteConnection) -> (Engine, TableId) {
    let database = catalog();
    let tid = orders_id(&database);
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        database,
        PostgreSqlDialect {},
    );
    (
        AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(conn))),
        tid,
    )
}

fn register_min(e: &mut Engine, bootstrap: Value<Postgres>) -> u64 {
    let qid = match e
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got Engine, got {other:?}"),
    };
    assert!(subql::Install::install(
        e,
        qid,
        subql::ScalarInstall {
            value: bootstrap,
            checkpoint: None::<subql::NoCheckpoint>,
            fence: None,
        }
    )
    .is_ok());
    qid
}

#[test]
fn delete_of_extreme_resolves_via_diesel_connector() {
    // SQLite reflects the state AFTER id=1 has been deleted: just id=2 with
    // price=9.0. The model previously had {1: 5.0, 2: 9.0}. We bootstrap
    // with 5.0 to mirror that. The delete event then forces the connector to
    // re-query SQLite, which now contains only id=2 with price=9.0.
    let conn = sqlite_with(&[(2, 9.0)]);
    let (mut e, tid) = build_engine(conn);
    let qid = register_min(&mut e, Value::Float(5.0));

    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(e.pending_read_count(), 0, "no pending reads");
    assert_eq!(n.scalar_updates.len(), 1, "expected MIN to be re-executed");
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(
        n.scalar_updates[0].value,
        Value::Float(9.0),
        "Diesel returned the new MIN"
    );
}

#[test]
fn empty_set_min_decodes_as_null() {
    // SQLite has no rows. The connector re-executes MIN(price) and gets a
    // single SQL-NULL row, which decodes as Value::Null.
    let conn = sqlite_with(&[]);
    let (mut e, tid) = build_engine(conn);
    let qid = register_min(&mut e, Value::Float(5.0));

    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(
        n.scalar_updates[0].value,
        Value::Null,
        "MIN over empty set -> SQL NULL -> Value::Null"
    );
}

/// The SQLite result code of a connector error, or `None` for any other
/// error.
fn result_code(error: &ReExecError<diesel::result::Error>) -> Option<i32> {
    match error {
        ReExecError::Connector {
            error: diesel::result::Error::DatabaseError(_, information),
            ..
        } => information.native_code(),
        _ => None,
    }
}

/// The connector reads SQLite's result codes although the engine speaks
/// PostgreSQL, so a missing table ends the subscription.
#[test]
fn a_dropped_table_ends_its_subscription_by_the_sqlite_result_code() {
    let mut conn = sqlite_with(&[(2, 9.0)]);
    sql_query("DROP TABLE orders").execute(&mut conn).unwrap();
    let (mut e, tid) = build_engine(conn);
    let qid = register_min(&mut e, Value::Float(5.0));

    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    let reads = e.resolve_collect().unwrap();
    assert_eq!(reads.stopped.len(), 1);
    assert_eq!(reads.stopped[0].subscription_id, qid);
    assert_eq!(result_code(&reads.stopped[0].error), Some(1));
    assert!(reads.scalar_updates.is_empty());
    assert_eq!(e.pending_read_count(), 0);
}

#[test]
fn a_busy_database_keeps_the_read_queued_until_the_lock_is_released() {
    let directory = tempfile::tempdir().unwrap();
    let url = directory.path().join("orders.sqlite");
    let url = url.to_str().unwrap();
    let mut holder = sqlite_at(url, &[(2, 9.0)]);
    let mut reader = SqliteConnection::establish(url).unwrap();
    sql_query("PRAGMA busy_timeout = 0")
        .execute(&mut reader)
        .unwrap();
    let (mut e, tid) = build_engine(reader);
    let qid = register_min(&mut e, Value::Float(5.0));

    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    sql_query("BEGIN EXCLUSIVE").execute(&mut holder).unwrap();
    let error = e.resolve_collect().unwrap_err();
    assert_eq!(result_code(&error).map(|code| code & 0xff), Some(5));
    assert_eq!(e.pending_read_count(), 1);

    sql_query("COMMIT").execute(&mut holder).unwrap();
    let reads = e.resolve_collect().unwrap();
    assert!(reads.stopped.is_empty());
    assert_eq!(reads.scalar_updates.len(), 1);
    assert_eq!(reads.scalar_updates[0].subscription_id, qid);
    assert_eq!(reads.scalar_updates[0].value, Value::Float(9.0));
}
