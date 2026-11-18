use diesel::connection::SimpleConnection;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, Value};
use subql::reexec::{AutoResolvingEngine, ReExecError, ResolvedReads, ResolverMode};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NoCheckpoint, SubscriptionId, TableId};

const DROPPED_TABLE: &str = "CREATE TABLE dropped (id INT PRIMARY KEY, price DOUBLE);";
const HEALTHY_TABLE: &str = "CREATE TABLE healthy (id INT PRIMARY KEY, price DOUBLE);";

diesel::table! {
    dropped (id) {
        id -> Integer,
        price -> Double,
    }
}

diesel::table! {
    healthy (id) {
        id -> Integer,
        price -> Double,
    }
}

pub fn setup(connection: &mut diesel::MysqlConnection) -> (ParserDB, [TableId; 2]) {
    // Diesel's query DSL does not express table creation.
    connection
        .batch_execute(&format!("{DROPPED_TABLE} {HEALTHY_TABLE}"))
        .expect("create read tables");
    diesel::insert_into(healthy::table)
        .values([
            (healthy::id.eq(1), healthy::price.eq(5.0)),
            (healthy::id.eq(2), healthy::price.eq(9.0)),
        ])
        .execute(&mut *connection)
        .expect("insert healthy minimum");
    diesel::insert_into(dropped::table)
        .values((dropped::id.eq(1), dropped::price.eq(5.0)))
        .execute(connection)
        .expect("insert initial minimum");
    let catalog = ParserDB::parse::<MySqlDialect>(
        "CREATE TABLE dropped (id INT PRIMARY KEY, price FLOAT); \
         CREATE TABLE healthy (id INT PRIMARY KEY, price FLOAT);",
    )
    .expect("parse read catalog");
    let tables = ["dropped", "healthy"].map(|table| {
        catalog_helpers::table_id::<MySql, _>(&catalog, table).expect("resolve read table")
    });
    (catalog, tables)
}

pub fn queue_reads<Mode>(
    engine: &mut AutoResolvingEngine<TestEvent<MySql>, DefaultIds, ParserDB, Mode>,
    tables: [TableId; 2],
) -> [SubscriptionId; 2]
where
    Mode: ResolverMode<MySql, AuthContext = ()>,
{
    let subscriptions = [
        super::reexec::register_captured(engine, 1, "SELECT MIN(price) FROM dropped"),
        super::reexec::register_captured(engine, 2, "SELECT MIN(price) FROM healthy"),
    ];
    for (subscription, table) in subscriptions.into_iter().zip(tables) {
        subql::Install::install(
            &mut *engine,
            subscription,
            subql::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<subql::NoCheckpoint>,
                fence: None,
            },
        )
        .expect("install initial minimum");
        engine
            .apply_leaving_reads_queued(
                &TestEvent::<MySql>::delete(table, vec![Value::Int(1), Value::Float(5.0)])
                    .with_pk_columns([0u16]),
            )
            .expect("queue minimum read");
    }
    assert_eq!(engine.pending_read_count(), 2);
    subscriptions
}

pub fn drop_table(connection: &mut diesel::MysqlConnection) {
    // Diesel's query DSL does not express table removal.
    connection
        .batch_execute("DROP TABLE dropped")
        .expect("drop read table");
    diesel::delete(healthy::table.filter(healthy::id.eq(1)))
        .execute(connection)
        .expect("delete healthy minimum");
}

/// Deletes the minimum both queued reads were displaced from, leaving `9.0`
/// as each table's minimum.
pub fn displace_minimums(connection: &mut diesel::MysqlConnection) {
    diesel::insert_into(dropped::table)
        .values((dropped::id.eq(2), dropped::price.eq(9.0)))
        .execute(&mut *connection)
        .expect("insert next minimum");
    diesel::delete(dropped::table.filter(dropped::id.eq(1)))
        .execute(&mut *connection)
        .expect("delete dropped minimum");
    diesel::delete(healthy::table.filter(healthy::id.eq(1)))
        .execute(connection)
        .expect("delete healthy minimum");
}

/// Makes a read on the session give up on a table lock after one second,
/// written as SQL since diesel's query DSL does not express session
/// variables.
pub const WAIT_ONE_SECOND_FOR_LOCKS: &str = "SET SESSION lock_wait_timeout = 1";

pub fn lock_table(connection: &mut diesel::MysqlConnection) {
    // Diesel's query DSL does not express table locks.
    connection
        .batch_execute("LOCK TABLES dropped WRITE")
        .expect("lock read table");
}

pub fn unlock_tables(connection: &mut diesel::MysqlConnection) {
    // Diesel's query DSL does not express table locks.
    connection
        .batch_execute("UNLOCK TABLES")
        .expect("unlock read table");
}

pub fn assert_error_number(error: &diesel::result::Error, number: i32) {
    assert!(
        matches!(error, diesel::result::Error::DatabaseError(_, information)
        if information.native_code() == Some(number)),
        "{error:?}"
    );
}

/// The read of `subscriptions[0]` stopped with an error `assert_error`
/// accepts, and `subscriptions[1]` read its new minimum.
pub fn assert_stopped<Error: core::fmt::Debug>(
    reads: &ResolvedReads<DefaultIds, MySql, NoCheckpoint, Error>,
    subscriptions: [SubscriptionId; 2],
    assert_error: impl FnOnce(&Error),
) {
    assert_eq!(reads.stopped.len(), 1);
    assert_eq!(reads.stopped[0].subscription_id, subscriptions[0]);
    match &reads.stopped[0].error {
        ReExecError::Connector { error, .. } => assert_error(error),
        other => panic!("unexpected read error {other:?}"),
    }
    assert_eq!(reads.scalar_updates.len(), 1);
    assert_eq!(reads.scalar_updates[0].subscription_id, subscriptions[1]);
    assert_eq!(reads.scalar_updates[0].value, Value::Float(9.0));
}
