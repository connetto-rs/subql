use diesel::connection::SimpleConnection;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, Value};
use subql::reexec::{AutoResolvingEngine, ResolverMode};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionId, TableId};

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

pub fn restore_table(connection: &mut diesel::MysqlConnection) {
    // Diesel's query DSL does not express table creation.
    connection
        .batch_execute(DROPPED_TABLE)
        .expect("restore read table");
    diesel::insert_into(dropped::table)
        .values((dropped::id.eq(2), dropped::price.eq(9.0)))
        .execute(connection)
        .expect("insert restored minimum");
}
