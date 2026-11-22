//! Row security an engine reads from the access rules it follows rather
//! than from the catalog it serves.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, CatalogError, DefaultIds, RegisterError, SubscriptionEngine,
    SubscriptionRequest, TableId,
};

const SERVED_DDL: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER);
     CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT);";

const SECURED_DDL: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT);";

const ITEMS_ONLY_DDL: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn parse(ddl: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap()
}

fn served() -> (Engine, TableId, TableId) {
    let db = parse(SERVED_DDL);
    let items = catalog_helpers::table_id::<Postgres, _>(&db, "items").unwrap();
    let notes = catalog_helpers::table_id::<Postgres, _>(&db, "notes").unwrap();
    (
        SubscriptionEngine::new(db, PostgreSqlDialect {}),
        items,
        notes,
    )
}

#[test]
fn followed_row_security_refuses_a_shared_aggregate() {
    let (mut engine, items, _) = served();
    engine
        .register(SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM items"))
        .expect("the served catalog leaves items without row security");

    engine.follow_access_rules(Arc::new(parse(SECURED_DDL)));

    let refused = engine.register(SubscriptionRequest::new(
        2u64,
        "SELECT SUM(amount) FROM items",
    ));
    assert!(
        matches!(
            refused,
            Err(RegisterError::AggregatorOnRlsTable { table_id }) if table_id == items
        ),
        "the followed rules secure items, got {refused:?}"
    );
    assert_eq!(
        engine.subscription_count(),
        1,
        "the aggregate registered before the rules changed stays"
    );
}

#[test]
fn followed_rules_lacking_a_served_table_refuse_it_as_unknown() {
    let (mut engine, _, notes) = served();
    engine.follow_access_rules(Arc::new(parse(ITEMS_ONLY_DDL)));

    let refused = engine.register(SubscriptionRequest::new(1u64, "SELECT * FROM notes"));
    assert!(
        matches!(
            refused,
            Err(RegisterError::Catalog(CatalogError::UnknownTable(table))) if table == notes
        ),
        "no rules name notes, got {refused:?}"
    );
    engine
        .register(SubscriptionRequest::new(1u64, "SELECT * FROM items"))
        .expect("the followed rules name items");
}

#[cfg(all(feature = "diesel-typed-sqlite", feature = "executor-diesel"))]
#[test]
fn a_resolving_engine_follows_rules_read_under_another_dialect() {
    use diesel::{Connection as _, SqliteConnection};
    use sqlparser::dialect::SQLiteDialect;
    use subql::backend::SQLite;
    use subql::reexec::{AutoResolvingEngine, DieselConnector, SyncMode};

    let served = ParserDB::parse::<SQLiteDialect>(SERVED_DDL).unwrap();
    let items = catalog_helpers::table_id::<SQLite, _>(&served, "items").unwrap();
    let mut engine: AutoResolvingEngine<TestEvent<SQLite>, DefaultIds, ParserDB, _> =
        AutoResolvingEngine::new(
            SubscriptionEngine::new(served, SQLiteDialect {}),
            SyncMode(DieselConnector::<_, SQLite>::new(
                SqliteConnection::establish(":memory:").unwrap(),
            )),
        );
    engine.follow_access_rules(Arc::new(parse(SECURED_DDL)));

    let refused = engine.register(
        SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM items"),
        (),
    );
    assert!(
        matches!(
            refused,
            Err(RegisterError::AggregatorOnRlsTable { table_id }) if table_id == items
        ),
        "the PostgreSQL rules secure the SQLite engine's items, got {refused:?}"
    );
}
