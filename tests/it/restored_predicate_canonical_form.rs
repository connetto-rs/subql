//! A predicate saved under an older canonical spelling, restored under the current one.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::compiler::canonicalize::{hash_sql, normalize_sql};
use subql::persistence::shard::{deserialize_shard, serialize_shard};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionRequest, TableId, Tier};

use crate::common::store::{StoredEngine, TempStore};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price INT, status TEXT);";
const FILTER: &str =
    "SELECT * FROM orders WHERE status = 'paid' AND price > 10 AND id < 5 OR price = 0";
/// How `sqlparser-canonicalize` before connetto-rs/sqlparser-canonicalize#41 spelled `FILTER`.
const NESTED: &str = "(((('paid' = status) AND (10 < price)) AND (5 > id)) OR (0 = price))";

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap()
}

fn orders() -> TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").unwrap()
}

/// Register `sql` for consumer 1, returning the subscription and whether it made a new predicate.
fn register(engine: &mut StoredEngine, sql: &str) -> (u64, bool) {
    let registered = engine
        .register(SubscriptionRequest::new(1u64, sql))
        .unwrap();
    let Tier::InProcess(served) = registered.tier else {
        panic!("the filter is served in process");
    };
    (registered.subscription_id, served.created_new_predicate)
}

/// Rewrite the saved shard so each consumer's subscription sits on its own copy of the one saved predicate, spelled as given.
fn respell_saved_predicate(store: &TempStore, spellings: &[(u64, &str)]) {
    let path = store.path().join(format!("table_{}.shard", orders()));
    let (_, mut payload) =
        deserialize_shard::<DefaultIds, _>(&std::fs::read(&path).unwrap(), &catalog()).unwrap();
    let [saved] = payload.predicates.as_slice() else {
        panic!("one saved predicate");
    };
    let mut predicates = Vec::new();
    for (consumer, spelling) in spellings {
        let mut predicate = saved.clone();
        predicate.hash = hash_sql(spelling);
        predicate.normalized_sql = (*spelling).to_string();
        predicate.refcount = 1;
        for binding in &mut payload.bindings {
            if binding.consumer_id == *consumer {
                binding.predicate_hash = predicate.hash;
            }
        }
        predicates.push(predicate);
    }
    payload.predicates = predicates;
    let bytes = serialize_shard(orders(), &payload, &catalog()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

fn paid_order() -> TestEvent<Postgres> {
    TestEvent::insert(
        orders(),
        vec![Value::Int(1), Value::Int(20), Value::String("paid".into())],
    )
}

/// Registering again after a restore returns the restored subscription, whatever spelling it
/// was saved under, and the next save files it under the current spelling.
#[test]
fn a_predicate_saved_under_an_older_spelling_still_deduplicates() {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    let (saved, _) = register(&mut engine, FILTER);
    engine.snapshot_table(orders()).unwrap();
    drop(engine);
    respell_saved_predicate(&store, &[(1, NESTED)]);

    let mut engine = store.open(catalog());
    assert_eq!(
        register(&mut engine, FILTER),
        (saved, false),
        "the restored subscription answers the second registration"
    );
    assert_eq!(engine.consumers(&paid_order()).unwrap().inserted(), &[1]);

    engine.snapshot_table(orders()).unwrap();
    let path = store.path().join(format!("table_{}.shard", orders()));
    let (_, payload) =
        deserialize_shard::<DefaultIds, _>(&std::fs::read(path).unwrap(), &catalog()).unwrap();
    let current = normalize_sql(FILTER, &PostgreSqlDialect {}).unwrap();
    let [predicate] = payload.predicates.as_slice() else {
        panic!("one saved predicate");
    };
    assert_ne!(current, NESTED);
    assert_eq!(
        (predicate.normalized_sql.as_str(), predicate.hash),
        (current.as_str(), hash_sql(&current))
    );
}

/// Two saved spellings that now read alike both come back, and a new registration joins one of them.
#[test]
fn saved_spellings_that_now_read_alike_both_come_back() {
    const CONJUNCTION: &str =
        "SELECT * FROM orders WHERE status = 'paid' AND price > 10 AND id < 5";
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    let (first, _) = register(&mut engine, CONJUNCTION);
    let second = engine
        .register(SubscriptionRequest::new(2u64, CONJUNCTION))
        .unwrap()
        .subscription_id;
    engine.snapshot_table(orders()).unwrap();
    drop(engine);
    respell_saved_predicate(
        &store,
        &[
            (1, "((('paid' = status) AND (10 < price)) AND (5 > id))"),
            (2, "(('paid' = status) AND ((10 < price) AND (5 > id)))"),
        ],
    );

    let mut engine = store.open(catalog());
    let (again, created) = register(&mut engine, CONJUNCTION);
    assert!(!created, "the registration joins a restored predicate");
    assert_eq!(again, first, "consumer 1 gets back its own subscription");
    let mut told = engine.consumers(&paid_order()).unwrap().inserted().to_vec();
    told.sort_unstable();
    assert_eq!(told, vec![1, 2], "both restored subscriptions still match");
    assert_ne!(first, second);
}
