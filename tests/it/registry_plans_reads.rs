//! One registration path: a bare registry plans a read for what it cannot
//! serve itself, rather than refusing it.
#![allow(clippy::unwrap_used)]

use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{SubscriptionRequest, Tier};

use crate::common::postgres_engine;

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE managers (id INT PRIMARY KEY);";

/// No wrapper anywhere: the registry itself reports which read serves a query
/// its own evaluator cannot decide.
#[test]
fn a_bare_registry_plans_the_read_it_cannot_serve() {
    let (mut engine, table) = postgres_engine::<TestEvent<Postgres>>(DDL);

    let extreme = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT MIN(price) FROM orders",
        ))
        .expect("needing a read is not grounds to refuse");
    assert!(
        matches!(extreme.tier, Tier::Scalar { .. }),
        "got {:?}",
        extreme.tier
    );

    let keyed = engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE lower(status) = 'paid'",
        ))
        .expect("a filter outside the evaluator is planned, not refused");
    assert_eq!(
        keyed.tier,
        Tier::KeyedRows {
            query: subql::reexec::BoundQuery::new(
                "SELECT \"id\", \"price\", \"status\" FROM orders WHERE lower(status) = 'paid'"
                    .to_string(),
                Vec::new(),
            ),
            table_id: table,
        },
        "one table, so the read asks only about the rows that changed"
    );

    let whole = engine
        .register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM orders WHERE lower(status) = 'paid' \
             AND id IN (SELECT id FROM managers)",
        ))
        .expect("a filter over two tables is planned, not refused");
    match whole.tier {
        Tier::WholeRows { tables, .. } => assert_eq!(tables.len(), 2, "both tables trigger it"),
        other => panic!("expected a whole re-read, got {other:?}"),
    }

    assert_eq!(engine.reread_count(), 3, "the registry holds all three");
}

/// A query nothing can plan is still refused, and says why.
#[test]
fn a_statement_no_tier_can_serve_is_still_refused() {
    let (mut engine, _) = postgres_engine::<TestEvent<Postgres>>(DDL);
    let refusal = engine
        .register(SubscriptionRequest::new(1u64, "DELETE FROM orders"))
        .expect_err("only a SELECT can be maintained or re-read");
    assert!(
        matches!(refusal, subql::RegisterError::UnsupportedSql(_)),
        "got {refusal:?}"
    );
}
