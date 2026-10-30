//! PostgreSQL `interval` predicates answered in process.
//!
//! PostgreSQL compares intervals by span, a month as 30 days and a day as 24
//! hours, so `1 mon` equals `30 days` though the two print differently.
//! Engines without the type re-read every interval predicate, as do the
//! aggregates and the arithmetic over one, which the engine does not model.

#![allow(clippy::unwrap_used)]

use crate::common::semantics::{notifies, register, served};
use subql::backend::{Interval, MySql, Postgres, SQLite, Value};

const DDL: &str = "CREATE TABLE shifts (id INT PRIMARY KEY, span INTERVAL);";

fn shift(span: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::Interval(Interval::parse(span).unwrap()),
    ]
}

fn reaches(predicate: &str, span: &str) -> bool {
    notifies::<Postgres>(
        DDL,
        "shifts",
        &format!("SELECT * FROM shifts WHERE {predicate}"),
        shift(span),
    )
}

/// A quoted literal and a typed `interval` literal both read as PostgreSQL's
/// `interval_in` reads them, and equality is by span.
#[test]
fn equality_is_by_span_for_every_literal_spelling() {
    for predicate in [
        "span = '01:00:00'",
        "span = '1 hour'",
        "span = interval '60 minutes'",
        "span = interval '1' hour",
        "'PT1H' = span",
    ] {
        assert!(reaches(predicate, "01:00:00"), "{predicate}");
        assert!(!reaches(predicate, "00:02:00"), "{predicate}");
    }
    assert!(reaches("span = '1 mon'", "30 days"));
    assert!(reaches("span <> '1 mon'", "31 days"));
    assert!(!reaches("span <> '1 mon'", "720:00:00"));
}

/// Ordering follows the span, so a month sits above 29 days and below 31,
/// and a negative day with positive hours sits below zero.
#[test]
fn ordering_is_by_span() {
    assert!(reaches("span > interval '29 days'", "1 mon"));
    assert!(!reaches("span > interval '31 days'", "1 mon"));
    assert!(reaches("span < '00:00:00'", "-1 days +02:00:00"));
    assert!(reaches("span BETWEEN '1 day' AND '2 days'", "36:00:00"));
    assert!(reaches("span >= 'infinity'", "infinity"));
    assert!(!reaches("span < '-infinity'", "-178000000 years"));
}

/// An `IN` list is a membership test under the same span equality.
#[test]
fn an_in_list_matches_by_span() {
    let predicate = "span IN ('1 mon', interval '15 minutes')";
    assert!(reaches(predicate, "30 days"));
    assert!(reaches(predicate, "00:15:00"));
    assert!(!reaches(predicate, "00:16:00"));
    assert!(reaches("span NOT IN ('1 mon')", "29 days"));
}

type PgEngine = subql::SubscriptionEngine<
    subql::testing::TestEvent<Postgres>,
    subql::DefaultIds,
    sql_traits::structs::ParserDB,
>;

fn engine() -> (PgEngine, subql::TableId) {
    let db =
        sql_traits::structs::ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(DDL).unwrap();
    let table = subql::catalog_helpers::table_id::<Postgres, _>(&db, "shifts").unwrap();
    (
        subql::SubscriptionEngine::new(db, sqlparser::dialect::PostgreSqlDialect {}),
        table,
    )
}

/// Text that PostgreSQL would refuse as an interval, or a number where an
/// interval is compared, refuses the predicate rather than registering one
/// that could never match.
#[test]
fn an_unreadable_interval_literal_is_refused() {
    let (mut engine, _) = engine();
    for predicate in ["span = '1 fortnight'", "span = 3600"] {
        assert!(
            engine
                .register(subql::SubscriptionRequest::new(
                    1u64,
                    format!("SELECT * FROM shifts WHERE {predicate}")
                ))
                .is_err(),
            "{predicate}"
        );
    }
}

/// An interval bound to a placeholder reads as the literal it prints as.
#[test]
fn a_bound_interval_matches_by_span() {
    let (mut engine, table) = engine();
    let bound = Value::Interval(Interval::parse("30 days").unwrap());
    let registered = engine
        .register(
            subql::SubscriptionRequest::new(1u64, "SELECT * FROM shifts WHERE span = $1")
                .binds(vec![bound]),
        )
        .unwrap();
    assert!(registered.not_served_because.is_none());
    let mut reached = |span: &str| {
        !engine
            .consumers(&subql::testing::TestEvent::insert(table, shift(span)))
            .unwrap()
            .inserted()
            .is_empty()
    };
    assert!(reached("1 mon"));
    assert!(!reached("1 day"));
}

/// What the engine does not model over an interval is read from the database.
#[test]
fn interval_aggregates_arithmetic_and_grouping_are_reread() {
    for statement in [
        "SELECT SUM(span) FROM shifts",
        "SELECT AVG(span) FROM shifts",
        "SELECT * FROM shifts WHERE span + interval '1 hour' > '2 hours'",
        "SELECT * FROM shifts WHERE span * 2 > '2 hours'",
        "SELECT span, COUNT(*) FROM shifts GROUP BY span",
        "SELECT * FROM shifts WHERE -span > '1 hour'",
        "SELECT * FROM shifts WHERE span > interval '1 hour' * 2",
        "SELECT * FROM shifts WHERE id = interval '1 hour'",
    ] {
        let registered = register::<Postgres>(DDL, statement);
        assert!(
            registered.not_served_because.is_some(),
            "{statement} is served in process"
        );
    }
}

/// An engine without PostgreSQL's interval compares the stored text, so its
/// interval predicates are read from the database.
#[test]
fn engines_without_the_type_reread_interval_predicates() {
    let predicate = "SELECT * FROM shifts WHERE span = '01:00:00'";
    assert!(served::<Postgres>(DDL, predicate));
    assert!(!served::<SQLite>(DDL, predicate));
    assert!(!served::<MySql>(DDL, predicate));
}
