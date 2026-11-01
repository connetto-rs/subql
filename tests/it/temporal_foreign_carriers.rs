//! Temporal arithmetic over a backend whose temporal and decimal carriers are
//! the `i64`.
//!
//! Such a backend plans the arithmetic in process, since the kinds are
//! PostgreSQL's, but reading a carrier as the `chrono` type or `BigDecimal`
//! the operator needs is refused. So a dispatched row is refused with
//! `OperandKinds`, never answered, and a `/` whose quotient needs the
//! declared `div_precision_increment` is routed to a read at registration.
#![allow(clippy::unwrap_used)]

use crate::common::foreign_carriers::ForeignCarriers;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Interval, Postgres, Value};
use subql::compiler::vm::refusal::{ArithmeticOp, EvaluationRefusal};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ConsumerNotifications, DefaultIds, NotServed, SubscriptionEngine,
    SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, start TIMESTAMP, start2 TIMESTAMP, \
                   tza TIMESTAMPTZ, tzb TIMESTAMPTZ, d DATE, tm TIME, tm2 TIME, \
                   span INTERVAL, amount NUMERIC)";

/// A row of `t` whose temporal and decimal cells are the `i64` carriers.
fn row() -> Vec<Value<ForeignCarriers>> {
    vec![
        Value::Int(1),
        Value::Timestamp(1i64),
        Value::Timestamp(2i64),
        Value::TimestampTz(1i64),
        Value::TimestampTz(2i64),
        Value::Date(1i64),
        Value::Time(1i64),
        Value::Time(2i64),
        Value::Interval(Interval::parse("2 day").expect("the span parses")),
        Value::Decimal(1i64),
    ]
}

/// Register `predicate` over `t` and insert the row, asserting it is served in
/// process. The temporal op then runs on the `i64` cells and refuses.
fn dispatch(
    predicate: &str,
) -> ConsumerNotifications<DefaultIds, subql::NoCheckpoint, ForeignCarriers> {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id::<Postgres, _>(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<ForeignCarriers>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        ))
        .expect("the predicate registers");
    assert!(
        registered.not_served_because.is_none(),
        "{predicate} is served in process"
    );
    engine
        .consumers(&TestEvent::insert(table, row()))
        .expect("dispatch succeeds")
}

fn refused(
    notifications: &ConsumerNotifications<DefaultIds, subql::NoCheckpoint, ForeignCarriers>,
) -> Vec<EvaluationRefusal> {
    notifications
        .evaluation_failures()
        .iter()
        .map(|failure| failure.refusal)
        .collect()
}

/// `timestamp - timestamp` reads the two carriers as instants to take apart,
/// which the `i64` is not, so the span is refused.
#[test]
fn a_temporal_difference_over_foreign_carriers_is_refused() {
    let notifications = dispatch("start - start2 > '1 day'");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Subtract
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}

/// `timestamp + interval` moves the timestamp by the span, which needs its
/// carrier as an instant to shift, so the moved value is refused.
#[test]
fn a_timestamp_moved_by_a_span_over_foreign_carriers_is_refused() {
    let notifications = dispatch("start + span > start2");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Add
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}

/// `interval * number` scales the span by a factor read as a `BigDecimal`,
/// which the `i64` decimal is not, so the product is refused.
#[test]
fn a_span_scaled_by_a_foreign_decimal_is_refused() {
    let notifications = dispatch("span * amount > '1 day'");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Multiply
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}

/// `interval / number` answers a decimal whose scale needs the declared
/// `div_precision_increment`, which a hand-built catalog never declares, so
/// registration routes it to a read rather than answering an unknown scale.
#[test]
fn a_span_divided_over_foreign_carriers_is_routed_to_a_read() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<ForeignCarriers>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE span / 2 > '1 day'",
        ))
        .expect("the predicate registers");
    assert!(
        matches!(
            registered.not_served_because,
            Some(NotServed::DivisionPrecisionNotDeclared)
        ),
        "span / 2 is routed to a read, not answered"
    );
}

/// `date + interval` moves the date, which needs its carrier as a `NaiveDate`
/// to shift, so the moved value is refused.
#[test]
fn a_date_moved_by_a_span_over_foreign_carriers_is_refused() {
    let notifications = dispatch("d + span > start");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Add
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}

/// `time + interval` moves the time, which needs its carrier as a `NaiveTime`
/// to shift, so the moved value is refused.
#[test]
fn a_time_moved_by_a_span_over_foreign_carriers_is_refused() {
    let notifications = dispatch("tm + span > tm2");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Add
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}

/// `timestamptz - timestamptz` reads the two carriers as instants to take
/// apart, which the `i64` is not, so the span is refused.
#[test]
fn a_timestamptz_difference_over_foreign_carriers_is_refused() {
    let notifications = dispatch("tza - tzb > '1 day'");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Subtract
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}

/// `timestamptz + interval` moves the instant, which needs its carrier as a
/// `DateTime<Utc>` to shift, so the moved value is refused. The span is a
/// literal so registration sees no month or day and serves it in process.
#[test]
fn a_timestamptz_moved_by_an_hourly_span_over_foreign_carriers_is_refused() {
    let notifications = dispatch("tza + '1 hour' > tzb");
    assert_eq!(
        refused(&notifications),
        vec![EvaluationRefusal::OperandKinds {
            operation: ArithmeticOp::Add
        }]
    );
    assert!(
        notifications.inserted().is_empty(),
        "a refused row is never inserted"
    );
}
