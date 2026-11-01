//! PostgreSQL interval arithmetic answered in process.
//!
//! Every expected answer is PostgreSQL's, measured on 16 and 18: months
//! added to the calendar month with the day clamped, `interval * number`
//! cascading what a month and a day leave over, `timestamp - timestamp`
//! carrying whole days into the days field, and a time wrapping in its day.

#![allow(clippy::unwrap_used)]

use crate::common::semantics::{notifies, register};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use subql::backend::{Interval, Postgres, Value};
use subql::compiler::vm::refusal::{ArithmeticOp, EvaluationRefusal};

const DDL: &str = "CREATE TABLE shifts (id INT PRIMARY KEY, start TIMESTAMP, zoned TIMESTAMPTZ, \
                   day DATE, clock TIME, span INTERVAL, factor DOUBLE PRECISION, units INT, \
                   amount NUMERIC);";

/// One row's cells, in column order.
struct Shift {
    start: &'static str,
    zoned: &'static str,
    day: &'static str,
    clock: &'static str,
    span: &'static str,
    factor: f64,
    units: i64,
    amount: &'static str,
}

const SHIFT: Shift = Shift {
    start: "2024-01-31 10:00:00",
    zoned: "2024-03-01 00:00:00",
    day: "2024-01-31",
    clock: "23:30:00",
    span: "1 mon 1 day 00:00:01",
    factor: 1.5,
    units: 3,
    amount: "0.3333",
};

impl Shift {
    fn cells(&self) -> Vec<Value<Postgres>> {
        // `chrono` reads a year past 9999 only after a sign.
        let at = |text: &str| {
            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(&format!("+{text}"), "%Y-%m-%d %H:%M:%S")
                })
                .unwrap()
        };
        vec![
            Value::Int(1),
            Value::Timestamp(at(self.start)),
            Value::TimestampTz(DateTime::<Utc>::from_naive_utc_and_offset(
                at(self.zoned),
                Utc,
            )),
            Value::Date(NaiveDate::parse_from_str(self.day, "%Y-%m-%d").unwrap()),
            Value::Time(NaiveTime::parse_from_str(self.clock, "%H:%M:%S").unwrap()),
            Value::Interval(Interval::parse(self.span).unwrap()),
            Value::Float(self.factor),
            Value::Int(self.units),
            Value::Decimal(self.amount.parse().unwrap()),
        ]
    }
}

fn holds(shift: &Shift, predicate: &str) -> bool {
    notifies::<Postgres>(
        DDL,
        "shifts",
        &format!("SELECT * FROM shifts WHERE {predicate}"),
        shift.cells(),
    )
}

/// Each predicate is true of the row and its neighbour by a microsecond is
/// not, so an answer off by any amount fails one of the two.
#[test]
fn arithmetic_answers_as_postgres_answers() {
    for (predicate, wrong) in [
        (
            "start + span = '2024-03-01 10:00:01'",
            "start + span = '2024-03-01 10:00:01.000001'",
        ),
        (
            "span + start = '2024-03-01 10:00:01'",
            "span + start = '2024-03-01 10:00:00'",
        ),
        (
            "start + interval '1 mon' = '2024-02-29 10:00:00'",
            "start + interval '1 mon' = '2024-03-02 10:00:00'",
        ),
        (
            "start - interval '1 mon' = '2023-12-31 10:00:00'",
            "start - interval '1 mon' = '2024-01-01 10:00:00'",
        ),
        (
            "start + '1 day' = '2024-02-01 10:00:00'",
            "start + '1 day' = '2024-02-01 10:00:01'",
        ),
        (
            "start - '2024-01-01 00:00:00' = '30 days 10:00:00'",
            "start - '2024-01-01 00:00:00' = '30 days 10:00:01'",
        ),
        (
            "day + interval '1 mon 12:00' = '2024-02-29 12:00:00'",
            "day + interval '1 mon 12:00' = '2024-02-29 12:00:01'",
        ),
        (
            "day - interval '1 mon' = '2023-12-31 00:00:00'",
            "day - interval '1 mon' = '2023-12-31 00:00:01'",
        ),
        (
            "clock + interval '1 day 01:00' = '00:30:00'",
            "clock + interval '1 day 01:00' = '00:31:00'",
        ),
        (
            "clock - interval '50:00' = '21:30:00'",
            "clock - interval '50:00' = '21:31:00'",
        ),
        (
            "span * 1.5 = '1 mon 16 days 12:00:01.5'",
            "span * 1.5 = '1 mon 16 days 12:00:01.4'",
        ),
        (
            "span * factor = '1 mon 16 days 12:00:01.5'",
            "factor * span = '1 mon 16 days 12:00:01.6'",
        ),
        (
            "units * span = '3 mons 3 days 00:00:03'",
            "span * units = '3 mons 3 days 00:00:04'",
        ),
        (
            "span * amount = '10 days 07:58:31.0533'",
            "span * amount = '10 days 07:58:31.0534'",
        ),
        (
            "span / units = '10 days 08:00:00.333333'",
            "span / units = '10 days 08:00:00.333334'",
        ),
        (
            "-span = '-1 mons -1 days -00:00:01'",
            "-span = '-1 mons -1 days'",
        ),
        (
            "span + '1 hour' = '1 mon 1 day 01:00:01'",
            "'1 hour' + span = '1 mon 1 day 01:00:02'",
        ),
        (
            "span - interval '1 day' = '1 mon 00:00:01'",
            "span - interval '1 day' = '1 mon 00:00:02'",
        ),
        (
            "zoned + interval '90 minutes' = '2024-03-01 01:30:00+00'",
            "zoned + interval '90 minutes' = '2024-03-01 01:30:01+00'",
        ),
        (
            "zoned - interval '90 minutes' = '2024-02-29 22:30:00+00'",
            "zoned - interval '90 minutes' = '2024-02-29 22:30:01+00'",
        ),
        (
            "zoned + (- interval '90 minutes') = '2024-02-29 22:30:00+00'",
            "zoned + (- interval '90 minutes') = '2024-02-29 22:30:01+00'",
        ),
        (
            "zoned - '2024-01-31 12:00:00+05' = '29 days 17:00:00'",
            "zoned - '2024-01-31 12:00:00+05' = '29 days 17:00:01'",
        ),
        (
            "start + span * 2 > '2024-04-01 00:00:00'",
            "start + span * 2 > '2024-04-02 10:00:02'",
        ),
    ] {
        assert!(holds(&SHIFT, predicate), "{predicate}");
        assert!(!holds(&SHIFT, wrong), "{wrong}");
    }
}

/// A month after the 31st clamps to the month's last day, in a leap year and
/// out of one.
#[test]
fn a_month_clamps_to_the_last_day() {
    let leap = Shift {
        start: "2023-01-31 10:00:00",
        ..SHIFT
    };
    assert!(holds(
        &leap,
        "start + interval '1 mon' = '2023-02-28 10:00:00'"
    ));
    assert!(holds(
        &SHIFT,
        "start + interval '1 mon' = '2024-02-29 10:00:00'"
    ));
}

/// The refusal each predicate meets on `shift`, sorted.
fn refusals(shift: &Shift, predicates: &[&str]) -> Vec<EvaluationRefusal> {
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::testing::TestEvent;

    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let table = subql::catalog_helpers::table_id::<Postgres, _>(&database, "shifts").unwrap();
    let mut engine: subql::SubscriptionEngine<TestEvent<Postgres>, subql::DefaultIds, ParserDB> =
        subql::SubscriptionEngine::new(database, PostgreSqlDialect {});
    for (id, predicate) in (1u64..).zip(predicates) {
        let registered = engine
            .register(subql::SubscriptionRequest::new(
                id,
                format!("SELECT * FROM shifts WHERE {predicate}"),
            ))
            .unwrap();
        assert!(registered.not_served_because.is_none(), "{predicate}");
    }
    let notifications = engine
        .consumers(&TestEvent::insert(table, shift.cells()))
        .unwrap();
    assert!(
        notifications.inserted().is_empty(),
        "a refused row reaches no consumer"
    );
    let mut refused: Vec<_> = notifications
        .evaluation_failures()
        .iter()
        .map(|failure| failure.refusal)
        .collect();
    refused.sort_by_key(|refusal| format!("{refusal:?}"));
    refused
}

/// What PostgreSQL raises on is refused for the row, and the subscription
/// is told why.
#[test]
fn a_result_postgres_raises_on_is_refused() {
    assert_eq!(
        refusals(
            &SHIFT,
            &[
                "start + interval '2147483647 mons' > '2024-01-01 00:00:00'",
                "span / (units - 3) > '1 day'",
                "span * 1e10 > '1 day'",
                "clock + interval 'infinity' > '00:00:00'",
                "clock - interval 'infinity' > '00:00:00'",
            ]
        ),
        [
            EvaluationRefusal::DatetimeOutOfRange {
                operation: ArithmeticOp::Add
            },
            EvaluationRefusal::DatetimeOutOfRange {
                operation: ArithmeticOp::Add
            },
            EvaluationRefusal::DatetimeOutOfRange {
                operation: ArithmeticOp::Multiply
            },
            EvaluationRefusal::DatetimeOutOfRange {
                operation: ArithmeticOp::Subtract
            },
            EvaluationRefusal::DivisionByZero {
                operation: ArithmeticOp::Divide
            },
        ]
    );
}

/// Subtracting an interval whose negation leaves its range is refused, as
/// PostgreSQL 16 and 18 raise there.
#[test]
fn subtracting_an_interval_that_cannot_negate_is_refused() {
    let lowest = Shift {
        span: "-178956970 years -8 mons",
        ..SHIFT
    };
    assert_eq!(
        refusals(&lowest, &["start - span IS NOT NULL"]),
        [EvaluationRefusal::DatetimeOutOfRange {
            operation: ArithmeticOp::Subtract
        }]
    );
}

/// PostgreSQL holds timestamps to the year 294276 and `chrono` to 262142,
/// so a result between the two is refused for the row. Answering it as a
/// null would leave `IS NOT NULL` false where PostgreSQL keeps the row.
#[test]
fn a_timestamp_past_the_last_chrono_year_is_refused() {
    let late = Shift {
        start: "262000-12-31 00:00:00",
        ..SHIFT
    };
    assert_eq!(
        refusals(
            &late,
            &[
                "start + interval '1000 years' IS NOT NULL",
                "(start - span) + '1000 years' > '2024-01-01 00:00:00'",
            ]
        ),
        [
            EvaluationRefusal::DatetimeUnrepresentable {
                operation: ArithmeticOp::Add
            },
            EvaluationRefusal::DatetimeUnrepresentable {
                operation: ArithmeticOp::Add
            },
        ]
    );
    let late_zoned = Shift {
        zoned: "262142-12-31 23:59:59",
        ..SHIFT
    };
    assert_eq!(
        refusals(&late_zoned, &["zoned + interval '1 hour' IS NOT NULL"]),
        [EvaluationRefusal::DatetimeUnrepresentable {
            operation: ArithmeticOp::Add
        }]
    );
}

/// A number no double precision holds, on which PostgreSQL raises scaling
/// an interval by it.
const HUGE_AMOUNT: &str = concat!(
    "99999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
);

/// A number no double holds is refused, and PostgreSQL raises scaling an
/// interval by it in either direction.
#[test]
fn a_number_no_double_holds_is_refused() {
    let big = Shift {
        amount: HUGE_AMOUNT,
        ..SHIFT
    };
    assert_eq!(
        refusals(
            &big,
            &["span * amount > '1 day'", "span / amount > '1 day'"]
        ),
        [
            EvaluationRefusal::DecimalOutsideFloatRange,
            EvaluationRefusal::DecimalOutsideFloatRange,
        ]
    );
}

/// What stays a read: a `timestamptz` moved by months, days or an interval
/// column, which the session's time zone decides, and the operators
/// PostgreSQL cannot resolve or has not.
#[test]
fn what_postgres_or_the_time_zone_decides_is_read() {
    for predicate in [
        "zoned + span > '2024-01-01 00:00:00+00'",
        "zoned + interval '1 day' > '2024-01-01 00:00:00+00'",
        "zoned + (+ interval '90 minutes') > '2024-01-01 00:00:00+00'",
        "zoned - interval '1 mon' > '2024-01-01 00:00:00+00'",
        "day + '1 day' > '2024-01-01'",
        "clock - '01:00' > '00:00'",
        "span * '2' > '1 day'",
        "span % 2 > '1 day'",
        "span - start > '1 day'",
        "2 / span > '1 day'",
        "start - zoned > '1 day'",
        "start + day > '2024-01-01 00:00:00'",
        "factor > (1 + 'a') / 2",
    ] {
        let registered =
            register::<Postgres>(DDL, &format!("SELECT * FROM shifts WHERE {predicate}"));
        assert!(
            registered.not_served_because.is_some(),
            "{predicate} is served in process"
        );
    }
}
