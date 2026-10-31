//! `SUM` and `AVG` over a PostgreSQL `interval` column, maintained in
//! process.
//!
//! Both answer an interval. The total keeps months, days and microseconds
//! apart, the mean divides it as `interval_div` does, and a seed reads the
//! finite rows apart from the infinite ones so that removing an infinity
//! leaves the right total behind.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Interval, Postgres, SQLite, ScalarFamily, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateInstallError, AggregateResultValue, AggregateSeedInstall,
    AggregateValueChange, AggregateValueUpdate, DefaultIds, Install, NumericValue, PgLsn,
    SubscriptionEngine, SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE shifts (id INT PRIMARY KEY, crew TEXT, span INTERVAL);";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let shifts = catalog_helpers::table_id::<Postgres, _>(&database, "shifts").unwrap();
    (
        SubscriptionEngine::new(database, PostgreSqlDialect {}),
        shifts,
    )
}

fn interval(text: &str) -> Interval {
    Interval::parse(text).unwrap()
}

fn row(id: i64, crew: &str, span: Option<&str>) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(crew.into()),
        span.map_or(Value::Null, |span| Value::Interval(interval(span))),
    ]
}

fn insert(shifts: TableId, id: i64, crew: &str, span: Option<&str>, lsn: u64) -> Event {
    TestEvent::insert(shifts, row(id, crew, span))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn delete(shifts: TableId, id: i64, crew: &str, span: Option<&str>, lsn: u64) -> Event {
    TestEvent::delete(shifts, row(id, crew, span))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn change(shifts: TableId, id: i64, from: &str, to: &str, lsn: u64) -> Event {
    TestEvent::update(shifts, row(id, "a", Some(from)), row(id, "a", Some(to)))
        .with_pk_columns([0u16])
        .with_changed_columns([2u16])
        .with_checkpoint(PgLsn(lsn))
}

/// The four seed components: finite sum, finite count, and each infinity's count.
fn seed_row(sum: Option<&str>, finite: i64, infinite: i64, negative: i64) -> Vec<Value<Postgres>> {
    vec![
        sum.map_or(Value::Null, |sum| Value::Interval(interval(sum))),
        Value::Int(finite),
        Value::Int(infinite),
        Value::Int(negative),
    ]
}

fn register(engine: &mut Engine, id: u64, sql: &str) -> subql::AggregateBootstrap<Postgres> {
    engine
        .register(SubscriptionRequest::new(id, sql))
        .unwrap()
        .served()
        .expect("maintained in process")
        .aggregate_bootstrap
        .clone()
        .expect("an aggregate carries its seed query")
}

fn install(
    engine: &mut Engine,
    id: u64,
    rows: Vec<Vec<Value<Postgres>>>,
) -> Result<Vec<AggregateValueUpdate<DefaultIds>>, AggregateInstallError> {
    Install::install(engine, id, AggregateSeedInstall { rows, fence: None }).map(|u| u.updates)
}

fn value(updates: &[AggregateValueUpdate<DefaultIds>]) -> AggValue {
    assert_eq!(updates.len(), 1, "{updates:?}");
    updates[0].folded_value().unwrap()
}

fn printed(value: &AggValue) -> String {
    value.to_string()
}

/// The seed reads the finite rows' sum and count apart from each infinity,
/// through the printed text, which works on PostgreSQL 16 as on 17.
#[test]
fn the_seed_reads_finite_rows_apart_from_infinite_ones() {
    let (mut engine, _) = engine();
    let bootstrap = register(&mut engine, 1, "SELECT AVG(span) FROM shifts");
    assert_eq!(
        bootstrap.query.sql(),
        "SELECT SUM(span) FILTER (WHERE (span)::TEXT NOT IN ('infinity', '-infinity')) AS c0, \
         COUNT(span) FILTER (WHERE (span)::TEXT NOT IN ('infinity', '-infinity')) AS c1, \
         COUNT(span) FILTER (WHERE (span)::TEXT = 'infinity') AS c2, \
         COUNT(span) FILTER (WHERE (span)::TEXT = '-infinity') AS c3 FROM shifts"
    );
    assert_eq!(
        bootstrap.kinds,
        vec![
            ScalarFamily::Interval,
            ScalarFamily::Int,
            ScalarFamily::Int,
            ScalarFamily::Int
        ]
    );
}

/// `SUM` keeps the fields apart and `AVG` divides as PostgreSQL does, and
/// both follow inserts, updates and deletes back to NULL.
#[test]
fn sum_and_avg_follow_every_change() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT SUM(span) FROM shifts");
    register(&mut engine, 2, "SELECT AVG(span) FROM shifts");
    assert_eq!(
        printed(&value(
            &install(&mut engine, 1, vec![seed_row(Some("1 mon"), 1, 0, 0)]).unwrap()
        )),
        "1 mon"
    );
    install(&mut engine, 2, vec![seed_row(Some("1 mon"), 1, 0, 0)]).unwrap();

    let answers = |engine: &mut Engine, event: &Event| -> Vec<String> {
        let mut updates = engine.aggregate_updates(event).unwrap();
        updates.sort_by_key(|update| update.subscription);
        updates
            .iter()
            .map(|u| printed(&u.folded_value().unwrap()))
            .collect()
    };
    assert_eq!(
        answers(&mut engine, &insert(shifts, 2, "a", Some("1 day"), 10)),
        ["1 mon 1 day", "15 days 12:00:00"]
    );
    assert_eq!(
        answers(&mut engine, &insert(shifts, 3, "a", Some("00:00:01"), 11)),
        ["1 mon 1 day 00:00:01", "10 days 08:00:00.333333"]
    );
    assert_eq!(
        answers(&mut engine, &insert(shifts, 4, "a", None, 12)),
        Vec::<String>::new(),
        "a NULL interval moves neither"
    );
    // PostgreSQL 16 answers this mean, the day's remainder cascading below zero.
    assert_eq!(
        answers(
            &mut engine,
            &change(shifts, 2, "1 day", "-1 days +02:00:00", 13)
        ),
        ["1 mon -1 days +02:00:01", "10 days -07:19:59.666667"]
    );
    for (id, span, lsn) in [(1, "1 mon", 14), (2, "-1 days +02:00:00", 15)] {
        answers(&mut engine, &delete(shifts, id, "a", Some(span), lsn));
    }
    let updates = engine
        .aggregate_updates(&delete(shifts, 3, "a", Some("00:00:01"), 16))
        .unwrap();
    assert!(updates.iter().all(|update| matches!(
        update.folded_value(),
        Some(AggValue::Sum(None) | AggValue::Avg(None))
    )));
}

/// A seeded infinity answers infinity, and once it is deleted the finite
/// rows the seed read apart are what remains.
#[test]
fn an_infinite_seed_gives_back_its_finite_rows() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT AVG(span) FROM shifts");
    let opening = install(&mut engine, 1, vec![seed_row(Some("1 mon 1 day"), 2, 1, 0)]).unwrap();
    assert_eq!(
        value(&opening),
        AggValue::Avg(Some(NumericValue::Interval(interval("infinity"))))
    );
    let updates = engine
        .aggregate_updates(&delete(shifts, 9, "a", Some("infinity"), 10))
        .unwrap();
    assert_eq!(printed(&value(&updates)), "15 days 12:00:00");
}

/// Infinities of both signs and a field leaving its range are PostgreSQL's
/// `interval out of range`, so the total stops and a read answers.
#[test]
fn a_total_postgres_raises_on_stops_maintenance() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT SUM(span) FROM shifts");
    assert_eq!(
        install(&mut engine, 1, vec![seed_row(None, 0, 1, 1)]).err(),
        Some(AggregateInstallError::SumOutOfRange(1))
    );
    install(
        &mut engine,
        1,
        vec![seed_row(Some("2000000000 mons"), 1, 0, 0)],
    )
    .unwrap();
    let transitions = engine
        .aggregate_updates(&insert(shifts, 2, "a", Some("2000000000 mons"), 10))
        .unwrap()
        .transitions;
    assert!(
        matches!(
            transitions.first().map(|transition| &transition.reason),
            Some(subql::MaintenanceStopReason::SumOutOfRange { .. })
        ),
        "{transitions:?}"
    );
}

/// Each group keeps its own interval total.
#[test]
fn a_grouped_sum_keeps_one_total_per_group() {
    let (mut engine, shifts) = engine();
    let bootstrap = register(
        &mut engine,
        1,
        "SELECT crew, SUM(span) FROM shifts GROUP BY crew",
    );
    assert_eq!(bootstrap.kinds.len(), 6, "crew, four components, row count");
    let mut opening = install(
        &mut engine,
        1,
        vec![
            vec![
                Value::String("a".into()),
                Value::Interval(interval("1 mon")),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(1),
            ],
            vec![
                Value::String("b".into()),
                Value::Interval(interval("2 days")),
                Value::Int(2),
                Value::Int(0),
                Value::Int(0),
                Value::Int(2),
            ],
        ],
    )
    .unwrap();
    opening.sort_by_key(|update| printed(&update.folded_value().unwrap()));
    assert_eq!(
        opening
            .iter()
            .map(|u| printed(&u.folded_value().unwrap()))
            .collect::<Vec<_>>(),
        ["1 mon", "2 days"]
    );
    let updates = engine
        .aggregate_updates(&insert(shifts, 4, "b", Some("12:00:00"), 10))
        .unwrap();
    assert_eq!(
        updates[0].change,
        AggregateValueChange::Set(AggregateResultValue::Folded(AggValue::Sum(Some(
            NumericValue::Interval(interval("2 days 12:00:00"))
        ))))
    );
}

/// The mean's day remainder can run past the day, and the cascade carries a
/// whole day into the clock, which PostgreSQL 16 and 18 answer alike.
#[test]
fn the_mean_cascades_the_day_remainder_into_the_clock() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT AVG(span) FROM shifts");
    assert_eq!(
        printed(&value(
            &install(
                &mut engine,
                1,
                vec![seed_row(Some("1 mon 6 days"), 1, 0, 0)]
            )
            .unwrap()
        )),
        "1 mon 6 days"
    );
    for (id, lsn) in [(2, 10), (3, 11), (4, 12), (5, 13), (6, 14)] {
        assert!(engine
            .aggregate_updates(&insert(shifts, id, "a", Some("0"), lsn))
            .is_ok(),);
    }
    assert_eq!(
        printed(&value(
            &engine
                .aggregate_updates(&insert(shifts, 7, "a", Some("0"), 15))
                .unwrap()
        )),
        "5 days 03:25:42.832457"
    );
}

/// A one-row mean over a clock past the double's exact integers reads its
/// microseconds back unchanged, as PostgreSQL 16 and 18 both print it.
#[test]
fn a_huge_clock_keeps_its_own_mean() {
    let (mut engine, _) = engine();
    register(&mut engine, 1, "SELECT AVG(span) FROM shifts");
    assert_eq!(
        printed(&value(
            &install(
                &mut engine,
                1,
                vec![seed_row(Some("5000000 hours"), 1, 0, 0)]
            )
            .unwrap()
        )),
        "5000000:00:00"
    );
}

/// A day total past the field's range stops maintenance, and PostgreSQL 16
/// and 18 both raise `interval out of range` on the same rows.
#[test]
fn a_day_total_that_leaves_range_stops_maintenance() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT SUM(span) FROM shifts");
    install(
        &mut engine,
        1,
        vec![seed_row(Some("1500000000 days"), 1, 0, 0)],
    )
    .unwrap();
    let transitions = engine
        .aggregate_updates(&insert(shifts, 2, "a", Some("1500000000 days"), 10))
        .unwrap()
        .transitions;
    assert!(
        matches!(
            transitions.first().map(|transition| &transition.reason),
            Some(subql::MaintenanceStopReason::SumOutOfRange { .. })
        ),
        "{transitions:?}"
    );
}

/// A clock total past the field's range stops maintenance, and PostgreSQL
/// 16 and 18 both raise `interval out of range` on the same rows.
#[test]
fn a_clock_total_that_leaves_range_stops_maintenance() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT SUM(span) FROM shifts");
    install(
        &mut engine,
        1,
        vec![seed_row(Some("4611686018427387904 microseconds"), 1, 0, 0)],
    )
    .unwrap();
    let transitions = engine
        .aggregate_updates(&insert(
            shifts,
            2,
            "a",
            Some("4611686018427387904 microseconds"),
            10,
        ))
        .unwrap()
        .transitions;
    assert!(
        matches!(
            transitions.first().map(|transition| &transition.reason),
            Some(subql::MaintenanceStopReason::SumOutOfRange { .. })
        ),
        "{transitions:?}"
    );
}

/// A net total holding the infinity's own fields is the infinity, which the
/// engine cannot answer. PostgreSQL 16 answers a value and 18 raises on the
/// same rows, so the stop serves the database's answer.
#[test]
fn a_total_holding_the_infinity_fields_stops_maintenance() {
    let (mut engine, shifts) = engine();
    register(&mut engine, 1, "SELECT SUM(span) FROM shifts");
    install(&mut engine, 1, vec![seed_row(None, 0, 0, 0)]).unwrap();
    for (id, span, lsn) in [
        (1, "178956970 years 7 mons", 10),
        (2, "2147483647 days", 11),
    ] {
        let updates = engine
            .aggregate_updates(&insert(shifts, id, "a", Some(span), lsn))
            .unwrap();
        assert!(
            updates.transitions.iter().all(|transition| {
                !matches!(
                    transition.reason,
                    subql::MaintenanceStopReason::SumOutOfRange { .. }
                )
            }),
            "{span} stays in range"
        );
    }
    let transitions = engine
        .aggregate_updates(&insert(
            shifts,
            3,
            "a",
            Some("9223372036854775807 microseconds"),
            12,
        ))
        .unwrap()
        .transitions;
    assert!(
        matches!(
            transitions.first().map(|transition| &transition.reason),
            Some(subql::MaintenanceStopReason::SumOutOfRange { .. })
        ),
        "{transitions:?}"
    );
}

/// An install over no rows answers NULL for both functions, as PostgreSQL's
/// `sum` and `avg` do over no rows.
#[test]
fn an_install_over_no_rows_answers_null() {
    let (mut engine, _) = engine();
    register(&mut engine, 1, "SELECT SUM(span) FROM shifts");
    register(&mut engine, 2, "SELECT AVG(span) FROM shifts");
    let updates = install(&mut engine, 1, vec![seed_row(None, 0, 0, 0)]).unwrap();
    assert!(matches!(&value(&updates), AggValue::Sum(None)));
    let updates = install(&mut engine, 2, vec![seed_row(None, 0, 0, 0)]).unwrap();
    assert!(matches!(&value(&updates), AggValue::Avg(None)));
}

/// A grouped install whose seed holds the infinity's own fields refuses to
/// maintain the total. PostgreSQL 16's seed read reports exactly that
/// value on such rows, and the engine cannot carry it.
#[test]
fn a_grouped_install_whose_total_leaves_range_stops() {
    let (mut engine, _) = engine();
    register(
        &mut engine,
        1,
        "SELECT crew, SUM(span) FROM shifts GROUP BY crew",
    );
    assert_eq!(
        install(
            &mut engine,
            1,
            vec![vec![
                Value::String("a".into()),
                Value::Interval(interval(
                    "178956970 years 7 mons 2147483647 days 9223372036854775807 microseconds",
                ),),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(1),
            ]]
        )
        .err(),
        Some(AggregateInstallError::SumOutOfRange(1))
    );
}

/// What stays a read: the variance family, `HAVING` over an interval
/// aggregate, and every interval aggregate on an engine without the type.
#[test]
fn what_is_not_maintained_is_read() {
    for statement in [
        "SELECT VAR_POP(span) FROM shifts",
        "SELECT crew, SUM(span) FROM shifts GROUP BY crew HAVING SUM(span) > 1",
        "SELECT crew, AVG(span) FROM shifts GROUP BY crew HAVING COUNT(span) > 1",
    ] {
        let registered = crate::common::semantics::register::<Postgres>(DDL, statement);
        assert!(registered.not_served_because.is_some(), "{statement}");
    }
    assert!(crate::common::semantics::register::<Postgres>(
        DDL,
        "SELECT crew, SUM(span) FROM shifts GROUP BY crew HAVING COUNT(*) > 1"
    )
    .not_served_because
    .is_none());
    let sqlite = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
    let mut sqlite: SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(sqlite, SQLiteDialect {});
    assert!(sqlite
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT SUM(span) FROM shifts"
        ))
        .unwrap()
        .not_served_because
        .is_some());
}

/// The replay detector folds an interval `SUM` from its retained rows,
/// empties it on a truncate as PostgreSQL's empty `sum` answers, and
/// carries it through a checkpoint.
#[test]
fn the_replay_detector_folds_an_interval_sum() {
    use subql::{ReplayAnswer, ReplayDetector, ReplayTable, ReplayValue};

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let shifts = catalog_helpers::table_id::<Postgres, _>(&catalog, "shifts").unwrap();
    let engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});
    let spec = || SubscriptionRequest::new(1, "SELECT SUM(span) FROM shifts");
    let mut detector: ReplayDetector<Postgres> = engine
        .replay_detector(
            &spec(),
            vec![ReplayTable {
                table_id: shifts,
                rows: vec![row(1, "a", Some("1 mon")), row(2, "a", Some("infinity"))],
            }],
        )
        .unwrap();
    let sum = |detector: &ReplayDetector<Postgres>| match detector.answer() {
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::Sum(sum))) => sum.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        sum(&detector),
        Some(NumericValue::Interval(interval("infinity")))
    );
    let removal = TestEvent::<Postgres>::delete(shifts, row(2, "a", Some("infinity")))
        .with_pk_columns([0u16]);
    assert!(detector.apply_transaction(&[removal], &catalog).unwrap());
    assert_eq!(
        sum(&detector),
        Some(NumericValue::Interval(interval("1 mon")))
    );
    let truncate = TestEvent::<Postgres>::truncate(shifts);
    assert!(detector.apply_transaction(&[truncate], &catalog).unwrap());
    assert_eq!(sum(&detector), None);

    let bytes = detector.checkpoint().unwrap();
    let restored: ReplayDetector<Postgres> =
        engine.restore_replay_detector(&spec(), &bytes).unwrap();
    assert_eq!(restored.answer(), detector.answer());
}
