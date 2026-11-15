//! Columns a PostgreSQL catalog declares unquoted in mixed case, read and
//! written back through a live server.
//!
//! PostgreSQL stores an unquoted `Region` as `region`, names it so in every
//! wal2json message, and reads a quoted `"Region"` as another column. Each SQL
//! statement subql renders for the database has to name the stored spelling.
//!
//! Requires Docker. Run with:
//!
//! ```sh
//! cargo test --test it stored_column_names_pg:: \
//!     --features executor-diesel-postgres,membership-term -- --ignored
//! ```
#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::sql_types::BigInt;
use diesel::{sql_query, PgConnection, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{AutoResolvingEngine, PgDieselConnector, SnapshotResult, SyncMode};
use subql::{
    AggregateResultValue, AggregateValueChange, DefaultIds, Registered, SubscriptionEngine,
    SubscriptionRequest, Tier, Wal2JsonV2Event,
};

const LEDGER: &str = "CREATE TABLE ledger (Id INT PRIMARY KEY, Region TEXT NOT NULL, \
                      Amount INT NOT NULL);";

type Engine =
    AutoResolvingEngine<Wal2JsonV2Event, DefaultIds, ParserDB, SyncMode<PgDieselConnector>>;

/// The ledger on `db` with two rows and a wal2json slot opened after them, and
/// an engine reading it under the same catalog.
fn ledger(db: &common::pg::PgDatabase, slot: &str) -> (PgConnection, Engine) {
    let mut conn = db.connect();
    for statement in [
        LEDGER,
        "ALTER TABLE ledger REPLICA IDENTITY FULL",
        "INSERT INTO ledger VALUES (1, 'North', 5), (2, 'South', 7)",
    ] {
        sql_query(statement).execute(&mut conn).expect(statement);
    }
    common::create_slot(&mut conn, slot);
    let catalog = ParserDB::parse::<PostgreSqlDialect>(LEDGER).expect("catalog");
    let inner = SubscriptionEngine::<Wal2JsonV2Event, DefaultIds, ParserDB>::new(
        catalog,
        PostgreSqlDialect {},
    );
    let engine = AutoResolvingEngine::new(inner, SyncMode(PgDieselConnector::new(db.connect())));
    (conn, engine)
}

#[derive(QueryableByName)]
struct Counted {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// How many rows the server answers `sql` with, or a panic carrying the
/// server's refusal and the statement it refused.
fn rows_answering(conn: &mut PgConnection, sql: &str) -> i64 {
    let counted: Vec<Counted> = sql_query(format!("SELECT count(*) AS n FROM ({sql}) AS read"))
        .load(conn)
        .unwrap_or_else(|error| panic!("the server refused the read: {error}\n{sql}"));
    counted[0].n
}

/// Register `sql`, asserting the tier `expected` accepts.
fn register(engine: &mut Engine, sql: &str, expected: fn(&Tier) -> bool) -> Registered {
    let registered = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql),
            (),
        )
        .unwrap_or_else(|error| panic!("{sql} registers: {error:?}"));
    assert!(
        expected(&registered.tier),
        "unexpected tier for {sql}: {:?}",
        registered.tier
    );
    registered
}

/// `lower(Region)` is outside the in-process language, so the filter is asked
/// of the database row by row, and the row's key and every column it selects
/// are named in that read.
#[test]
#[ignore = "requires Docker"]
fn a_keyed_read_names_each_column_as_postgres_stores_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    let slot = db.slot("stored_columns_keyed");
    let (mut conn, mut engine) = ledger(&db, &slot);
    let subscription = register(
        &mut engine,
        "SELECT * FROM ledger WHERE lower(Region) = 'north'",
        |tier| matches!(tier, Tier::KeyedRows { .. }),
    )
    .subscription_id;

    sql_query("INSERT INTO ledger VALUES (3, 'North', 11)")
        .execute(&mut conn)
        .expect("insert");
    let events = common::read_wal2json_v2(&common::drain_slot(&mut conn, &slot));
    let settled = engine.apply(&events[0]).expect("apply").resolve_collect();
    let notifications = settled.reads.expect("the keyed read runs");

    assert_eq!(notifications.row_deltas.len(), 1);
    let delta = &notifications.row_deltas[0];
    assert_eq!(delta.subscription_id, subscription);
    assert_eq!(delta.key, vec![Value::Int(3)]);
    assert_eq!(
        delta.row.as_deref(),
        Some(&[Value::Int(3), Value::String("North".into()), Value::Int(11)][..])
    );
}

/// A grouped `MIN` is re-read by the database, and its seed names the group
/// and the aggregated column.
#[test]
#[ignore = "requires Docker"]
fn a_grouped_reread_seed_names_each_column_as_postgres_stores_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    let (_conn, mut engine) = ledger(&db, &db.slot("stored_columns_min"));
    let subscription = register(
        &mut engine,
        "SELECT Region, MIN(Amount) FROM ledger GROUP BY Region",
        |tier| matches!(tier, Tier::GroupedScalar { .. }),
    )
    .subscription_id;

    let snapshot = engine
        .snapshot(subscription)
        .expect("the seed reads")
        .expect("the subscription exists");

    let SnapshotResult::GroupedAggregate { updates, .. } = snapshot else {
        panic!("expected a grouped snapshot, got {snapshot:?}");
    };
    let mut minima: Vec<_> = updates.into_iter().map(|update| update.change).collect();
    minima.sort_by_key(|change| format!("{change:?}"));
    assert_eq!(
        minima,
        [5, 7].map(
            |amount| AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(amount)))
        )
    );
}

/// A grouped `SUM` is maintained in process from a seed the caller runs, and
/// that seed names the group and the summed column.
#[test]
#[ignore = "requires Docker"]
fn a_grouped_aggregate_seed_names_each_column_as_postgres_stores_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    let (mut conn, mut engine) = ledger(&db, &db.slot("stored_columns_sum"));
    let registered = register(
        &mut engine,
        "SELECT Region, SUM(Amount) FROM ledger GROUP BY Region",
        |tier| !matches!(tier, Tier::GroupedScalar { .. }),
    );
    let bootstrap = registered
        .served()
        .and_then(|served| served.aggregate_bootstrap.as_ref())
        .expect("an in-process aggregate hands out its seed");

    assert_eq!(rows_answering(&mut conn, bootstrap.query.sql()), 2);
}

#[cfg(feature = "membership-term")]
mod membership {
    use super::rows_answering;
    use crate::common;

    use diesel::{sql_query, RunQueryDsl};
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::ConfidenceLevel;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::backend::{Postgres, Value};
    use subql::term::TermDescription;
    use subql::testing::{deployment, TestEvent};
    use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

    const SCHEMA: &str = "CREATE TABLE teams (Id INTEGER PRIMARY KEY);
        CREATE TABLE team_members (TeamId INTEGER REFERENCES teams (Id), UserId TEXT,
                                   PRIMARY KEY (TeamId, UserId));
        CREATE TABLE notes (Id INTEGER PRIMARY KEY, TeamId INTEGER);";

    const TERM: &str = "SELECT * FROM notes WHERE TeamId IN \
         (SELECT TeamId FROM team_members WHERE UserId = current_setting('app.user_id', true))";

    /// The seed read subql hands the caller selects the membership's subject
    /// by its stored name, so the server answers it.
    #[test]
    #[ignore = "requires Docker"]
    fn a_membership_seed_names_each_column_as_postgres_stores_it() {
        common::assert_docker_available();
        let db = common::pg_database();
        let mut conn = db.connect();
        for statement in SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
            sql_query(statement).execute(&mut conn).expect(statement);
        }
        for statement in [
            "INSERT INTO teams VALUES (1)",
            "INSERT INTO team_members VALUES (1, 'alice')",
            "SET app.user_id = 'alice'",
        ] {
            sql_query(statement).execute(&mut conn).expect(statement);
        }
        let engine = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            deployment::db(SCHEMA),
            PostgreSqlDialect {},
        )
        .with_translator(
            TranslatorBuilder::new(deployment::callers())
                .with_min_confidence(ConfidenceLevel::B)
                .build(),
        );
        let request =
            SubscriptionRequest::new(1u64, TERM).subscriber(Value::String("alice".into()));
        let TermDescription::Membership(description) =
            engine.describe_terms(&request).unwrap().remove(0)
        else {
            panic!("a membership subquery is described with its seed read");
        };

        assert_eq!(rows_answering(&mut conn, &description.seed_sql), 1);
    }
}
