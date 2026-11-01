//! Aggregate maintenance restored from the store across a restart.
//!
//! The shards bring an aggregate's bindings back live, so the restarted
//! engine still folds events against it and a stop condition still changes
//! its tier. The registration metadata that change reads has to come back
//! with the store, or the first event the stream cannot answer fails the
//! dispatch.
#![expect(
    clippy::unwrap_used,
    reason = "a setup step that fails leaves nothing for the test to say"
)]

use diesel::connection::SimpleConnection;
use diesel::{Connection as _, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{SQLite, Value};
use subql::reexec::{AutoResolvingEngine, DieselConnector, SyncMode};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggregateSeedInstall, DefaultIds, Install, MaintenanceStopReason,
    SubscriptionEngine, SubscriptionRequest, TableId,
};

type Event = TestEvent<SQLite>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;
type Auto = AutoResolvingEngine<
    Event,
    DefaultIds,
    ParserDB,
    SyncMode<DieselConnector<SqliteConnection, SQLite>>,
>;

const DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)";
const AGGREGATE_SQL: &str = "SELECT COUNT(*) FROM t WHERE a > 1";
/// Shared-cache, so every connection the restart opens sees the same rows.
const URL: &str = "file:agg_restart_maintenance?mode=memory&cache=shared";

/// The parsed catalog and the table id its one table resolves to.
fn catalog() -> (ParserDB, TableId) {
    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id::<SQLite, _>(&catalog, "t").unwrap();
    (catalog, table)
}

fn connector() -> SyncMode<DieselConnector<SqliteConnection, SQLite>> {
    SyncMode(DieselConnector::new(
        SqliteConnection::establish(URL).unwrap(),
    ))
}

#[test]
fn a_restored_aggregate_transitions_when_the_stream_cannot_answer() {
    // One shared in-memory database, held open for the whole test.
    let mut model = SqliteConnection::establish(URL).unwrap();
    model
        .batch_execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);
             INSERT INTO t VALUES (1, 2), (3, 4);",
        )
        .unwrap();

    let store = tempfile::tempdir().unwrap();
    let (first_catalog, table) = catalog();

    // Register the aggregate against the store, seed it from its own
    // numbers, and put the shard and the reads file on disk before the
    // restart.
    let mut first =
        Engine::with_storage(first_catalog, SQLiteDialect {}, store.path().to_path_buf())
            .unwrap()
            .into_parts()
            .0;
    first.set_rotation_threshold(0);
    let registered = first
        .register(SubscriptionRequest::new(1u64, AGGREGATE_SQL))
        .unwrap();
    let subscription = registered.subscription_id;
    Install::install(
        &mut first,
        subscription,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(2)]],
            fence: None,
        },
    )
    .unwrap();
    first.snapshot_table(table).unwrap();
    drop(first);

    // The restart brings the bindings back. The first UPDATE whose old
    // image is missing omits the column the filter reads, so maintenance
    // stops and the tier must change to a read.
    let (second_catalog, _) = catalog();
    let restored =
        Engine::with_storage(second_catalog, SQLiteDialect {}, store.path().to_path_buf()).unwrap();
    let mut restarted: Auto = AutoResolvingEngine::adopt(restored, connector(), |_| (), |_| ());
    restarted.set_rotation_threshold(0);
    let event = TestEvent::update(table, Vec::new(), vec![Value::Int(1), Value::Int(0)])
        .with_pk_columns([0u16])
        .with_changed_columns([1u16]);
    let settled = restarted
        .apply(&event)
        .unwrap_or_else(|error| {
            panic!("the restored aggregate changes tier after the restart: {error}")
        })
        .resolve_collect();
    assert!(
        settled.dispatched.transitions.iter().any(|transition| {
            transition.subscription_id == subscription
                && matches!(
                    transition.reason,
                    MaintenanceStopReason::MissingOldRow { .. }
                )
        }),
        "the stop changes the restored aggregate's tier"
    );
}
