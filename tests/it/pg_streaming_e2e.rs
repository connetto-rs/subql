//! Docker-backed end-to-end tests for [`PgStreamingCdcSource`].
//!
//! Each test is `#[ignore]` so default `cargo test` does not require
//! Docker. Run with:
//!
//! ```sh
//! cargo test --test it pg_streaming_e2e:: --features pg-streaming \
//!     -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::items_after_statements
)]

use crate::common;

use parking_lot::Mutex;
use std::net::{Shutdown as SocketShutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use diesel::{sql_query, Connection, ExpressionMethods, PgConnection, RunQueryDsl};
use pg_walstream::{error::ReplicationError, ColumnValue, EventType, ReplicaIdentity};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::CdcEvent;
use subql::{
    CdcSource, EventKind, PgChangeEvent, PgCommit, PgCommitPosition, PgLsn, PgRelation,
    PgRelationColumn, PgStreamingCdcSource, PgStreamingConfig, PgStreamingError, SourceItem,
    TimelineSwitch,
};

diesel::table! {
    orders (id) {
        id -> Integer,
        price -> Double,
    }
}

/// Insert one row per id in a single statement, so in one transaction.
fn insert(conn: &mut PgConnection, ids: &[i32]) {
    let rows: Vec<_> = ids
        .iter()
        .map(|&id| (orders::id.eq(id), orders::price.eq(f64::from(id))))
        .collect();
    diesel::insert_into(orders::table)
        .values(&rows)
        .execute(conn)
        .expect("insert");
}

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";
const SCRATCH_DDL: &str = "CREATE TABLE scratch (id INT PRIMARY KEY, note TEXT);";

/// How long a test waits for something that must arrive.
///
/// Liveness only: no assertion below measures how fast anything is, so this
/// is deliberately generous. A short wait measures the runner instead of the
/// code, and a shared CI runner starting eight Postgres containers stalls for
/// seconds at a time.
const ARRIVAL: Duration = Duration::from_secs(30);

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

/// Open a replication-mode connection and validate it accepts
/// `IDENTIFY_SYSTEM`. No DML, no streaming yet. Bare constructor
/// contract.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn connect_against_real_pg() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_connect");
    let publication = "subql_pg_streaming_connect_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_secs(10));

    current_thread_rt().block_on(async move {
        let _source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect succeeds against live PG with valid slot");
    });

    common::drop_slot(&mut setup, &slot);
}

#[test]
fn invalid_connection_parameters_are_reported_as_postgres_errors() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(
        "subql_invalid_option=1",
        "unused_slot",
        "unused_publication",
    );
    let error = current_thread_rt().block_on(async move {
        tokio::time::timeout(ARRIVAL, PgStreamingCdcSource::connect(config, catalog))
            .await
            .expect("invalid parameters are refused promptly")
            .err()
            .expect("an invalid connection option must fail")
    });
    assert!(matches!(error, PgStreamingError::Postgres(_)), "{error:?}");
}

/// Query the slot's `confirmed_flush_lsn` via a side connection and
/// parse the `XXXX/YYYY` text form into a [`PgLsn`].
fn confirmed_flush_lsn(conn: &mut diesel::PgConnection, slot: &str) -> Option<PgLsn> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        confirmed_flush_lsn: String,
    }
    // pg_lsn has no Diesel type mapping; the ::text cast requires raw SQL
    let rows: Vec<Row> = diesel::sql_query(format!(
        "SELECT confirmed_flush_lsn::text AS confirmed_flush_lsn \
         FROM pg_replication_slots WHERE slot_name = '{slot}'"
    ))
    .load(conn)
    .expect("query pg_replication_slots");
    rows.into_iter()
        .next()
        .and_then(|r| PgLsn::parse(&r.confirmed_flush_lsn))
}

/// Drive an INSERT in a side connection and assert `source.next_item()`
/// returns the corresponding typed CDC event without waiting for a tick.
///
/// The claim is that delivery is wire-driven, so the ceiling is stated
/// against the configuration rather than against the clock: the status
/// interval is set to thirty seconds and the event must arrive within a sixth
/// of it. An implementation that surfaced events on its interval instead
/// would blow that by five seconds, while a wire-driven one has three orders
/// of magnitude of headroom, measured at four milliseconds on an idle machine
/// and fifteen under contention. The observed latency is reported rather than
/// asserted, because a wall-clock number measures the runner: a ceiling of
/// two hundred milliseconds failed CI on 2026-09-07 while passing locally.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn next_item_delivers_an_insert_without_waiting_for_a_tick() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_next_item");
    let publication = "subql_pg_streaming_next_item_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    // Status interval far above the ceiling so a tick-driven event cannot pass the latency test.
    const STATUS_INTERVAL: Duration = Duration::from_secs(30);
    const CEILING: Duration = Duration::from_secs(5);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config =
        PgStreamingConfig::new(db.url(), &slot, publication).status_interval(STATUS_INTERVAL);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Commit before any next_item() poll so the timestamp precedes the wait.
        let commit_at = Instant::now();
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");

        let _relation = tokio::time::timeout(CEILING, source.next_item())
            .await
            .expect("the relation must arrive on the wire, not on the status interval")
            .expect("next_item must not error")
            .and_then(SourceItem::into_relation)
            .expect("the fresh slot states its table first");
        let event = tokio::time::timeout(CEILING, source.next_item())
            .await
            .expect("the event must arrive on the wire, not on the status interval")
            .expect("next_item must not error")
            .and_then(SourceItem::into_event)
            .expect("the row arrives before its commit");
        // Reported only; wall-clock includes the blocking INSERT and runner scheduling.
        let observed_latency = commit_at.elapsed();

        assert_eq!(
            event.kind(),
            EventKind::Insert,
            "first event must be the INSERT we just issued, got {:?}",
            event.kind()
        );
        println!(
            "COMMIT-to-event latency: {}us (ceiling: {}s, status interval: {}s)",
            observed_latency.as_micros(),
            CEILING.as_secs(),
            STATUS_INTERVAL.as_secs()
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// A fresh slot's stream opens with the published table's `Relation`
/// message, which states the table's definition ahead of its first change,
/// and re-states it after the server changes the definition.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_fresh_slot_delivers_the_tables_relation_first() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_relation");
    let publication = "subql_pg_streaming_relation_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");

        let first = next_item(&mut source).await;
        let relation: PgRelation = first
            .into_relation()
            .expect("the first item is the table's relation");
        assert_eq!(relation.schema(), "public");
        assert_eq!(relation.table(), "orders");
        assert_eq!(relation.replica_identity(), ReplicaIdentity::Default);
        let names: Vec<&str> = relation
            .columns()
            .iter()
            .map(PgRelationColumn::name)
            .collect();
        assert_eq!(names, vec!["id", "price"]);

        let event = next_item(&mut source).await;
        assert!(
            event.into_event().is_some(),
            "the insert follows its table's relation"
        );
        let commit = next_item(&mut source).await;
        assert!(commit.into_commit().is_some(), "the commit follows");

        // A definition change re-states the relation ahead of the next
        // change, with the new column and the identity unchanged.
        sql_query("ALTER TABLE orders ADD COLUMN status TEXT")
            .execute(&mut dml)
            .expect("add a column the catalog lacks");
        sql_query("INSERT INTO orders VALUES (2, 25.0, 'paid')")
            .execute(&mut dml)
            .expect("second insert");

        let restated = next_item(&mut source).await;
        let relation: PgRelation = restated
            .into_relation()
            .expect("the change re-states the relation");
        let names: Vec<&str> = relation
            .columns()
            .iter()
            .map(PgRelationColumn::name)
            .collect();
        assert_eq!(
            names,
            vec!["id", "price", "status"],
            "the restated relation carries the new column"
        );
        assert_eq!(relation.replica_identity(), ReplicaIdentity::Default);

        let event = next_item(&mut source).await;
        assert!(
            event.into_event().is_some(),
            "the second insert follows its restated relation"
        );
        let commit = next_item(&mut source).await;
        assert!(commit.into_commit().is_some(), "its commit follows");
    });
    common::drop_slot(&mut setup, &slot);
}

/// Acknowledging a commit moves the slot's `confirmed_flush_lsn` to exactly
/// the end the commit named, so a consumer that stored that end before
/// acknowledging holds the position the slot resumes from.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_a_commit_moves_the_slot_to_the_end_it_named() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_ack");
    let publication = "subql_pg_streaming_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1]);
        let row = next_row(&mut source).await;
        let commit = next_commit(&mut source).await;
        assert_eq!(
            commit.position(),
            PgCommitPosition::at_commit(row.position().commit_lsn(), row.position().xid()),
            "the commit follows its row"
        );
        assert!(commit.end_lsn() > row.position().commit_lsn());

        source.ack(commit.position()).await.expect("ack the commit");
        let flushed = flushed_after_ack(
            &mut source,
            |lsn| lsn >= commit.end_lsn(),
            &mut probe,
            &slot_inner,
        )
        .await;
        assert_eq!(flushed, commit.end_lsn());
        assert_eq!(source.acknowledged_position(), Some(commit.end_lsn()));
    });

    common::drop_slot(&mut setup, &slot);
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_early_commit_acknowledgement_waits_for_commit_delivery() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_early_ack");
    let publication = "subql_pg_streaming_early_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let base = confirmed_flush_lsn(&mut probe, &slot_inner).expect("the slot exists");
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1, 2]);
        let first = next_row(&mut source).await;
        let position =
            PgCommitPosition::at_commit(first.position().commit_lsn(), first.position().xid());
        tokio::time::timeout(ARRIVAL, source.ack(position))
            .await
            .expect("the acknowledgement responds")
            .expect("queue the early acknowledgement");
        assert_eq!(source.acknowledged_position(), Some(base));
        assert_eq!(confirmed_flush_lsn(&mut probe, &slot_inner), Some(base));

        let second = next_row(&mut source).await;
        assert!(first.position() < second.position());
        assert_eq!(second.position().commit_lsn(), position.commit_lsn());
        assert_eq!(source.acknowledged_position(), Some(base));
        let commit = next_commit(&mut source).await;
        assert_eq!(commit.position(), position);
        assert_eq!(source.acknowledged_position(), Some(commit.end_lsn()));
        let flushed = flushed_after_ack(
            &mut source,
            |lsn| lsn >= commit.end_lsn(),
            &mut probe,
            &slot_inner,
        )
        .await;
        assert_eq!(flushed, commit.end_lsn());
    });

    common::drop_slot(&mut setup, &slot);
}

/// Acknowledging every row of a transaction without its commit leaves the
/// slot where it started, however many status updates go out.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_every_row_without_the_commit_never_moves_the_slot() {
    const STATUS_INTERVAL: Duration = Duration::from_millis(100);
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_rows_only");
    let publication = "subql_pg_streaming_rows_only_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config =
        PgStreamingConfig::new(db.url(), &slot, publication).status_interval(STATUS_INTERVAL);
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let started = confirmed_flush_lsn(&mut probe, &slot_inner).expect("the slot exists");
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1, 2]);
        next_row(&mut source).await;
        let last = next_row(&mut source).await;
        next_commit(&mut source).await;

        source.ack(last.position()).await.expect("ack the last row");
        // Let several status intervals elapse, driving the reader so the
        // periodic feedback the slot would act on actually leaves the source.
        drive_idle(&mut source, Duration::from_millis(500)).await;
        assert_eq!(
            confirmed_flush_lsn(&mut probe, &slot_inner),
            Some(started),
            "rows alone must not release their transaction"
        );
        assert_eq!(source.acknowledged_position(), Some(started));
    });

    common::drop_slot(&mut setup, &slot);
}

type Item = SourceItem<PgChangeEvent, PgCommit, PgRelation>;

/// Commit the newer transaction while the older one is open, then the
/// older, the shape every concurrent writer produces.
fn interleave(older: &mut PgConnection, newer: &mut PgConnection, rows_in_the_older: usize) {
    let older_ids: Vec<i32> = (0..rows_in_the_older)
        .map(|id| 100 + i32::try_from(id).expect("few rows"))
        .collect();
    older
        .transaction::<_, diesel::result::Error, _>(|older| {
            insert(older, &older_ids);
            insert(newer, &[1]);
            Ok(())
        })
        .expect("commit the older");
}

/// The items an older transaction committing after a newer one delivers.
fn interleaved_items(rows_in_the_older: usize) -> Vec<Item> {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut older = db.connect();
    let mut newer = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_commit_order");
    let publication = "subql_pg_streaming_commit_order_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);

    let items = current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        interleave(&mut older, &mut newer, rows_in_the_older);
        let mut items = Vec::new();
        // A fresh slot states the table's relation before the first row,
        // and the commit-order assertions below are about rows and commits.
        let first = next_item(&mut source).await;
        assert!(
            matches!(first, SourceItem::Relation(_)),
            "the fresh slot states its table first, got {first:?}"
        );
        // The newer's row and commit, then the older's rows and commit.
        for _ in 0..rows_in_the_older + 3 {
            items.push(next_item(&mut source).await);
        }
        items
    });
    common::drop_slot(&mut setup, &slot);
    items
}

fn item_position(item: &Item) -> PgCommitPosition {
    match item {
        SourceItem::Event(event) => event.position(),
        SourceItem::Commit(commit) => commit.position(),
        // A relation carries a message LSN, not a commit position, so a
        // test that positions items must consume it on its own first.
        SourceItem::Relation(_) => {
            panic!("a relation has no commit position, got {item:?}")
        }
    }
}

/// The next item, which must arrive.
async fn next_item(source: &mut PgStreamingCdcSource) -> Item {
    tokio::time::timeout(ARRIVAL, source.next_item())
        .await
        .expect("the item arrives")
        .expect("next_item must not error")
        .expect("source must not have shut down")
}

/// The next row, skipping the table definitions a fresh slot states first.
async fn next_row(source: &mut PgStreamingCdcSource) -> PgChangeEvent {
    loop {
        match next_item(source).await {
            SourceItem::Event(event) => return event,
            SourceItem::Relation(_) => {}
            SourceItem::Commit(commit) => panic!("expected a row, got {commit:?}"),
        }
    }
}

/// The next commit, skipping the table definitions a fresh slot states
/// first.
async fn next_commit(source: &mut PgStreamingCdcSource) -> PgCommit {
    loop {
        match next_item(source).await {
            SourceItem::Commit(commit) => return commit,
            SourceItem::Relation(_) => {}
            SourceItem::Event(event) => panic!("expected a commit, got {event:?}"),
        }
    }
}

/// Each transaction's rows and then its commit arrive in commit order, so
/// the newer transaction's commit comes before the older one's rows.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn items_follow_commit_order_across_interleaved_transactions() {
    let items = interleaved_items(1);
    let shape: Vec<bool> = items
        .iter()
        .map(|item| matches!(item, SourceItem::Commit(_)))
        .collect();
    assert_eq!(
        shape,
        [false, true, false, true],
        "row, commit, row, commit: {items:?}"
    );
    assert!(
        item_position(&items[0]) < item_position(&items[2]),
        "the newer transaction committed first, so its row must order first: {items:?}"
    );
}

/// Every delivered item orders strictly after the one before it, including the rows inside one
/// transaction and the commit after them, so a consumer resuming from any position it holds misses
/// nothing after it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn positions_strictly_increase_through_a_multi_row_transaction() {
    let positions: Vec<PgCommitPosition> = interleaved_items(2).iter().map(item_position).collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "positions must strictly increase in delivery order: {positions:?}"
    );
}

/// Acknowledging the first of two interleaved commits releases the slot to
/// exactly that commit's end, before the second transaction.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_the_first_of_interleaved_commits_releases_exactly_its_end() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    let mut older = db.connect();
    let mut newer = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_interleaved_ack");
    let publication = "subql_pg_streaming_interleaved_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        interleave(&mut older, &mut newer, 1);
        next_row(&mut source).await;
        let first = next_commit(&mut source).await;
        next_row(&mut source).await;
        let second = next_commit(&mut source).await;
        assert!(first.end_lsn() < second.end_lsn());

        source
            .ack(first.position())
            .await
            .expect("ack the first commit");
        let flushed = flushed_after_ack(
            &mut source,
            |lsn| lsn >= first.end_lsn(),
            &mut probe,
            &slot_inner,
        )
        .await;
        assert_eq!(flushed, first.end_lsn());
    });

    common::drop_slot(&mut setup, &slot);
}

/// Connect, waiting out the walsender of a source just dropped, which holds
/// the slot until it notices.
async fn connect_when_free(config: &PgStreamingConfig) -> PgStreamingCdcSource {
    let deadline = Instant::now() + ARRIVAL;
    loop {
        let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let error = match PgStreamingCdcSource::connect(config.clone(), catalog).await {
            Ok(source) => return source,
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("is active") && Instant::now() < deadline,
            "connect failed {error}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the reader without expecting an item, so the keepalive feedback the
/// server needs actually leaves the source while the database sits idle.
async fn drive_idle(source: &mut PgStreamingCdcSource, for_: Duration) {
    let deadline = Instant::now() + for_;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), source.next_item()).await {
            Ok(Ok(Some(item))) => panic!("an item arrived in an idle database {item:?}"),
            Ok(Ok(None)) => panic!("the source closed while the database was idle"),
            Ok(Err(err)) => panic!("next_item failed while the database was idle {err}"),
            Err(_) => {}
        }
    }
}

/// Whether the server still reports a live backend holding the slot.
///
/// Replication-slot administration, which the query DSL does not express.
fn slot_is_active(conn: &mut diesel::PgConnection, slot: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        active: bool,
    }
    let row: Row = diesel::sql_query(format!(
        "SELECT active_pid IS NOT NULL AS active \
         FROM pg_replication_slots WHERE slot_name = '{slot}'"
    ))
    .get_result(conn)
    .expect("query pg_replication_slots");
    row.active
}

/// Wait until the slot's confirmed flush passes `past`, driving the reader
/// between queries so the feedback reaches the server, and return the
/// position the server holds.
async fn flushed_after_ack(
    source: &mut PgStreamingCdcSource,
    past: impl Fn(PgLsn) -> bool,
    probe: &mut diesel::PgConnection,
    slot: &str,
) -> PgLsn {
    let deadline = Instant::now() + ARRIVAL;
    loop {
        drive_idle(source, Duration::from_millis(100)).await;
        let flushed = confirmed_flush_lsn(probe, slot).expect("the slot exists");
        if past(flushed) {
            return flushed;
        }
        assert!(
            Instant::now() < deadline,
            "the slot never took the acknowledgement"
        );
    }
}

/// Acknowledging part of a transaction leaves the slot before it, so a fresh
/// source receives the whole transaction again, commit and end included, and
/// acknowledging its commit releases it, so a fresh source never sees it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_part_of_a_transaction_never_releases_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_partial_ack");
    let publication = "subql_pg_streaming_partial_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1, 2]);
        let first = next_row(&mut source).await.position();
        let second = next_row(&mut source).await.position();
        let commit = next_commit(&mut source).await;

        source.ack(first).await.expect("ack the first row");
        let flushed = flushed_after_ack(&mut source, |_| true, &mut probe, &slot_inner).await;
        assert!(
            flushed < first.commit_lsn(),
            "the slot passed a transaction the consumer holds half of: {flushed:?} against {first:?}"
        );
        drop(source);
        let mut source = connect_when_free(&config).await;
        let again = [
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
        ];
        assert_eq!(
            again,
            [first, second],
            "the half-acknowledged transaction must come back whole"
        );
        assert_eq!(next_commit(&mut source).await, commit, "with the same end");

        source.ack(commit.position()).await.expect("ack the commit");
        flushed_after_ack(&mut source, |lsn| lsn >= commit.end_lsn(), &mut probe, &slot_inner).await;
        drop(source);
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[3]);
        let after = next_row(&mut source).await.position();
        assert!(
            after > commit.position(),
            "the acknowledged transaction must not come back, got {after:?}"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Resuming from a position delivers exactly the items after it, none
/// skipped and none repeated: from a row, the rest of its transaction and
/// its commit with the same end, from the last row, the commit alone, and
/// from the commit, the next transaction.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn resuming_from_a_position_delivers_exactly_the_items_after_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_resume");
    let publication = "subql_pg_streaming_resume_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = |start: Option<PgCommitPosition>| {
        PgStreamingConfig::new(db.url(), &slot, publication).start(start)
    };

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config(None)).await;
        insert(&mut dml, &[1, 2, 3]);
        let rows = [
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
        ];
        let commit = next_commit(&mut source).await;
        drop(source);

        let mut source = connect_when_free(&config(Some(rows[0]))).await;
        let rest = [
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
        ];
        assert_eq!(rest, rows[1..], "the rest of the transaction, once each");
        assert_eq!(
            next_commit(&mut source).await,
            commit,
            "then its commit, same end"
        );
        insert(&mut dml, &[4]);
        let next = next_row(&mut source).await.position();
        assert!(
            next > commit.position(),
            "then the next transaction, got {next:?}"
        );
        drop(source);

        let mut source = connect_when_free(&config(Some(rows[2]))).await;
        assert_eq!(
            next_commit(&mut source).await,
            commit,
            "resuming after the last row delivers the commit it had not had"
        );
        drop(source);

        let mut source = connect_when_free(&config(Some(commit.position()))).await;
        assert_eq!(
            next_row(&mut source).await.position(),
            next,
            "resuming after the commit starts at the next transaction"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Resuming from an unacknowledged row must not release any WAL before the
/// consumer acknowledges the transaction's commit.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn resuming_from_an_unacknowledged_row_releases_nothing_until_the_commit_is_acked() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_resume_retention");
    let publication = "subql_pg_streaming_resume_retention_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = |start: Option<PgCommitPosition>| {
        PgStreamingConfig::new(db.url(), &slot, publication)
            .start(start)
            .status_interval(Duration::from_millis(100))
    };
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config(None)).await;
        insert(&mut dml, &[1, 2]);
        let first = next_row(&mut source).await.position();
        let second = next_row(&mut source).await.position();
        let commit = next_commit(&mut source).await;
        let base = confirmed_flush_lsn(&mut probe, &slot_inner).expect("the slot exists");
        drop(source);

        let mut source = connect_when_free(&config(Some(first))).await;
        assert_eq!(
            next_row(&mut source).await.position(),
            second,
            "the resume starts at the rest of the unacknowledged transaction"
        );
        assert_eq!(
            next_commit(&mut source).await,
            commit,
            "still unacknowledged"
        );

        // Let several feedback intervals elapse. An automatic release of the
        // resume position would have reached the server by now.
        drive_idle(&mut source, Duration::from_secs(1)).await;
        assert_eq!(
            confirmed_flush_lsn(&mut probe, &slot_inner),
            Some(base),
            "resuming from an unacknowledged row must not release WAL on its own"
        );

        source.ack(commit.position()).await.expect("ack the commit");
        let flushed = flushed_after_ack(
            &mut source,
            |lsn| lsn >= commit.end_lsn(),
            &mut probe,
            &slot_inner,
        )
        .await;
        assert_eq!(flushed, commit.end_lsn());
    });

    common::drop_slot(&mut setup, &slot);
}

/// A read cancelled by its deadline keeps the WAL progress it saw, and the
/// reader still delivers normally afterwards.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_cancelled_read_retains_wal_progress_above_the_slot_seed() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "3s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    // Created before the slot and carried by no publication.
    sql_query(SCRATCH_DDL)
        .execute(&mut setup)
        .expect("create scratch table");
    let slot = db.slot("subql_pg_streaming_retention_seed");
    let publication = "subql_pg_streaming_retention_seed_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        assert_eq!(
            source.unacknowledged_bytes(),
            0,
            "the retention gauges start at the slot's own seed"
        );

        sql_query("INSERT INTO scratch VALUES (1, 'unpublished')")
            .execute(&mut dml)
            .expect("insert unpublished data");

        // Drive reads that have no item to deliver. Each bounded timeout
        // abandons the pending read, so the progress it saw must survive.
        let deadline = Instant::now() + ARRIVAL;
        while source.unacknowledged_bytes() == 0 {
            assert!(
                Instant::now() < deadline,
                "the unpublished WAL never reached the reader"
            );
            let _ = tokio::time::timeout(Duration::from_millis(250), source.next_item()).await;
        }
        assert!(
            source.unacknowledged_bytes() > 0,
            "a cancelled read must keep the WAL progress it saw"
        );

        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert a published row");
        let row = next_row(&mut source).await;
        let commit = next_commit(&mut source).await;
        assert!(
            commit.end_lsn() > row.position().commit_lsn(),
            "delivery survives the abandoned read"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Idle through an impatient server's `wal_sender_timeout` while the reader
/// is polled, and confirm a freshly issued row still delivers.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn connection_survives_wal_sender_timeout() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "3s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_survive");
    let publication = "subql_pg_streaming_survive_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(500));

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // The pull reader only stays alive while it is polled, so poll it
        // across the whole idle window and issue the insert from a
        // parallel branch.
        let (relation, ()) =
            tokio::join!(tokio::time::timeout(ARRIVAL, source.next_item()), async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                sql_query("INSERT INTO orders VALUES (1, 5.0)")
                    .execute(&mut dml)
                    .expect("insert after idle");
            },);
        relation
            .expect("next_item timeout: connection likely torn down")
            .expect("next_item err: connection likely torn down")
            .and_then(SourceItem::into_relation)
            .expect("the fresh slot states its table first");
        let ev = tokio::time::timeout(ARRIVAL, source.next_item())
            .await
            .expect("next_item timeout: connection likely torn down")
            .expect("next_item err: connection likely torn down")
            .and_then(SourceItem::into_event)
            .expect("the row arrives before its commit");
        assert_eq!(ev.kind(), EventKind::Insert);
    });

    common::drop_slot(&mut setup, &slot);
}

/// A slow consumer that pauses between reads still receives every row and
/// every commit, each exactly once, in commit order.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_slow_consumer_still_receives_every_item_in_commit_order() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_slow_consumer");
    let publication = "subql_pg_streaming_slow_consumer_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(500));

    const N: i32 = 100;

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // One transaction per row, all committed before the first read.
        for id in 1..=N {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }

        let schema = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        for id in 1..=N {
            // A deliberate pause between reads, so the consumer lags the wire.
            tokio::time::sleep(Duration::from_millis(10)).await;
            let row = next_row(&mut source).await;
            let row_id = match row
                .value_at(&schema, subql::backend::RowKind::New, 0)
                .unwrap()
            {
                subql::backend::Value::Int(v) => v,
                other => panic!("expected int id, got {other:?}"),
            };
            assert_eq!(
                row_id,
                i64::from(id),
                "the slow consumer must miss nothing and reorder nothing"
            );
            next_commit(&mut source).await;
        }
    });

    common::drop_slot(&mut setup, &slot);
}

/// Dropping the source closes its replication connection, frees the slot's
/// backend, and leaves the slot reusable.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn drop_source_closes_the_connection_and_frees_the_slot() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_shutdown");
    let publication = "subql_pg_streaming_shutdown_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let slot_inner = slot.clone();
    assert!(
        !slot_is_active(&mut probe, &slot_inner),
        "the slot starts free"
    );

    let mut probe = current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        let deadline = Instant::now() + ARRIVAL;
        while !slot_is_active(&mut probe, &slot_inner) {
            assert!(
                Instant::now() < deadline,
                "the slot never became active while the source held it"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        drop(source);
        probe
    });

    let deadline = Instant::now() + ARRIVAL;
    while slot_is_active(&mut probe, &slot) {
        assert!(
            Instant::now() < deadline,
            "the dropped source still holds the slot"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1]);
        let row = next_row(&mut source).await;
        assert_eq!(row.kind(), EventKind::Insert);
        next_commit(&mut source).await;
    });

    common::drop_slot(&mut setup, &slot);
}

/// `events_received` counts the row events the source delivers, and commits
/// do not count.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn events_received_counter_tracks_pushed_events() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_events_counter");
    let publication = "subql_pg_streaming_events_counter_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        assert_eq!(
            source.events_received(),
            0,
            "counter starts at 0 before any event has been delivered"
        );

        const N: i32 = 5;
        for id in 1..=N {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }
        for _ in 0..N {
            assert_eq!(next_row(&mut source).await.kind(), EventKind::Insert);
            next_commit(&mut source).await;
        }

        let observed = source.events_received();
        assert_eq!(
            observed,
            u64::try_from(N).unwrap(),
            "counter must equal the number of delivered row events"
        );
        println!("events_received after {N} inserts: {observed}");
    });

    common::drop_slot(&mut setup, &slot);
}

/// Terminating the slot's backend makes the source terminal, with no
/// automatic reconnect.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_killed_backend_makes_the_source_terminal() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_killed");
    let publication = "subql_pg_streaming_killed_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let slot_inner = slot.clone();

    let mut setup = current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        insert(&mut setup, &[1]);
        next_row(&mut source).await;
        let commit = next_commit(&mut source).await;

        // Only this slot's backend, named by the slot rather than by a
        // sweep, so no other test's connection is touched.
        sql_query(format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{slot_inner}' AND active_pid IS NOT NULL"
        ))
        .execute(&mut setup)
        .expect("terminate the slot's backend");

        tokio::time::timeout(ARRIVAL, source.next_item())
            .await
            .expect("the reader reports the death promptly")
            .expect_err("next_item must report the dead connection");
        let error = tokio::time::timeout(ARRIVAL, source.next_item())
            .await
            .expect("the terminal source keeps responding")
            .expect_err("no automatic reconnect");
        assert!(matches!(error, PgStreamingError::SourceClosed), "{error:?}");
        let error = tokio::time::timeout(ARRIVAL, source.ack(commit.position()))
            .await
            .expect("the terminal source refuses acknowledgements promptly")
            .expect_err("a dead source must not accept an acknowledgement");
        assert!(matches!(error, PgStreamingError::SourceClosed), "{error:?}");
        assert!(
            !slot_is_active(&mut probe, &slot_inner),
            "the server never saw a replacement connection"
        );
        setup
    });

    common::drop_slot(&mut setup, &slot);
}

/// An acknowledgement whose feedback the dead connection refuses makes the
/// source terminal.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_acknowledgement_the_connection_cannot_send_makes_the_source_terminal() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_dead_ack");
    let publication = "subql_pg_streaming_dead_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    // Closing the router's sockets resets the client side, which the
    // server's own proxy would only half-close.
    let router = EndpointRouter::start(
        loopback_port(&db.url()),
        loopback_port(&db.url()),
        Duration::ZERO,
        Instant::now() + Duration::from_secs(120),
        None,
    );
    let config = PgStreamingConfig::new(router.url(db.name()), &slot, publication)
        .status_interval(Duration::from_secs(3600));
    let slot_inner = slot.clone();

    let mut setup = current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        let mut commits = Vec::new();
        for id in 1..=3 {
            insert(&mut setup, &[id]);
            next_row(&mut source).await;
            commits.push(next_commit(&mut source).await);
        }

        router.finish();

        // The first write to the closed socket can still be buffered, and
        // the reset it provokes refuses the next one.
        let mut refused = None;
        for commit in &commits {
            match tokio::time::timeout(ARRIVAL, source.ack(commit.position()))
                .await
                .expect("the acknowledgement responds")
            {
                Ok(()) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(error) => {
                    refused = Some(error);
                    break;
                }
            }
        }
        let error = refused.expect("feedback to a closed connection fails");
        assert!(
            !matches!(error, PgStreamingError::SourceClosed),
            "{error:?}"
        );
        let last = commits.last().expect("three commits").position();
        let error = tokio::time::timeout(ARRIVAL, source.ack(last))
            .await
            .expect("the terminal source refuses acknowledgements promptly")
            .expect_err("a source whose feedback failed accepts no acknowledgement");
        assert!(matches!(error, PgStreamingError::SourceClosed), "{error:?}");
        let error = tokio::time::timeout(ARRIVAL, source.next_item())
            .await
            .expect("the terminal source keeps responding")
            .expect_err("no automatic reconnect");
        assert!(matches!(error, PgStreamingError::SourceClosed), "{error:?}");
        let deadline = Instant::now() + ARRIVAL;
        while slot_is_active(&mut probe, &slot_inner) {
            assert!(Instant::now() < deadline, "the walsender never exited");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        setup
    });

    common::drop_slot(&mut setup, &slot);
}

/// Reconnecting at a saved resume position after the slot was dropped must
/// fail with a typed slot error rather than create a replacement slot.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn reopening_a_source_whose_slot_was_dropped_fails_typed() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_slot_dropped");
    let publication = "subql_pg_streaming_slot_dropped_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    let slot_inner = slot.clone();

    let resume = current_thread_rt().block_on(async {
        // A first, valid connect, so the consumer holds a resume position.
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1]);
        let position = next_row(&mut source).await.position();
        next_commit(&mut source).await;
        position
    });

    // The operator drops the slot and the database moves on.
    common::drop_slot(&mut setup, &slot_inner);
    insert(&mut setup, &[2]);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let err = current_thread_rt().block_on(async move {
        match PgStreamingCdcSource::connect(config.start(Some(resume)), catalog).await {
            Ok(_) => panic!("a dropped slot must refuse the reconnect"),
            Err(error) => error,
        }
    });
    assert!(
        matches!(
            err,
            PgStreamingError::Postgres(ReplicationError::ReplicationSlot(_))
        ),
        "the missing slot must surface as a typed slot error, got {err:?}",
    );

    assert!(
        confirmed_flush_lsn(&mut probe, &slot).is_none(),
        "the reader must not have created a replacement slot"
    );
}

#[derive(diesel::QueryableByName)]
struct ControlSystem {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    system_identifier: i64,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    timeline: i32,
}

/// The cluster's identity as plain SQL reports it, outside the replication protocol.
fn control_system(conn: &mut diesel::PgConnection) -> (u64, u32) {
    // The DSL cannot put a set-returning function in FROM.
    let row = sql_query(
        "SELECT s.system_identifier, c.timeline_id AS timeline \
         FROM pg_control_system() s, pg_control_checkpoint() c",
    )
    .get_result::<ControlSystem>(conn)
    .expect("pg_control_system");
    (
        u64::try_from(row.system_identifier).expect("system id positive"),
        u32::try_from(row.timeline).expect("timeline positive"),
    )
}

/// Parse a raw history file without the code under test.
fn raw_history_switches(content: &str) -> Vec<TimelineSwitch> {
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut fields = line.split('\t');
            TimelineSwitch {
                timeline: fields.next().unwrap().trim().parse().unwrap(),
                switch_lsn: PgLsn::parse(fields.next().unwrap().trim()).unwrap(),
            }
        })
        .collect()
}

fn docker(args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("docker {}: {e}", args.join(" ")));
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

fn docker_ok(args: &[&str]) -> String {
    docker(args).unwrap_or_else(|err| panic!("{err}"))
}

fn wait_until(what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + ARRIVAL;
    while !probe() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {ARRIVAL:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Drop the physical slot a backup reserved, ending the walsender still holding
/// it if waiting does not free it.
///
/// A physical slot is cluster-wide, so one left behind retains WAL for every
/// test sharing the server. Function calls the DSL cannot express.
fn drop_backup_slot(source: &mut PgConnection, slot: &str) -> Result<(), String> {
    let started = Instant::now();
    loop {
        // Selecting from the view drops the slot only if it is there, so a
        // success means it is gone either way.
        let dropped = sql_query(format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
             WHERE slot_name = '{slot}'"
        ))
        .execute(source);
        let Err(err) = dropped else {
            return Ok(());
        };
        if started.elapsed() > ARRIVAL {
            return Err(format!(
                "physical slot {slot} is still on the shared server, retaining its WAL: {err}"
            ));
        }
        if started.elapsed() > Duration::from_secs(5) {
            let _ = sql_query(format!(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = '{slot}' AND active_pid IS NOT NULL"
            ))
            .execute(source);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// A promoted point-in-time clone of the shared server.
struct PitrCluster {
    container: String,
    volume: String,
    port: u16,
}

impl PitrCluster {
    /// Clone the shared server with `pg_basebackup`, recover to the backup's consistency point and promote to timeline 2.
    fn promoted(name: &str, source_container: &str, source: &mut PgConnection) -> Self {
        let mut clone = Self {
            container: format!("subql-pitr-{name}"),
            volume: format!("subql-pitr-{name}-vol"),
            port: 0,
        };
        let image = common::pg_image_ref();
        docker_ok(&["volume", "create", &clone.volume]);
        // Reserved before the backup starts, so the shared server cannot recycle
        // WAL the backup has yet to stream. Function calls the DSL cannot express.
        let slot = format!("subql_pitr_{name}");
        sql_query(format!(
            "SELECT pg_create_physical_replication_slot('{slot}', true)"
        ))
        .execute(source)
        .expect("reserve WAL for the backup");
        // Postgres refuses an immediate target without a restore_command, and consistency never consults it.
        let seed = format!(
            r#"pg_basebackup -h 127.0.0.1 -U subql_test -D /dst -X stream -S {slot} \
               && printf "recovery_target = 'immediate'\nrestore_command = 'false'\n" >> /dst/postgresql.auto.conf \
               && touch /dst/recovery.signal"#
        );
        let seeded = docker(&[
            "run",
            "--rm",
            "--network",
            &format!("container:{source_container}"),
            "-v",
            &format!("{}:/dst", clone.volume),
            "-e",
            "PGPASSWORD=subql_test",
            &image,
            "sh",
            "-c",
            &seed,
        ]);
        // The walsender can still hold the slot for a moment after the backup exits.
        match (seeded, drop_backup_slot(source, &slot)) {
            (Ok(_), Ok(())) => {}
            (Ok(_), Err(slot_err)) => panic!("{slot_err}"),
            (Err(backup_err), Ok(())) => panic!("{backup_err}"),
            (Err(backup_err), Err(slot_err)) => panic!("{backup_err}\n{slot_err}"),
        }
        let data = format!("{}:/var/lib/postgresql/data", clone.volume);
        let mut run = vec![
            "run",
            "-d",
            "--name",
            &clone.container,
            "-v",
            &data,
            "-p",
            "127.0.0.1:0:5432",
            "-e",
            "POSTGRES_USER=subql_test",
            "-e",
            "POSTGRES_PASSWORD=subql_test",
            &image,
        ];
        run.extend(common::pg::PG_COMMAND);
        docker_ok(&run);
        // PG16 pauses at the recovery target, and resuming promotes.
        let in_recovery = |want: &str| {
            let out = std::process::Command::new("docker")
                .args([
                    "exec",
                    &clone.container,
                    "psql",
                    "user=subql_test dbname=postgres",
                    "-tAc",
                    "SELECT pg_is_in_recovery()",
                ])
                .output()
                .expect("docker runs");
            out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == want
        };
        wait_until("clone reached the paused recovery point", || {
            in_recovery("t")
        });
        docker_ok(&[
            "exec",
            &clone.container,
            "psql",
            "user=subql_test dbname=postgres",
            "-c",
            "SELECT pg_wal_replay_resume()",
        ]);
        wait_until("clone promoted", || in_recovery("f"));
        let mapped = docker_ok(&["port", &clone.container, "5432/tcp"]);
        clone.port = mapped
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .and_then(|p| p.parse().ok())
            .expect("mapped port");
        clone
    }

    /// libpq URL of the test database on the clone.
    fn url(&self, database: &str) -> String {
        common::pg::PgDatabase::url_at(self.port, database)
    }

    /// The clone's raw `00000002.history`, read off its disk.
    fn raw_history(&self) -> String {
        docker_ok(&[
            "exec",
            &self.container,
            "sh",
            "-c",
            "find /var/lib/postgresql/data/pg_wal -name 00000002.history -exec cat {} +",
        ])
    }

    /// Delete the clone's `00000002.history` from its disk.
    fn remove_history(&self) {
        docker_ok(&[
            "exec",
            &self.container,
            "sh",
            "-c",
            "find /var/lib/postgresql/data/pg_wal -name 00000002.history -delete",
        ]);
    }
}

impl Drop for PitrCluster {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
        let _ = std::process::Command::new("docker")
            .args(["volume", "rm", &self.volume])
            .output();
    }
}

/// `cluster_identity` matches `pg_control_system` on timeline 1, with no history.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn cluster_identity_of_the_running_cluster() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_identity");
    let publication = "subql_pg_streaming_identity_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let (system_id, timeline) = control_system(&mut setup);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        let identity = source.cluster_identity();
        assert_eq!(identity.system_id, system_id);
        assert_eq!(identity.timeline, timeline);
        assert_eq!(identity.history, Vec::<TimelineSwitch>::new());
    });

    common::drop_slot(&mut setup, &slot);
}

/// A promoted point-in-time clone reports timeline 2 and its own history file's switch point,
/// and refuses to start once that history is gone.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn cluster_identity_of_a_promoted_point_in_time_clone() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    // Recovery discards restored logical slots, so only the publication is created before the clone.
    let publication = "subql_pg_streaming_pitr_pub";
    common::create_publication(&mut setup, publication, "orders");

    let clone = PitrCluster::promoted(db.name(), db.container(), &mut setup);
    let mut on_clone =
        diesel::PgConnection::establish(&clone.url(db.name())).expect("connect to clone");
    let slot = db.slot("subql_pg_streaming_pitr");
    common::create_pgoutput_slot(&mut on_clone, &slot);
    let (system_id, _) = control_system(&mut on_clone);
    let expected = raw_history_switches(&clone.raw_history());
    assert!(!expected.is_empty(), "history file names the branch");

    let connect = |config: PgStreamingConfig| {
        let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        current_thread_rt().block_on(PgStreamingCdcSource::connect(config, catalog))
    };
    let config = PgStreamingConfig::new(clone.url(db.name()), &slot, publication);
    let source = connect(config.clone()).expect("connect to promoted clone");
    let identity = source.cluster_identity();
    assert_eq!(identity.timeline, 2);
    assert_eq!(identity.history, expected);
    // Promotion keeps the system id, which is why the timeline is needed.
    assert_eq!(identity.system_id, system_id);
    drop(source);

    clone.remove_history();
    let deadline = Instant::now() + ARRIVAL;
    let error = loop {
        match connect(config.clone()) {
            Err(error) if error.to_string().contains("is active") && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => break error,
            Ok(_) => panic!("a clone without its timeline history must not report an identity"),
        }
    };
    assert!(
        !matches!(error, PgStreamingError::SourceClosed),
        "{error:?}"
    );
}

/// A physical slot that bears the configured name is refused rather than streamed.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_physical_slot_of_the_configured_name_is_refused() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_physical");
    let publication = "subql_pg_streaming_physical_pub";
    common::create_publication(&mut setup, publication, "orders");
    // Replication administration, which the query DSL does not express.
    sql_query(format!(
        "SELECT pg_create_physical_replication_slot('{slot}')"
    ))
    .execute(&mut setup)
    .expect("create physical slot");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    let result = current_thread_rt().block_on(PgStreamingCdcSource::connect(config, catalog));
    assert!(
        result.is_err(),
        "a physical slot cannot feed logical decoding"
    );
    common::drop_slot(&mut setup, &slot);
}

struct RouterState {
    stopped: bool,
    sockets: Vec<TcpStream>,
}

impl RouterState {
    fn stop(&mut self) {
        self.stopped = true;
        for socket in self.sockets.drain(..) {
            let _ = socket.shutdown(SocketShutdown::Both);
        }
    }
}

struct EndpointRouter {
    stalls: Arc<AtomicU64>,
    state: Arc<Mutex<RouterState>>,
    port: u16,
    accept_thread: Option<std::thread::JoinHandle<()>>,
}

const ROUTER_ACCEPT_POLL: Duration = Duration::from_millis(5);
const ROUTER_JOIN_BUDGET: Duration = Duration::from_secs(30);

fn join_bounded(handle: std::thread::JoinHandle<()>, deadline: Instant) -> bool {
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(ROUTER_ACCEPT_POLL);
    }
    handle.join().is_ok()
}

fn loopback_port(url: &str) -> u16 {
    let host_port = url.rsplit_once('@').expect("fixture host").1;
    let port = host_port.rsplit_once(':').expect("fixture port").1;
    port.split('/')
        .next()
        .expect("fixture port")
        .parse()
        .expect("fixture port")
}

impl EndpointRouter {
    fn start(
        first: u16,
        later: u16,
        delay: Duration,
        deadline: Instant,
        heartbeat: Option<Arc<AtomicU64>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("router listener");
        let port = listener.local_addr().expect("router address").port();
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let state = Arc::new(Mutex::new(RouterState {
            stopped: false,
            sockets: Vec::new(),
        }));
        let stalls = Arc::new(AtomicU64::new(0));
        let accept_state = Arc::clone(&state);
        let accept_stalls = Arc::clone(&stalls);
        let accept_thread = std::thread::spawn(move || {
            let mut handlers = Vec::new();
            let mut initial = true;
            while Instant::now() < deadline {
                if accept_state.lock().stopped {
                    break;
                }
                let client = match listener.accept() {
                    Ok((client, _)) => client,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(ROUTER_ACCEPT_POLL);
                        continue;
                    }
                    Err(error) => panic!("router accept failed {error}"),
                };
                {
                    let mut gate = accept_state.lock();
                    if gate.stopped {
                        let _ = client.shutdown(SocketShutdown::Both);
                        break;
                    }
                    gate.sockets.push(client.try_clone().expect("track client"));
                }
                let target = if initial { first } else { later };
                initial = false;
                let state = Arc::clone(&accept_state);
                let stalls = Arc::clone(&accept_stalls);
                let heartbeat = heartbeat.clone();
                handlers.push(std::thread::spawn(move || {
                    router_handler(client, target, delay, deadline, heartbeat, &stalls, &state);
                }));
            }
            accept_state.lock().stop();
            let join_deadline = Instant::now() + ROUTER_JOIN_BUDGET;
            for handler in handlers {
                assert!(
                    join_bounded(handler, join_deadline),
                    "router handler failed"
                );
            }
        });
        Self {
            stalls,
            state,
            port,
            accept_thread: Some(accept_thread),
        }
    }

    fn url(&self, database: &str) -> String {
        common::PgDatabase::url_at(self.port, database)
    }

    fn stalled_handshakes(&self) -> u64 {
        self.stalls.load(Ordering::Relaxed)
    }

    fn shutdown(&mut self) -> bool {
        self.state.lock().stop();
        self.accept_thread.take().is_none_or(|thread| {
            let deadline = Instant::now()
                .checked_add(ROUTER_JOIN_BUDGET)
                .unwrap_or_else(Instant::now);
            join_bounded(thread, deadline)
        })
    }

    fn finish(mut self) {
        assert!(self.shutdown(), "router cleanup failed");
    }
}

fn router_handler(
    mut client: TcpStream,
    port: u16,
    delay: Duration,
    deadline: Instant,
    heartbeat: Option<Arc<AtomicU64>>,
    stalls: &AtomicU64,
    state: &Mutex<RouterState>,
) {
    if !delay.is_zero() {
        let heartbeat = heartbeat.expect("handshake heartbeat");
        let before = heartbeat.load(Ordering::Relaxed);
        std::thread::sleep(delay.min(deadline.saturating_duration_since(Instant::now())));
        if heartbeat.load(Ordering::Relaxed) == before {
            stalls.fetch_add(1, Ordering::Relaxed);
        }
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() || state.lock().stopped {
        let _ = client.shutdown(SocketShutdown::Both);
        return;
    }
    let mut target = TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        remaining.min(Duration::from_secs(10)),
    )
    .expect("router target");
    {
        let mut gate = state.lock();
        if gate.stopped {
            let _ = client.shutdown(SocketShutdown::Both);
            let _ = target.shutdown(SocketShutdown::Both);
            return;
        }
        gate.sockets.push(target.try_clone().expect("track target"));
    }
    let remaining = deadline
        .saturating_duration_since(Instant::now())
        .max(Duration::from_millis(1));
    for socket in [&client, &target] {
        socket
            .set_read_timeout(Some(remaining))
            .expect("bounded reads");
        socket
            .set_write_timeout(Some(remaining))
            .expect("bounded writes");
    }
    let mut client_read = client.try_clone().expect("client relay");
    let mut target_write = target.try_clone().expect("target relay");
    let upstream = std::thread::spawn(move || {
        let _ = std::io::copy(&mut client_read, &mut target_write);
        let _ = target_write.shutdown(SocketShutdown::Write);
    });
    let _ = std::io::copy(&mut target, &mut client);
    let _ = client.shutdown(SocketShutdown::Both);
    let _ = target.shutdown(SocketShutdown::Both);
    assert!(
        join_bounded(upstream, Instant::now() + ROUTER_JOIN_BUDGET),
        "router relay failed"
    );
}

impl Drop for EndpointRouter {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn inserted_id(event: &PgChangeEvent) -> i32 {
    match &event.change().event_type {
        EventType::Insert { data, .. } => data
            .get("id")
            .and_then(ColumnValue::as_str)
            .and_then(|text| text.parse().ok())
            .expect("pending row id"),
        other => panic!("expected an insert, got {other:?}"),
    }
}

/// Metadata and unacknowledged feedback belong to the endpoint delivering the row.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_foreign_baseline_cannot_advance_a_foreign_slot() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_foreign_seed");
    let publication = "subql_pg_streaming_foreign_seed_pub";
    // Recovery discards restored logical slots.
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let clone = PitrCluster::promoted(db.name(), db.container(), &mut setup);
    let mut on_clone =
        diesel::PgConnection::establish(&clone.url(db.name())).expect("connect to the clone");
    common::create_pgoutput_slot(&mut on_clone, &slot);
    let clone_baseline = confirmed_flush_lsn(&mut on_clone, &slot).expect("the clone slot exists");

    // A foreign seed must exceed the clone's retained position.
    insert(&mut setup, &[1]);
    let advance = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));
    let mut probe = db.connect();
    let slot_name = slot.clone();
    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&advance).await;
        next_row(&mut source).await;
        let commit = next_commit(&mut source).await;
        source
            .ack(commit.position())
            .await
            .expect("ack the advance commit");
        flushed_after_ack(
            &mut source,
            |lsn| lsn >= commit.end_lsn(),
            &mut probe,
            &slot_name,
        )
        .await;
    });
    let primary_baseline = confirmed_flush_lsn(&mut setup, &slot).expect("the primary slot exists");
    assert!(
        primary_baseline > clone_baseline,
        "the primary must lead the clone for a foreign seed to be observable: \
         {primary_baseline:?} against {clone_baseline:?}"
    );

    insert(&mut setup, &[100]);
    insert(&mut on_clone, &[200]);

    let router = EndpointRouter::start(
        loopback_port(&db.url()),
        clone.port,
        Duration::ZERO,
        Instant::now() + Duration::from_secs(120),
        None,
    );
    let config = PgStreamingConfig::new(router.url(db.name()), &slot, publication)
        .status_interval(Duration::from_millis(100));

    current_thread_rt().block_on(async {
        let mut source = connect_when_free(&config).await;
        let streamed_the_clone = match inserted_id(&next_row(&mut source).await) {
            100 => false,
            200 => true,
            id => panic!("unexpected endpoint marker {id}"),
        };
        let (system_id, timeline) = if streamed_the_clone {
            control_system(&mut on_clone)
        } else {
            control_system(&mut setup)
        };
        let expected_history = if streamed_the_clone {
            raw_history_switches(&clone.raw_history())
        } else {
            Vec::new()
        };
        let identity = source.cluster_identity();
        assert_eq!(
            identity.system_id, system_id,
            "the identity names the streamed cluster"
        );
        assert_eq!(
            identity.timeline, timeline,
            "the identity names the streamed timeline"
        );
        assert_eq!(
            identity.history, expected_history,
            "the identity carries the streamed history"
        );

        next_commit(&mut source).await;
        drive_idle(&mut source, Duration::from_secs(2)).await;
    });

    router.finish();
    assert_eq!(
        confirmed_flush_lsn(&mut setup, &slot).expect("the primary slot exists"),
        primary_baseline,
        "the primary slot must hold its baseline without an acknowledgement"
    );
    assert_eq!(
        confirmed_flush_lsn(&mut on_clone, &slot).expect("the clone slot exists"),
        clone_baseline,
        "the clone slot must hold its baseline without a foreign seed"
    );
    common::drop_slot(&mut setup, &slot);
}

/// Each delayed startup handshake leaves the current-thread heartbeat running.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn startup_never_blocks_the_current_thread_runtime() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "1s");
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_startup_block");
    let publication = "subql_pg_streaming_startup_block_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    insert(&mut setup, &[1]);

    const HANDSHAKE_DELAY: Duration = Duration::from_millis(300);
    let heartbeat = Arc::new(AtomicU64::new(0));
    let target = loopback_port(&db.url());
    let router = EndpointRouter::start(
        target,
        target,
        HANDSHAKE_DELAY,
        Instant::now() + Duration::from_secs(60),
        Some(Arc::clone(&heartbeat)),
    );
    let config = PgStreamingConfig::new(router.url(db.name()), &slot, publication)
        .status_interval(Duration::from_millis(100));

    current_thread_rt().block_on(async {
        let ticks = Arc::clone(&heartbeat);
        tokio::task::spawn(async move {
            let deadline = Instant::now() + ARRIVAL;
            while Instant::now() < deadline {
                ticks.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let mut source = connect_when_free(&config).await;
        let (system_id, timeline) = control_system(&mut setup);
        let identity = source.cluster_identity();
        assert_eq!(identity.system_id, system_id);
        assert_eq!(identity.timeline, timeline);
        next_row(&mut source).await;
        next_commit(&mut source).await;
    });

    assert!(
        heartbeat.load(Ordering::Relaxed) > 0,
        "the heartbeat must have run while the source connected"
    );
    assert_eq!(
        router.stalled_handshakes(),
        0,
        "a startup handshake blocked the current-thread runtime"
    );
    router.finish();
    common::drop_slot(&mut setup, &slot);
}
