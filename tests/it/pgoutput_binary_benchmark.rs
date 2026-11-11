//! Text against binary pgoutput on the push source.
//!
//! One server, one slot, protocol 1 and the same connection settings for
//! both formats, so only the format changes. Each run replays the slot from
//! its start, since nothing acknowledges, decodes every cell and dispatches
//! every event to one subscription per column.
//!
//! ```sh
//! SUBQL_BINARY_BENCH_ROWS=200000 cargo nextest run --cargo-profile testfast \
//!     --all-features --test it --run-ignored all -E 'test(/pgoutput_binary_benchmark/)' \
//!     --no-capture
//! ```

// Rates are printed as `f64`, where losing low bits of a count is harmless.
#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::cast_precision_loss)]

use crate::common;

use std::process::Command;
use std::time::{Duration, Instant};

use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind, Value};
use subql::{
    CdcSource, DefaultIds, PgChangeEvent, PgStreamingCdcSource, PgStreamingConfig,
    SubscriptionEngine, SubscriptionRequest,
};

const TABLE: &str = "CREATE TABLE mix (
    id BIGINT PRIMARY KEY, at TIMESTAMPTZ, amount NUMERIC(12, 3), uid UUID, body TEXT
)";

const FILTERS: &[&str] = &[
    "SELECT * FROM mix WHERE id > 0",
    "SELECT * FROM mix WHERE at > '2020-01-01 00:00:00+00'",
    "SELECT * FROM mix WHERE amount > 1",
    "SELECT * FROM mix WHERE uid IS NOT NULL",
    "SELECT * FROM mix WHERE body LIKE 'x%'",
];

const BATCH: u64 = 1_000;
const RUNS: usize = 3;

#[derive(QueryableByName)]
struct Pid {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(QueryableByName)]
struct Setting {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

fn docker_cat(container: &str, path: &str) -> String {
    let output = Command::new("docker")
        .args(["exec", container, "cat", path])
        .output()
        .expect("docker exec");
    String::from_utf8(output.stdout).expect("utf-8 proc file")
}

/// `utime + stime` in clock ticks from a `/proc/<pid>/stat` line.
fn cpu_ticks(stat: &str) -> u64 {
    // The command name may hold spaces, so fields count from its closing parenthesis.
    let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..].split(' ').collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

/// Bytes the container's `eth0` has transmitted.
fn tx_bytes(container: &str) -> u64 {
    let dev = docker_cat(container, "/proc/net/dev");
    let line = dev
        .lines()
        .find(|line| line.trim_start().starts_with("eth0:"))
        .expect("eth0 in /proc/net/dev");
    line.split_once(':')
        .unwrap()
        .1
        .split_whitespace()
        .nth(8)
        .unwrap()
        .parse()
        .unwrap()
}

struct Run {
    events: u64,
    wall: Duration,
    client_ticks: u64,
    walsender_ticks: u64,
    tx: u64,
    reexecutions: u64,
}

fn measure(
    db: &common::PgDatabase,
    conn: &mut diesel::PgConnection,
    slot: &str,
    publication: &str,
    catalog: &ParserDB,
    rows: u64,
    binary: bool,
) -> Run {
    let mut engine: SubscriptionEngine<PgChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});
    for (consumer, filter) in (1u64..).zip(FILTERS) {
        engine
            .register(SubscriptionRequest::new(consumer, *filter))
            .expect("filter registers");
    }
    let config = PgStreamingConfig::new(db.url(), slot, publication)
        .status_interval(Duration::from_secs(1))
        .binary(binary);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut source = PgStreamingCdcSource::connect(config, catalog.clone())
            .await
            .expect("connect");
        let walsender =
            sql_query("SELECT active_pid AS pid FROM pg_replication_slots WHERE slot_name = $1")
                .bind::<diesel::sql_types::Text, _>(slot)
                .get_result::<Pid>(conn)
                .expect("the slot is active")
                .pid;
        let walsender_stat = format!("/proc/{walsender}/stat");
        let walsender_before = cpu_ticks(&docker_cat(db.container(), &walsender_stat));
        let tx_before = tx_bytes(db.container());
        let client_before = cpu_ticks(&std::fs::read_to_string("/proc/self/stat").unwrap());
        let start = Instant::now();

        let (mut events, mut reexecutions) = (0u64, 0u64);
        while events < rows {
            let item = tokio::time::timeout(Duration::from_mins(1), source.next_item())
                .await
                .expect("an item within the timeout")
                .expect("the source reads")
                .expect("the source is open");
            let Some(event) = item.into_event() else {
                continue;
            };
            for col in 0..5 {
                if event
                    .value_at(catalog, RowKind::New, col)
                    .expect("the cell decodes")
                    == Value::Missing
                {
                    reexecutions += 1;
                }
            }
            let notified = engine.consumers(&event).expect("dispatch");
            reexecutions += u64::try_from(notified.unanswered().len()).unwrap();
            events += 1;
        }

        let wall = start.elapsed();
        let client_ticks =
            cpu_ticks(&std::fs::read_to_string("/proc/self/stat").unwrap()) - client_before;
        let walsender_ticks =
            cpu_ticks(&docker_cat(db.container(), &walsender_stat)) - walsender_before;
        let tx = tx_bytes(db.container()) - tx_before;
        Run {
            events,
            wall,
            client_ticks,
            walsender_ticks,
            tx,
            reexecutions,
        }
    })
}

fn setting(conn: &mut diesel::PgConnection, sql: &str) -> String {
    sql_query(sql).get_result::<Setting>(conn).unwrap().value
}

#[test]
#[ignore = "measurement, requires Docker; run with --ignored"]
fn pgoutput_binary_benchmark() {
    common::assert_docker_available();
    let rows: u64 = std::env::var("SUBQL_BINARY_BENCH_ROWS")
        .ok()
        .map_or(100_000, |rows| rows.parse().unwrap());
    let db = common::pg_database();
    let mut conn = db.connect();
    sql_query(TABLE).execute(&mut conn).expect("create table");
    let slot = db.slot("binary_bench");
    let publication = "binary_bench_pub";
    common::create_publication(&mut conn, publication, "mix");
    common::create_pgoutput_slot(&mut conn, &slot);
    for first in (0..rows).step_by(usize::try_from(BATCH).unwrap()) {
        // A generated batch, which the query DSL does not express.
        sql_query(format!(
            "INSERT INTO mix SELECT g, TIMESTAMPTZ '2024-03-15 12:00:00+00' + g * INTERVAL '1 second', \
             g * 1.125, gen_random_uuid(), repeat('x', 32) || g \
             FROM generate_series({}, {}) g",
            first + 1,
            (first + BATCH).min(rows)
        ))
        .execute(&mut conn)
        .expect("load batch");
    }
    let catalog = ParserDB::parse::<PostgreSqlDialect>(&format!("{TABLE};")).unwrap();

    println!(
        "server {} ssl {} protocol 1, {rows} rows inserted in transactions of {BATCH}",
        setting(
            &mut conn,
            "SELECT current_setting('server_version') AS value"
        ),
        setting(&mut conn, "SELECT current_setting('ssl') AS value"),
    );
    println!("format run events/s client_us/event walsender_us/event bytes/event reexecutions");
    // Clock ticks are 10 ms on Linux.
    let per_event_us = |ticks: u64, events: u64| ticks as f64 * 10_000.0 / events as f64;
    for run in 0..RUNS {
        for binary in [false, true] {
            let measured = measure(&db, &mut conn, &slot, publication, &catalog, rows, binary);
            println!(
                "{} {run} {:.0} {:.2} {:.2} {:.1} {}",
                if binary { "binary" } else { "text" },
                measured.events as f64 / measured.wall.as_secs_f64(),
                per_event_us(measured.client_ticks, measured.events),
                per_event_us(measured.walsender_ticks, measured.events),
                measured.tx as f64 / measured.events as f64,
                measured.reexecutions,
            );
        }
    }
    common::drop_slot(&mut conn, &slot);
}
