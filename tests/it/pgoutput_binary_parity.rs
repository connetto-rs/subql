//! A row delivered as binary pgoutput cells reads, dispatches and escalates
//! exactly as the same row delivered as text.
//!
//! Two emulators run the same statements, one sending text cells and one
//! sending each type's binary send format, and every event pair must read
//! the same values and notify, report and stop the same subscriptions.

#![allow(clippy::unwrap_used)]

use crate::common::decisions;

use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind};
use subql::{
    catalog_helpers, DefaultIds, EventKind, PgChangeEvent, PgSqliteEmuSource, SourceItem,
    SubscriptionEngine, SubscriptionRequest,
};

// The emulator's SQLite stores `NUMERIC(10, 2)` as minor units, so `amount` is written as 1250.
const PG_DDL: &str = "CREATE TABLE kinds (
    id INT PRIMARY KEY, flag BOOL, big BIGINT, single REAL, double DOUBLE PRECISION,
    amount NUMERIC(10, 2), body TEXT, code VARCHAR(8), blob BYTEA, day DATE,
    stamp TIMESTAMP, doc JSON, docb JSONB
);";

/// One subscription per column, each reading that column.
const FILTERS: &[&str] = &[
    "SELECT * FROM kinds WHERE id = 7",
    "SELECT * FROM kinds WHERE flag = TRUE",
    "SELECT * FROM kinds WHERE big > 100",
    "SELECT * FROM kinds WHERE single > 0.05",
    "SELECT * FROM kinds WHERE double < 3",
    "SELECT * FROM kinds WHERE amount > 100",
    "SELECT * FROM kinds WHERE body = 'héllo'",
    "SELECT * FROM kinds WHERE code LIKE 'a%'",
    "SELECT * FROM kinds WHERE blob IS NOT NULL",
    "SELECT * FROM kinds WHERE day > '2024-01-01'",
    "SELECT * FROM kinds WHERE stamp < '2025-01-01 00:00:00'",
    "SELECT * FROM kinds WHERE doc IS NOT NULL",
    "SELECT * FROM kinds WHERE docb IS NOT NULL",
];

const STATEMENTS: &[&str] = &[
    "INSERT INTO kinds VALUES (7, 1, 9000000000, 0.1, 2.5, 1250, 'héllo', 'ab', X'DEAD', \
     '2024-03-15', '2024-03-15 12:34:56.789', '{\"a\": 1}', '{\"a\": 1}')",
    "INSERT INTO kinds (id, body) VALUES (8, NULL)",
    "UPDATE kinds SET amount = 9999, body = 'bye', single = 1.5 WHERE id = 7",
    "DELETE FROM kinds WHERE id = 8",
];

type Engine = SubscriptionEngine<PgChangeEvent, DefaultIds, subql::ParserDB>;

fn engine(source: &PgSqliteEmuSource) -> Engine {
    let mut engine = Engine::new(source.pg_catalog().clone(), PostgreSqlDialect {});
    for (consumer, filter) in (1u64..).zip(FILTERS) {
        engine
            .register(SubscriptionRequest::new(consumer, *filter))
            .unwrap_or_else(|e| panic!("{filter} registers: {e}"));
    }
    engine
}

fn events(source: &mut PgSqliteEmuSource) -> Vec<PgChangeEvent> {
    let mut events = Vec::new();
    while let Some(item) = source.poll_next_item().expect("the drain encodes") {
        if let SourceItem::Event(event) = item {
            events.push(event);
        }
    }
    events
}

#[test]
fn binary_cells_read_and_dispatch_as_text_cells_do() {
    let mut text = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("text source");
    let mut binary = PgSqliteEmuSource::open_in_memory(PG_DDL)
        .expect("binary source")
        .with_binary_cells();
    let mut text_engine = engine(&text);
    let mut binary_engine = engine(&binary);
    let db = text.pg_catalog().clone();
    let table = catalog_helpers::table_id::<subql::backend::Postgres, _>(&db, "kinds").unwrap();
    let arity = u16::try_from(catalog_helpers::table_arity(&db, table).unwrap()).unwrap();

    for statement in STATEMENTS {
        text.execute_sql(statement).expect("text side runs");
        binary.execute_sql(statement).expect("binary side runs");
        let (text_events, binary_events) = (events(&mut text), events(&mut binary));
        assert_eq!(text_events.len(), binary_events.len(), "{statement}");
        for (from_text, from_binary) in text_events.iter().zip(&binary_events) {
            assert_eq!(from_text.kind(), from_binary.kind(), "{statement}");
            for row in [RowKind::New, RowKind::Old, RowKind::Pk] {
                for col in 0..arity {
                    let read = from_text.value_at(&db, row, col);
                    let inserted = from_text.kind() == EventKind::Insert && row == RowKind::New;
                    assert!(
                        !inserted || !matches!(read, Ok(subql::backend::Value::Missing)),
                        "{statement}: column {col} reads from text"
                    );
                    assert_eq!(
                        from_binary.value_at(&db, row, col),
                        read,
                        "{statement}: {row:?} column {col}"
                    );
                }
            }
            assert_eq!(
                decisions(&mut binary_engine, from_binary),
                decisions(&mut text_engine, from_text),
                "{statement}"
            );
        }
    }
}
