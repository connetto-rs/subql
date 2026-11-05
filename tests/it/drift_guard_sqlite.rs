//! DriftGuard over the live SQLite source.
//!
//! Every test runs one `DriftGuard` over a real `SqliteCdcSource` and a
//! real `SqliteLiveReader` against a file-backed database, and writes only
//! through the guard's inner connection. One table's drift names itself
//! and is dropped, while the other tables of the same drain keep being
//! served.
#![allow(clippy::unwrap_used)]

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{CdcEvent, RowKind, SQLite, Value};
use subql::drift::{DriftGuard, DriftSignal, Incompatibility, SqliteLiveReader};
use subql::sqlite_cdc::{SqliteCdcSource, SqliteChangesetEvent};
use subql::wal::{CdcSource, SourceItem};

type Guard = DriftGuard<SqliteCdcSource<ParserDB>, ParserDB, SqliteLiveReader>;
type Item = SourceItem<SqliteChangesetEvent, core::convert::Infallible, DriftSignal>;

/// A file-backed live database, its `SqliteLiveReader` connection and the
/// catalog. The file, not `:memory:`, is what lets the reader's own
/// connection see the source's database.
fn live_db(catalog_ddl: &str, statements: &[&str]) -> (tempfile::TempDir, Guard) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("live.sqlite");
    let mut source_connection = SqliteConnection::establish(path.to_str().unwrap()).unwrap();
    for statement in statements {
        sql_query(*statement)
            .execute(&mut source_connection)
            .unwrap();
    }
    let reader_connection = SqliteConnection::establish(path.to_str().unwrap()).unwrap();
    let catalog = ParserDB::parse::<SQLiteDialect>(catalog_ddl).unwrap();
    let source = SqliteCdcSource::new(source_connection, catalog.clone()).unwrap();
    let guard = subql::testing::block_on(DriftGuard::new(
        source,
        catalog,
        SqliteLiveReader::new(reader_connection),
    ))
    .unwrap();
    (dir, guard)
}

/// The catalog id of the table named `name`.
fn table_id(catalog: &ParserDB, name: &str) -> u32 {
    subql::catalog_helpers::table_id::<SQLite, ParserDB>(catalog, name).unwrap()
}

/// Every item the guard yields until it runs dry.
fn drain(guard: &mut Guard) -> Vec<Item> {
    let mut items = Vec::new();
    loop {
        let item = subql::testing::block_on(guard.next_item()).unwrap();
        match item {
            Some(item) => items.push(item),
            None => break,
        }
    }
    items
}

fn events_of(items: Vec<Item>) -> Vec<SqliteChangesetEvent> {
    items
        .into_iter()
        .filter_map(SourceItem::into_event)
        .collect()
}

/// The signals among `items`, read beside the events that follow them.
fn signals_of(items: &[Item]) -> Vec<DriftSignal> {
    items
        .iter()
        .filter_map(|item| match item {
            SourceItem::Relation(signal) => Some(signal.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn an_added_nullable_column_keeps_the_table_served() {
    let (_dir, mut guard) = live_db(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT);",
        &["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)"],
    );
    let orders = table_id(guard.catalog(), "orders");
    sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("ALTER TABLE orders ADD COLUMN extra TEXT")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    // The addition is allowed, so the guard judges it away and serves the
    // table: no signal, the insert arrives, and the extra column is
    // projected out of the event.
    assert!(items
        .iter()
        .all(|item| !matches!(item, SourceItem::Relation(_))));
    let events = events_of(items);
    assert_eq!(events.len(), 1, "the added column keeps the table served");
    let event = &events[0];
    assert_eq!(event.table_id(guard.catalog()), orders);
    assert_eq!(
        event.value_at(guard.catalog(), RowKind::New, 0).unwrap(),
        Value::Int(7)
    );
    assert_eq!(
        event.value_at(guard.catalog(), RowKind::New, 1).unwrap(),
        Value::Int(250),
        "amount is not shifted by the extra column"
    );
    assert_eq!(
        event.value_at(guard.catalog(), RowKind::New, 2).unwrap(),
        Value::String("paid".into())
    );
    assert!(!guard.is_refused(orders));
}

#[test]
fn a_dropped_column_refuses_orders_but_keeps_the_other_table() {
    let ddl = [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, v INTEGER)",
    ];
    let (_dir, mut guard) = live_db(&std::format!("{};\n{};", ddl[0], ddl[1]), &ddl);
    let orders = table_id(guard.catalog(), "orders");
    let ledger = table_id(guard.catalog(), "ledger");
    sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("INSERT INTO ledger (id, v) VALUES (1, 40)")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("ALTER TABLE orders DROP COLUMN amount")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    // The drop names orders alone: one refusal carrying the dropped column,
    // and the other table's event from the same drain still arrives.
    let signals = signals_of(&items);
    assert_eq!(
        signals,
        vec![DriftSignal::Refused {
            table: orders,
            reasons: vec![Incompatibility::ColumnDropped {
                column: "amount".into()
            }],
        }],
        "only orders is refused, and the reason names the dropped column"
    );
    assert!(guard.is_refused(orders));
    let events = events_of(items);
    assert_eq!(events.len(), 1, "ledger's event survives the same drain");
    assert_eq!(events[0].table_id(guard.catalog()), ledger);
    assert_eq!(
        events[0]
            .value_at(guard.catalog(), RowKind::New, 0)
            .unwrap(),
        Value::Int(1)
    );
}

#[test]
fn a_drop_plus_add_refuses_with_no_shifted_values() {
    let (_dir, mut guard) = live_db(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT);",
        &["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)"],
    );
    let orders = table_id(guard.catalog(), "orders");
    sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("ALTER TABLE orders DROP COLUMN amount")
        .execute(guard.inner().connection())
        .unwrap();
    // SQLite takes one `ALTER` operation per statement, so the add is a
    // second statement, which is how a rebuild shows up on the wire.
    sql_query("ALTER TABLE orders ADD COLUMN extra TEXT")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    // A drop plus an add renders the recorded row at the same width with
    // its values shifted, so the refusal has to come before any of that
    // reaches the consumer: one reason, and no orders event at all.
    let signals = signals_of(&items);
    assert_eq!(
        signals,
        vec![DriftSignal::Refused {
            table: orders,
            reasons: vec![Incompatibility::ColumnDropped {
                column: "amount".into()
            }],
        }],
        "the drop is reported and the nullable add is not a reason"
    );
    assert!(guard.is_refused(orders));
    let events = events_of(items);
    assert!(
        events.is_empty(),
        "no shifted value crosses the guard, got {events:?}"
    );
}

#[test]
fn a_loosened_check_refuses_the_table() {
    let catalog_ddl =
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER CHECK (amount > 100), status TEXT);";
    let (_dir, mut guard) = live_db(catalog_ddl, &[catalog_ddl.trim_end_matches(';')]);
    let orders = table_id(guard.catalog(), "orders");
    sql_query("INSERT INTO orders (id, amount, status) VALUES (1, 200, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    // SQLite rewrites a `CHECK` only through a table rebuild.
    sql_query(
        "CREATE TABLE orders_new (id INTEGER PRIMARY KEY, amount INTEGER CHECK (amount > 0), status TEXT)",
    )
    .execute(guard.inner().connection())
    .unwrap();
    sql_query("INSERT INTO orders_new SELECT id, amount, status FROM orders")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("DROP TABLE orders")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("ALTER TABLE orders_new RENAME TO orders")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    let signals = signals_of(&items);
    assert_eq!(
        signals,
        vec![DriftSignal::Refused {
            table: orders,
            reasons: vec![Incompatibility::CheckChanged {
                catalog: "amount > 100".into(),
                live: "amount > 0".into(),
            }],
        }],
        "the loosened CHECK is the reason"
    );
    assert!(guard.is_refused(orders));
    assert!(
        events_of(items).is_empty(),
        "no orders event crosses the guard"
    );
}

#[test]
fn a_table_outside_the_catalog_keeps_its_events_dropped() {
    let (_dir, mut guard) = live_db(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT);",
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "CREATE TABLE untracked (id INTEGER PRIMARY KEY, v INTEGER)",
        ],
    );
    let orders = table_id(guard.catalog(), "orders");
    sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("INSERT INTO untracked (id, v) VALUES (1, 40)")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    // The catalog never read the `untracked` table, so its changes never
    // surface and no signal names them: only orders arrives.
    assert!(
        items
            .iter()
            .all(|item| !matches!(item, SourceItem::Relation(_))),
        "an unknown table is not judged, got {items:?}"
    );
    let events = events_of(items);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].table_id(guard.catalog()), orders);
}

#[test]
fn acks_advance_across_dropped_events() {
    let ddl = [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, v INTEGER)",
    ];
    let (_dir, mut guard) = live_db(&std::format!("{};\n{};", ddl[0], ddl[1]), &ddl);
    let ledger = table_id(guard.catalog(), "ledger");
    sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("INSERT INTO ledger (id, v) VALUES (1, 40)")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("ALTER TABLE orders DROP COLUMN amount")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    assert_eq!(
        events_of(items).len(),
        1,
        "ledger's event crosses the refusal"
    );
    // The commit acks across the dropped events, so the consumer loop
    // advances and the source keeps serving what the catalog can read.
    subql::testing::block_on(guard.ack(subql::NoCheckpoint)).unwrap();
    sql_query("INSERT INTO ledger (id, v) VALUES (2, 80)")
        .execute(guard.inner().connection())
        .unwrap();
    let events = events_of(drain(&mut guard));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].table_id(guard.catalog()), ledger);
    assert_eq!(
        events[0]
            .value_at(guard.catalog(), RowKind::New, 0)
            .unwrap(),
        Value::Int(2)
    );
}
