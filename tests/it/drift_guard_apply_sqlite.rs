//! The apply path asks the guard's shared handle.
//!
//! A table a real `DriftGuard` over a real `SqliteCdcSource` refuses is
//! refused by the apply entry point with the guard's own reasons, before
//! any op touches the target database, while an upload naming only the
//! unrefused table of the same drain applies through the same handle.
#![allow(clippy::unwrap_used)]

use diesel::{sql_query, Connection, QueryableByName, RunQueryDsl, SqliteConnection};
use sqlite_diff_rs::{DiffOps, Insert, PatchSet, SimpleTable, Value};
use subql::drift::{DriftSignal, Incompatibility};
use subql::patchset::{ApplyError, SqliteAdapter};

use crate::drift_guard_sqlite::{drain, live_db, signals_of, table_id};

#[derive(QueryableByName, Debug, PartialEq)]
struct OrderRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    amount: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct LedgerRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    v: i64,
}

fn orders(conn: &mut SqliteConnection) -> Vec<OrderRow> {
    sql_query("SELECT id, amount, status FROM orders ORDER BY id")
        .load::<OrderRow>(conn)
        .unwrap()
}

fn ledger(conn: &mut SqliteConnection) -> Vec<LedgerRow> {
    sql_query("SELECT id, v FROM ledger ORDER BY id")
        .load::<LedgerRow>(conn)
        .unwrap()
}

fn ledger_insert_bytes(id: i64, v: i64) -> Vec<u8> {
    let table = SimpleTable::new("ledger", &[("id", ""), ("v", "")], &[0]);
    let insert = Insert::<_, String, Vec<u8>>::from(table)
        .set(0, Value::Integer(id))
        .unwrap()
        .set(1, Value::Integer(v))
        .unwrap();
    PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(insert)
        .build()
}

#[test]
fn a_guard_refused_table_is_refused_by_the_apply_path() {
    let ddl = [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, v INTEGER)",
    ];
    let (_dir, mut guard) = live_db(&std::format!("{};\n{};", ddl[0], ddl[1]), &ddl);
    let refusals = guard.refusals();
    sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
        .execute(guard.inner().connection())
        .unwrap();
    sql_query("ALTER TABLE orders DROP COLUMN amount")
        .execute(guard.inner().connection())
        .unwrap();

    let items = drain(&mut guard);
    // The guard's own judgment refuses orders over the live drop, with the
    // dropped column as the reason.
    assert_eq!(
        signals_of(&items),
        vec![DriftSignal::Refused {
            table: table_id(guard.catalog(), "orders"),
            reasons: vec![Incompatibility::ColumnDropped {
                column: "amount".into()
            }],
        }],
        "the guard refuses orders over the live drop"
    );
    assert_eq!(
        refusals.refused_tables(),
        vec![(
            table_id(guard.catalog(), "orders"),
            vec![Incompatibility::ColumnDropped {
                column: "amount".into()
            }]
        )]
    );

    // The target database is the consumer's replica, still in the
    // catalog's shape.
    let mut target = SqliteConnection::establish(":memory:").unwrap();
    for statement in ddl {
        sql_query(statement).execute(&mut target).unwrap();
    }
    let catalog = guard.catalog();
    let adapter = SqliteAdapter::new(catalog).expect("the catalog indexes");

    // An upload naming the refused table is refused with the guard's own
    // reasons, before any op applies.
    let orders_table = SimpleTable::new(
        "orders",
        &[("id", ""), ("amount", ""), ("status", "")],
        &[0],
    );
    let insert = Insert::<_, String, Vec<u8>>::from(orders_table)
        .set(0, Value::Integer(7))
        .unwrap()
        .set(1, Value::Integer(250))
        .unwrap()
        .set(2, Value::Text("paid".into()))
        .unwrap();
    let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(insert)
        .build();
    let err = subql::patchset::apply_diffset_bytes_with_catalog(
        catalog,
        &bytes,
        &refusals,
        &mut target,
        &adapter,
    )
    .unwrap_err();
    match err {
        ApplyError::RefusedTable { table, reasons } => {
            assert_eq!(table, "orders", "the upload's table is named");
            assert_eq!(
                reasons,
                vec![Incompatibility::ColumnDropped {
                    column: "amount".into()
                }],
                "the reasons are the guard's own, through the shared handle"
            );
        }
        other => panic!("the refusal carries the typed reason, got {other:?}"),
    }
    assert!(orders(&mut target).is_empty(), "orders stays untouched");
    assert!(
        ledger(&mut target).is_empty(),
        "the target database is unchanged by a refused upload"
    );

    // The same handle lets the upload that names only the unrefused table
    // apply as before.
    let applied = subql::patchset::apply_diffset_bytes_with_catalog(
        catalog,
        &ledger_insert_bytes(1, 40),
        &refusals,
        &mut target,
        &adapter,
    )
    .unwrap();
    assert_eq!(
        applied, 1,
        "the unrefused table applies through the same handle"
    );
    assert_eq!(ledger(&mut target), vec![LedgerRow { id: 1, v: 40 }]);
}
