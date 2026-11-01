//! The emulator writes a table's `Relation` frame inside the drain's
//! transaction, ahead of the table's first data frame, the shape a server
//! writes, and surfaces it to the consumer before the first event that
//! follows.
//!
//! Gated behind the `pg-sqlite-emu` feature.

#![allow(clippy::unwrap_used)]

use pg_walstream::ReplicaIdentity;
use subql::{PgRelation, PgSqliteEmuSource, SourceItem};

#[test]
fn the_first_item_is_the_tables_relation() {
    let mut source =
        PgSqliteEmuSource::open_in_memory("CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);")
            .expect("build source");
    source
        .execute_sql("INSERT INTO orders VALUES (1, 5.0)")
        .expect("insert");

    let first = source
        .poll_next_item()
        .expect("poll succeeds")
        .expect("an item pending");
    let relation: PgRelation = first.into_relation().expect("a relation item");
    assert_eq!(relation.schema(), "");
    assert_eq!(relation.table(), "orders");
    assert_eq!(relation.oid(), 1000);
    // The emulator always models full replica identity so updates keep
    // old images.
    assert_eq!(relation.replica_identity(), ReplicaIdentity::Full);
    let columns = relation.columns();
    assert_eq!(columns.len(), 2, "the relation states the table's columns");
    assert_eq!(columns[0].name(), "id");
    assert!(
        columns[0].is_key(),
        "the primary key column is flagged as key"
    );
    assert_eq!(columns[1].name(), "price");
    assert!(!columns[1].is_key());

    // The table's first event and the transaction's commit follow it.
    assert!(
        source
            .poll_next_item()
            .expect("poll succeeds")
            .and_then(SourceItem::into_event)
            .is_some(),
        "the first event follows its table's relation"
    );
    assert!(
        source
            .poll_next_item()
            .expect("poll succeeds")
            .and_then(SourceItem::into_commit)
            .is_some(),
        "the transaction's commit follows"
    );
    assert!(source.poll_next_item().expect("poll succeeds").is_none());
}
