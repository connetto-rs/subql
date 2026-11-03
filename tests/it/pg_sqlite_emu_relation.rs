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

    let items = source.drain().expect("drain succeeds");
    assert!(
        matches!(items.first(), Some(SourceItem::Relation(_))),
        "the first item is the table's relation, got {items:?}"
    );
    let relations: Vec<PgRelation> = items
        .iter()
        .cloned()
        .filter_map(SourceItem::into_relation)
        .collect();
    let commits: Vec<_> = items
        .iter()
        .cloned()
        .filter_map(SourceItem::into_commit)
        .collect();
    assert_eq!(
        items.len(),
        3,
        "the relation, the first event, and the commit"
    );
    assert_eq!(relations.len(), 1, "the table's relation leads the stream");
    let relation = &relations[0];
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
    assert!(
        matches!(items.last(), Some(SourceItem::Commit(_))),
        "the transaction's commit follows its rows, got {items:?}"
    );
    assert_eq!(commits.len(), 1, "one commit ends the transaction");
}
