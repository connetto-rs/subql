//! In-memory SQLite integration test for the diesel-typed follow-insert path.
//!
//! Unlike MySQL, diesel's SQLite backend supports `RETURNING`, so the typed
//! `register_follow_insert` path works here too, and SQLite runs in-process, so
//! this is a plain `#[test]` (no Docker). Proves `FollowRowDecode for Sqlite`:
//! executing `INSERT ... RETURNING id`, decoding the minted `INTEGER PRIMARY KEY`
//! from the returned `SqliteValue`, and registering the follow.
#![allow(clippy::unwrap_used)]

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{SQLite, Value};
use subql::diesel_api::FollowInsertError;
use subql::testing::TestEvent;
use subql::{DefaultIds, RegisterError, SubscriptionEngine};

type Engine = SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB>;

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

/// An in-memory database holding the table `ddl` creates.
fn database(ddl: &str) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query(ddl).execute(&mut conn).expect(ddl);
    conn
}

/// An engine over a catalog declaring `ddl`.
fn engine(ddl: &str) -> Engine {
    Engine::new(
        ParserDB::parse::<SQLiteDialect>(ddl).expect("catalog"),
        SQLiteDialect {},
    )
}

#[test]
fn register_follow_insert_sqlite_decodes_minted_pk() {
    let mut conn = database("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
    let mut engine = engine("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);");

    // Execute INSERT ... RETURNING id on SQLite; the DB mints id=1; subql reads
    // the returned SqliteValue, decodes the key, and follows it.
    let ann = engine
        .register_follow_insert(
            1,
            diesel::insert_into(users::table).values(users::name.eq("ann")),
            &mut conn,
        )
        .expect("follow insert ann");
    assert_eq!(ann.len(), 1);
    let ann_pk = engine.follow_row(1, "users", vec![Value::Int(1)]).unwrap();
    assert_eq!(ann[0].subscription_id, ann_pk.subscription_id);

    let bob = engine
        .register_follow_insert(
            1,
            diesel::insert_into(users::table).values(users::name.eq("bob")),
            &mut conn,
        )
        .expect("follow insert bob");
    assert_eq!(bob.len(), 1);
    let bob_pk = engine.follow_row(1, "users", vec![Value::Int(2)]).unwrap();
    assert_eq!(bob[0].subscription_id, bob_pk.subscription_id);

    assert_ne!(ann[0].subscription_id, bob[0].subscription_id);
}

// A table keyed on a blob, so the returned field decodes through the `Binary`
// arm of `FollowRowDecode for Sqlite`.
diesel::table! {
    docs (id) {
        id -> Binary,
        name -> Text,
    }
}

/// A returned blob field decodes to `Value::Bytes`. SQLite's four storage
/// classes are the whole universe a returned field can carry, and this is the
/// one the row side used to refuse outright while the bind side already read
/// it, so a blob-keyed table could be inserted into but never followed.
#[test]
fn register_follow_insert_sqlite_decodes_a_blob_key() {
    let mut conn = database("CREATE TABLE docs (id BLOB PRIMARY KEY, name TEXT)");
    let mut engine = engine("CREATE TABLE docs (id BLOB PRIMARY KEY, name TEXT);");

    let key = vec![0_u8, 1, 2, 255];
    let followed = engine
        .register_follow_insert(
            1,
            diesel::insert_into(docs::table)
                .values((docs::id.eq(key.clone()), docs::name.eq("spec"))),
            &mut conn,
        )
        .expect("follow insert docs");
    assert_eq!(followed.len(), 1);

    let by_key = engine
        .follow_row(1, "docs", vec![Value::Bytes(key)])
        .expect("follow the same row by its blob key");
    assert_eq!(followed[0].subscription_id, by_key.subscription_id);
}

/// An insert into a table the engine's catalog does not hold is refused
/// before it runs, naming the table as diesel rendered it.
#[test]
fn register_follow_insert_refuses_a_table_outside_the_catalog_before_inserting() {
    let mut conn = database("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
    let mut engine = engine("CREATE TABLE docs (id BLOB PRIMARY KEY, name TEXT);");

    let refused = engine.register_follow_insert(
        1,
        diesel::insert_into(users::table).values(users::name.eq("ann")),
        &mut conn,
    );
    assert!(matches!(
        refused,
        Err(FollowInsertError::Register(RegisterError::UnknownTable(name))) if name == "`users`"
    ));
    let inserted: i64 = users::table.count().get_result(&mut conn).unwrap();
    assert_eq!(inserted, 0);
}
