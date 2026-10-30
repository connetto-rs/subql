//! Docker-backed reads of PostgreSQL `interval` values through the
//! re-execution connectors: an interval bind reaches the server as an
//! interval, and an interval column reads back field for field.

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::connection::SimpleConnection;
use subql::backend::{Interval, Postgres, Value};
use subql::reexec::{Connector, PgDieselConnector, ReadQuery};

const SETUP: &str = "CREATE TABLE shifts (id INT PRIMARY KEY, span INTERVAL); \
                     INSERT INTO shifts VALUES (1, '1 mon'), (2, '1 day'), \
                     (3, '-1 days +02:00:00');";

fn interval(text: &str) -> Value<Postgres> {
    Value::Interval(Interval::parse(text).unwrap())
}

/// The bound `30 days` selects the row holding `1 mon`, which only an
/// interval comparison on the server does, and each row's interval reads
/// back as the months, days and microseconds the server stores.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn intervals_bind_and_read_back_on_postgres() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.connect().batch_execute(SETUP).unwrap();
    let connector = PgDieselConnector::new(db.connect());

    let bound = ReadQuery::owned(
        String::from("SELECT id FROM shifts WHERE span = $1"),
        vec![interval("30 days")],
    );
    let page = connector.read_page(&bound, 4096, &()).unwrap();
    assert_eq!(page.value.rows, vec![vec![Value::Int(1)]]);

    let all = ReadQuery::without_binds("SELECT span FROM shifts ORDER BY id");
    let page = connector.read_page(&all, 4096, &()).unwrap();
    assert_eq!(
        page.value.rows,
        vec![
            vec![interval("1 mon")],
            vec![interval("1 day")],
            vec![interval("-1 days +02:00:00")],
        ]
    );
}

/// The async connector binds an interval the same way.
#[cfg(feature = "executor-diesel-async-postgres")]
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_interval_binds_through_the_async_connector() {
    use diesel_async::pooled_connection::bb8::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::AsyncPgConnection;
    use subql::reexec::{AsyncConnector, PgAsyncDieselConnector};

    common::assert_docker_available();
    let db = common::pg_database();
    db.connect().batch_execute(SETUP).unwrap();
    let url = db.url();
    common::multi_thread_rt().block_on(async move {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        let pool = Pool::builder().build(manager).await.unwrap();
        let connector = PgAsyncDieselConnector::new(pool);
        let query = ReadQuery::owned(
            String::from("SELECT id FROM shifts WHERE span < $1 ORDER BY id"),
            vec![interval("00:00:00")],
        );
        let page = connector.read_page(&query, 4096, &()).await.unwrap();
        assert_eq!(page.value.rows, vec![vec![Value::Int(3)]]);
    });
}
