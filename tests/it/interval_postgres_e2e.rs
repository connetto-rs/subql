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

/// The interval seed runs on PostgreSQL, and the installed `SUM` and `AVG`
/// then answer what the server answers, before and after a folded insert.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn interval_aggregates_seed_and_fold_as_postgres_answers() {
    use diesel::RunQueryDsl;
    use subql::testing::TestEvent;

    #[derive(diesel::QueryableByName)]
    struct Printed {
        #[diesel(sql_type = diesel::sql_types::Text)]
        sum: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        avg: String,
    }

    common::assert_docker_available();
    let db = common::pg_database();
    db.connect().batch_execute(SETUP).unwrap();
    let catalog = sql_traits::structs::ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
        "CREATE TABLE shifts (id INT PRIMARY KEY, span INTERVAL);",
    )
    .unwrap();
    let table = subql::catalog_helpers::table_id::<Postgres, _>(&catalog, "shifts").unwrap();
    let mut engine: subql::SubscriptionEngine<
        TestEvent<Postgres>,
        subql::DefaultIds,
        sql_traits::structs::ParserDB,
    > = subql::SubscriptionEngine::new(catalog, sqlparser::dialect::PostgreSqlDialect {});
    let connector = PgDieselConnector::new(db.connect());
    let mut held = (String::new(), String::new());
    let mut record = |updates: Vec<subql::AggregateValueUpdate<subql::DefaultIds>>| {
        for update in updates {
            let value = update.folded_value().unwrap().to_string();
            if update.subscription == 1 {
                held.0 = value;
            } else {
                held.1 = value;
            }
        }
        held.clone()
    };
    for (id, sql) in [
        (1u64, "SELECT SUM(span) FROM shifts"),
        (2, "SELECT AVG(span) FROM shifts"),
    ] {
        let bootstrap = engine
            .register(subql::SubscriptionRequest::new(id, sql))
            .unwrap()
            .served()
            .unwrap()
            .aggregate_bootstrap
            .clone()
            .unwrap();
        let (row, _) = connector
            .execute_scalar_row(&bootstrap.query.as_read_query(), &bootstrap.kinds, &())
            .unwrap();
        let updates = subql::Install::install(
            &mut engine,
            id,
            subql::AggregateSeedInstall {
                rows: vec![row],
                fence: None,
            },
        )
        .unwrap();
        record(updates.updates);
    }
    let server = || -> (String, String) {
        let printed: Printed =
            diesel::sql_query("SELECT sum(span)::text AS sum, avg(span)::text AS avg FROM shifts")
                .get_result(&mut db.connect())
                .unwrap();
        (printed.sum, printed.avg)
    };
    assert_eq!(record(Vec::new()), server(), "the seed");

    db.connect()
        .batch_execute("INSERT INTO shifts VALUES (4, '1 year -00:00:00.000007')")
        .unwrap();
    let insert = TestEvent::<Postgres>::insert(
        table,
        vec![Value::Int(4), interval("1 year -00:00:00.000007")],
    )
    .with_pk_columns([0u16]);
    assert_eq!(
        record(engine.aggregate_updates(&insert).unwrap().updates),
        server(),
        "after an insert"
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
