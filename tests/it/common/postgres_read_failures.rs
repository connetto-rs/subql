use diesel::connection::SimpleConnection;
use diesel::{ExpressionMethods, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{AutoResolvingEngine, CursorError, ReExecError, ResolvedReads, ResolverMode};
#[cfg(feature = "executor-diesel-postgres")]
use subql::reexec::{Connector, SyncMode};
#[cfg(feature = "executor-diesel-postgres")]
use subql::SubscriptionEngine;
use subql::{DefaultIds, PgCommitPosition, SubscriptionRequest, Wal2JsonV2Event};

const CATALOG: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL); \
    CREATE TABLE dropped (id INTEGER PRIMARY KEY, status TEXT NOT NULL);";

diesel::table! {
    orders (id) {
        id -> Integer,
        status -> Text,
    }
}

pub fn prepare(connection: &mut PgConnection) -> ParserDB {
    connection.batch_execute(CATALOG).expect("create tables");
    diesel::insert_into(orders::table)
        .values((orders::id.eq(1), orders::status.eq("paid")))
        .execute(connection)
        .expect("insert order");
    ParserDB::parse::<PostgreSqlDialect>(CATALOG).expect("parse catalog")
}

pub fn register_and_queue<Mode>(
    engine: &mut AutoResolvingEngine<Wal2JsonV2Event, DefaultIds, ParserDB, Mode>,
) -> (u64, u64)
where
    Mode: ResolverMode<Postgres, AuthContext = ()>,
{
    let stopped_subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT DISTINCT id FROM dropped WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("register dropped table")
        .subscription_id;
    let healthy_subscription = engine
        .register(
            SubscriptionRequest::new(
                2u64,
                "SELECT DISTINCT id FROM orders WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("register healthy table")
        .subscription_id;
    queue_reads(engine);
    (stopped_subscription, healthy_subscription)
}

pub fn queue_reads<Mode>(
    engine: &mut AutoResolvingEngine<Wal2JsonV2Event, DefaultIds, ParserDB, Mode>,
) where
    Mode: ResolverMode<Postgres, AuthContext = ()>,
{
    for row in [
        r#"{"action":"U","schema":"public","table":"dropped"}"#,
        r#"{"action":"U","schema":"public","table":"orders"}"#,
    ] {
        let events = super::read_wal2json_v2(&super::in_transaction(row));
        engine
            .apply_leaving_reads_queued(&events[0])
            .expect("queue read");
    }
}

pub fn assert_stopped<Error: core::fmt::Debug>(
    reads: &ResolvedReads<DefaultIds, Postgres, PgCommitPosition, Error>,
    stopped_subscription: u64,
    healthy_subscription: u64,
    assert_error: impl FnOnce(&Error),
) {
    assert_eq!(reads.stopped.len(), 1);
    assert_eq!(reads.stopped[0].subscription_id, stopped_subscription);
    assert_eq!(reads.stopped[0].consumer_id, 1);
    match &reads.stopped[0].error {
        ReExecError::Cursor {
            error: CursorError::Connector(error),
            ..
        } => assert_error(error),
        other => panic!("unexpected read error {other:?}"),
    }
    assert_eq!(reads.rows_updates.len(), 1);
    assert_eq!(reads.rows_updates[0].subscription_id, healthy_subscription);
    assert_eq!(reads.rows_updates[0].rows, vec![vec![Value::Int(1)]]);
}

pub fn assert_undefined_table(error: &diesel::result::Error) {
    assert!(
        matches!(error, diesel::result::Error::DatabaseError(_, information)
        if information.sqlstate().is_some_and(|code| code.as_str() == "42P01")),
        "{error:?}"
    );
}

#[cfg(feature = "executor-diesel-postgres")]
pub fn dropped_table<Connection>(
    connector: Connection,
    mut connection: PgConnection,
    assert_error: impl FnOnce(&Connection::Error),
) where
    Connection: Connector<Backend = Postgres, Checkpoint = PgCommitPosition, AuthContext = ()>,
    Connection::Error: core::fmt::Debug,
{
    let catalog = prepare(&mut connection);
    let inner = SubscriptionEngine::<Wal2JsonV2Event, DefaultIds, ParserDB>::new(
        catalog,
        PostgreSqlDialect {},
    );
    let mut engine = AutoResolvingEngine::new(inner, SyncMode(connector));
    let (stopped_subscription, healthy_subscription) = register_and_queue(&mut engine);
    assert_eq!(engine.pending_read_count(), 2);
    connection
        .batch_execute("DROP TABLE dropped")
        .expect("drop table");
    let reads = engine.resolve_collect().expect("resolve independent reads");
    assert_stopped(
        &reads,
        stopped_subscription,
        healthy_subscription,
        assert_error,
    );
    assert_eq!(engine.pending_read_count(), 0);
    assert!(engine
        .snapshot(stopped_subscription)
        .expect("stopped subscription is absent")
        .is_none());
    assert!(engine
        .snapshot(healthy_subscription)
        .expect("healthy subscription still reads")
        .is_some());
    queue_reads(&mut engine);
    assert_eq!(engine.pending_read_count(), 1);
    assert!(engine
        .resolve_collect()
        .expect("resolve healthy trigger")
        .stopped
        .is_empty());
}
