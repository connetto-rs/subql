//! A subscription stopped on an event takes its queued reads with it, so the
//! next drain runs nothing it owns.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{RowKind, SQLite, ScalarFamily, Value};
use subql::reexec::{AutoResolvingEngine, Connector, ReadQuery, RowPage, Snapshot, SyncMode};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DispatchError, GroupedScalarSeedInstall, Install, NoCheckpoint,
    PgLsn, SubscriptionEngine, SubscriptionRequest, Tier, ValueError,
};

use crate::common::altered_cell::AlteredCell;

const DDL: &str =
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT COLLATE BINARY, amount INTEGER, status TEXT);";

type Event = TestEvent<SQLite>;
type AlteredEvent = AlteredCell<Event>;
type Engine = AutoResolvingEngine<AlteredEvent, DefaultIds, ParserDB, SyncMode<RefusingReads>>;

/// A read the test connectors refuse, so a test sees any read the engine
/// sent rather than the engine answering it.
#[derive(Debug, thiserror::Error)]
enum RefusedRead {
    #[error("a stopped subscription's read must not run")]
    StoppedSubscription,
    #[error("only the grouped read is answered")]
    Unanswered,
}

/// A connector that refuses every read, so a test sees any read the engine
/// sent rather than answering it.
struct RefusingReads;

impl Connector for RefusingReads {
    type AuthContext = ();
    type Error = RefusedRead;
    type Checkpoint = NoCheckpoint;
    type Backend = SQLite;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, SQLite>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> Result<
        (
            Value<SQLite>,
            Option<<NoCheckpoint as subql::Checkpoint>::Fence>,
        ),
        RefusedRead,
    > {
        Err(RefusedRead::StoppedSubscription)
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, SQLite>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<SQLite>, NoCheckpoint>, RefusedRead> {
        Err(RefusedRead::StoppedSubscription)
    }
}

/// A connector that answers the grouped read for the `south` group with a
/// snapshot whose fence misses the change that arrived while the read was
/// out, so the read is asked again and the follow-up is held until the next
/// resolve.
struct AnsweringSouth;

impl Connector for AnsweringSouth {
    type AuthContext = ();
    type Error = RefusedRead;
    type Checkpoint = PgLsn;
    type Backend = SQLite;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, SQLite>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<SQLite>, Option<PgLsn>), RefusedRead> {
        Err(RefusedRead::Unanswered)
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, SQLite>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<SQLite>, PgLsn>, RefusedRead> {
        Ok(Snapshot {
            value: RowPage {
                // The group key travels with the read trigger, so the answer
                // row holds the extreme and its row count alone.
                columns: vec!["min".into(), "count".into()],
                rows: vec![vec![Value::Int(3), Value::Int(2)]],
                more: false,
            },
            fence: Some(PgLsn(11)),
        })
    }
}

type FencedEvent = TestEvent<SQLite, PgLsn>;
type AlteredFenced = AlteredCell<FencedEvent>;
type FencedEngine =
    AutoResolvingEngine<AlteredFenced, DefaultIds, ParserDB, SyncMode<AnsweringSouth>>;

/// An `orders` row, with id, region, amount and status.
fn row(
    id: i64,
    region: Value<SQLite>,
    amount: Value<SQLite>,
    status: Value<SQLite>,
) -> Vec<Value<SQLite>> {
    vec![Value::Int(id), region, amount, status]
}

/// A change to the `south` row, which the filter cannot answer, whose region
/// cell no longer decodes when `altered` is set. With `insert` clear the
/// event removes the row.
fn south_change(orders: subql::TableId, insert: bool, at: PgLsn, altered: bool) -> AlteredFenced {
    let image = row(
        9,
        Value::String("south".into()),
        Value::Int(3),
        Value::Missing,
    );
    let event = if insert {
        FencedEvent::insert(orders, image)
    } else {
        FencedEvent::delete(orders, image)
    };
    AlteredCell::new(
        event.with_pk_columns([0u16]).with_checkpoint(at),
        RowKind::New,
        1,
        ScalarFamily::String,
        altered,
    )
}

/// A change to a fresh `east` row whose region cell no longer decodes.
fn altered_change(orders: subql::TableId, at: PgLsn) -> AlteredFenced {
    let image = row(
        10,
        Value::String("east".into()),
        Value::Int(2),
        Value::String("paid".into()),
    );
    AlteredCell::new(
        FencedEvent::insert(orders, image)
            .with_pk_columns([0u16])
            .with_checkpoint(at),
        RowKind::New,
        1,
        ScalarFamily::String,
        true,
    )
}

#[test]
fn a_stopped_subscription_takes_its_queued_reads_with_it() {
    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("DDL parses");
    let orders =
        catalog_helpers::table_id::<SQLite, _>(&catalog, "orders").expect("orders resolves");
    // The wrapper records a read context for every registration it makes, so
    // the seed lands on the engine before it is wrapped. This test never
    // resolves one of the grouped subscription's reads, which is the point.
    let mut inner =
        SubscriptionEngine::<AlteredEvent, DefaultIds, ParserDB>::new(catalog, SQLiteDialect {});
    // A grouped extreme seeded over its regions, so a later change to an
    // unseen group asks for a scoped read.
    let registered = inner
        .register(SubscriptionRequest::<DefaultIds, SQLite>::new(
            7u64,
            "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
        ))
        .expect("the grouped extreme registers");
    let Tier::GroupedScalar { .. } = registered.tier else {
        panic!(
            "expected the grouped scalar tier, got {:?}",
            registered.tier
        );
    };
    let subscription = registered.subscription_id;
    Install::install(
        &mut inner,
        subscription,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(5),
                Value::Int(2),
            ]],
            fence: None,
        },
    )
    .expect("the group seed installs");
    let mut engine: Engine = AutoResolvingEngine::new(inner, SyncMode(RefusingReads));

    // A change the filter cannot answer queues the group's read.
    let refresh = AlteredCell::new(
        Event::insert(
            orders,
            row(
                9,
                Value::String("south".into()),
                Value::Int(3),
                Value::Missing,
            ),
        )
        .with_pk_columns([0u16]),
        RowKind::New,
        1,
        ScalarFamily::String,
        false,
    );
    let first = engine
        .apply_leaving_reads_queued(&refresh)
        .expect("the change dispatches");
    assert_eq!(first.outstanding, 1, "the group read is queued");
    assert!(first.stopped.is_empty());

    // The column's type changes under the stream, so the next event's region
    // cell no longer decodes, and the subscription stops.
    let altered = AlteredCell::new(
        Event::insert(
            orders,
            row(
                10,
                Value::String("east".into()),
                Value::Int(2),
                Value::String("paid".into()),
            ),
        )
        .with_pk_columns([0u16]),
        RowKind::New,
        1,
        ScalarFamily::String,
        true,
    );
    let second = engine
        .apply_leaving_reads_queued(&altered)
        .expect("the altered event dispatches");
    assert_eq!(second.stopped.len(), 1);
    assert_eq!(second.stopped[0].subscription_id, subscription);
    assert_eq!(second.stopped[0].consumer_id, 7);
    assert!(matches!(
        second.stopped[0].error,
        DispatchError::Value(ValueError::Builtin { column: 1, .. })
    ));
    assert_eq!(
        second.outstanding, 0,
        "the stopped subscription's queued read is purged"
    );

    // The next drain runs nothing the stopped subscription owned.
    let settled = engine.resolve_collect().expect("the drain runs nothing");
    assert!(settled.scalar_updates.is_empty());
    assert!(settled.aggregate_updates.is_empty());
}

#[test]
fn a_stopped_subscription_takes_its_held_re_read_with_it() {
    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("DDL parses");
    let orders =
        catalog_helpers::table_id::<SQLite, _>(&catalog, "orders").expect("orders resolves");
    let mut engine: FencedEngine = AutoResolvingEngine::new(
        SubscriptionEngine::<AlteredFenced, DefaultIds, ParserDB>::new(catalog, SQLiteDialect {}),
        SyncMode(AnsweringSouth),
    );
    // Registration goes through the wrapper, which is what records the read
    // context the resolve below runs under.
    let registered = engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(
                7u64,
                "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
            ),
            (),
        )
        .expect("the grouped extreme registers");
    let Tier::GroupedScalar { .. } = registered.tier else {
        panic!(
            "expected the grouped scalar tier, got {:?}",
            registered.tier
        );
    };
    let subscription = registered.subscription_id;
    Install::install(
        &mut engine,
        subscription,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(5),
                Value::Int(2),
            ]],
            fence: Some(PgLsn(5)),
        },
    )
    .expect("the group seed installs");

    // A change the filter cannot answer queues the group's read.
    let refresh = south_change(orders, true, PgLsn(11), false);
    let first = engine
        .apply_leaving_reads_queued(&refresh)
        .expect("the change dispatches");
    assert_eq!(first.outstanding, 1, "the group read is queued");
    assert!(first.stopped.is_empty());

    // The min row is deleted while the read is out, so the answer will not
    // hold the change.
    let delete = south_change(orders, false, PgLsn(12), false);
    let second = engine
        .apply_leaving_reads_queued(&delete)
        .expect("the delete dispatches");
    assert_eq!(
        second.outstanding, 1,
        "the same read is still the one outstanding"
    );
    assert!(second.stopped.is_empty());

    // The read answers without holding the delete, so the group is re-read
    // and the follow-up is held until the next resolve.
    let settled = engine.resolve_collect().expect("the read resolves");
    assert!(
        settled.aggregate_updates.is_empty(),
        "the re-read alone decides the group, so the answer delivers nothing"
    );

    // The column's type changes under the stream, so the next event's region
    // cell no longer decodes, and the subscription stops.
    let altered = altered_change(orders, PgLsn(13));
    let third = engine
        .apply_leaving_reads_queued(&altered)
        .expect("the altered event dispatches");
    assert_eq!(third.stopped.len(), 1);
    assert_eq!(third.stopped[0].subscription_id, subscription);
    assert_eq!(
        third.outstanding, 0,
        "the held re-read is purged with the subscription"
    );

    // The next drain runs nothing the stopped subscription owned. Without
    // the purge it would release the held read against a removed context.
    let settled = engine.resolve_collect().expect("the drain runs nothing");
    assert!(settled.scalar_updates.is_empty());
    assert!(settled.aggregate_updates.is_empty());
}
