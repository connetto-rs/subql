//! A subscription stopped on an event takes its queued reads with it, so the
//! next drain runs nothing it owns.

#![allow(clippy::unwrap_used)]

use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{CdcEvent, RowKind, SQLite, ScalarFamily, Value};
use subql::reexec::{AutoResolvingEngine, Connector, ReadQuery, RowPage, Snapshot, SyncMode};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ColumnId, DefaultIds, DispatchError, EventKind, GroupedScalarSeedInstall,
    Install, NoCheckpoint, SubscriptionEngine, SubscriptionRequest, TableId, Tier, ValueError,
};

const DDL: &str =
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT COLLATE BINARY, amount INTEGER, status TEXT);";

type Event = TestEvent<SQLite>;
type AlteredEvent = AlteredRegion<Event>;
type Engine = AutoResolvingEngine<AlteredEvent, DefaultIds, ParserDB, SyncMode<RefusingReads>>;

/// A connector that refuses every read, so a test sees any read the engine
/// sent rather than answering it.
struct RefusingReads;

impl Connector for RefusingReads {
    type AuthContext = ();
    type Error = String;
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
        String,
    > {
        Err("a stopped subscription's read must not run".into())
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, SQLite>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<SQLite>, NoCheckpoint>, String> {
        Err("a stopped subscription's read must not run".into())
    }
}

/// An event whose new image carries a `region` cell that no longer decodes
/// into the column's declared text type, as the stream does after the
/// column's type changed. Every other cell decodes.
struct AlteredRegion<E> {
    inner: E,
    /// Whether the new image's `region` cell refuses to decode.
    altered: bool,
}

impl<E: CdcEvent> AlteredRegion<E> {
    const fn new(inner: E, altered: bool) -> Self {
        Self { inner, altered }
    }
}

impl<E: CdcEvent> CdcEvent for AlteredRegion<E> {
    type Backend = E::Backend;
    type Checkpoint = E::Checkpoint;

    fn kind(&self) -> EventKind {
        self.inner.kind()
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        self.inner.table_id(db)
    }

    fn checkpoint(&self) -> Option<E::Checkpoint> {
        self.inner.checkpoint()
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        self.inner.pk_columns(db)
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        self.inner.changed_columns(db)
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<E::Backend>, ValueError> {
        if self.altered && row == RowKind::New && col == 1 {
            return Err(ValueError::Builtin {
                column: col,
                kind: ScalarFamily::String,
            });
        }
        self.inner.value_at(db, row, col)
    }
}

/// An `orders` row, with id, region, amount and status.
fn row(
    id: i64,
    region: Value<SQLite>,
    amount: Value<SQLite>,
    status: Value<SQLite>,
) -> Vec<Value<SQLite>> {
    vec![Value::Int(id), region, amount, status]
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
    let refresh = AlteredRegion::new(
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
        false,
    );
    let first = engine
        .apply_leaving_reads_queued(&refresh)
        .expect("the change dispatches");
    assert_eq!(first.outstanding, 1, "the group read is queued");
    assert!(first.stopped.is_empty());

    // The column's type changes under the stream, so the next event's region
    // cell no longer decodes, and the subscription stops.
    let altered = AlteredRegion::new(
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
