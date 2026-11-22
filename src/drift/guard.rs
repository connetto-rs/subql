//! The drift guard: a [`CdcSource`] that judges every schema change
//! against the catalog and drops the events of refused tables.
//!
//! The guard wraps any [`CdcSource`] whose schema messages name a table
//! ([`DriftTableSignal`]), a fixed [`DatabaseLike`] catalog, one
//! [`LiveShapeReader`] per backend and a [`Timer`]. Before the first event
//! it judges every catalog table against a live read. On a schema message
//! it re-judges the table, and every reconciliation interval it re-judges
//! every table it has not refused, which finds a dropped table and a change
//! no schema message announces. It drops the events of refused tables and of
//! tables the catalog does not know, so the fold, dispatch and apply
//! never meet a table the consumer cannot read. It yields one
//! [`DriftSignal::Refused`] item per newly refused table, naming the
//! table and every reason, and one [`DriftSignal::ChangesLost`] item
//! per table whose undrained changes the source lost, so the consumer
//! can resync it. Commits and acks pass through, so acks advance across
//! dropped events.
//!
//! A consumer that follows the live access rules hands the guard the
//! [`TableRules`] it runs ([`RulesFollowing`]). Every live read then
//! compares the table's rules too, and a difference, or an event of the
//! consumer's marker table, makes the guard read every table it has not
//! refused at once and yield one [`DriftSignal::RulesChanged`] naming each
//! table whose rules changed, with the database read.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::future::{poll_fn, Future};
use core::marker::PhantomData;
use core::ops::Bound;
use core::pin::{pin, Pin};
use core::task::Poll;
use core::time::Duration;

use arc_swap::ArcSwap;
use sql_traits::prelude::{DatabaseLike, TableLike};
use sql_traits::structs::TargetName;

use super::refusal::RefusalView;
use super::rules::TableRules;
use super::shape::TableShape;
use super::verdict::{refusal_reasons, Incompatibility};
use crate::backend::{Backend, CdcEvent};
use crate::clock::Timer;
use crate::types::TableId;
use crate::wal::{CdcSource, SourceItem, SourceItemOf};

/// A schema message that names the table it is about, so a
/// [`DriftGuard`] can judge and resync that one table.
pub trait DriftTableSignal: Send + Sync {
    /// The catalog identity of the table this signal is about, or
    /// `None` when the catalog does not know it.
    fn table<DB: DatabaseLike>(&self, catalog: &DB) -> Option<TableId>;

    /// Whether the signal reports the table's undrained changes as lost.
    fn changes_lost(&self) -> bool;
}

/// A pgoutput `Relation` names its table by the schema and name PostgreSQL
/// stores, which the catalog answers with its id. pgoutput reports no lost
/// changes.
impl DriftTableSignal for crate::wal::PgRelation {
    fn table<DB: DatabaseLike>(&self, catalog: &DB) -> Option<TableId> {
        crate::catalog_helpers::table_id_in_schema::<crate::backend::Postgres, DB>(
            catalog,
            Some(self.schema()),
            self.table(),
        )
    }

    fn changes_lost(&self) -> bool {
        false
    }
}

/// The item a [`DriftGuard`] surfaces in place of the inner source's
/// schema message, where `D` is the database a live read loads.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DriftSignal<D> {
    /// A table is now refused. Its events are dropped until the guard
    /// is dropped, and a restart re-judges it.
    Refused {
        /// The catalog identity of the refused table.
        table: TableId,
        /// Every reason the live shape no longer conforms.
        reasons: Vec<Incompatibility>,
    },
    /// A table's undrained changes were lost and its events withheld.
    /// The consumer resyncs the table.
    ChangesLost {
        /// The catalog identity of the table.
        table: TableId,
    },
    /// The live rules of some tables differ from the rules the guard read
    /// last. The consumer translates its authorization again from
    /// `database`.
    RulesChanged {
        /// The catalog identities of the tables whose rules changed, in id
        /// order.
        tables: Vec<TableId>,
        /// The live database the rules were read from, holding every
        /// catalog table the guard has not refused.
        database: Arc<D>,
    },
}

/// Two [`DriftSignal::RulesChanged`] are equal when they name the same
/// tables from the same read.
impl<D> PartialEq for DriftSignal<D> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Refused { table, reasons },
                Self::Refused {
                    table: other_table,
                    reasons: other_reasons,
                },
            ) => table == other_table && reasons == other_reasons,
            (Self::ChangesLost { table }, Self::ChangesLost { table: other_table }) => {
                table == other_table
            }
            (
                Self::RulesChanged { tables, database },
                Self::RulesChanged {
                    tables: other_tables,
                    database: other_database,
                },
            ) => tables == other_tables && Arc::ptr_eq(database, other_database),
            _ => false,
        }
    }
}

impl<D> Eq for DriftSignal<D> {}

/// Reads the live shape and access rules of tables from the live database.
///
/// The stream carries only names and ordinals, so the facts a judgment
/// needs that the stream does not carry come from here, one read per
/// table per schema message.
pub trait LiveShapeReader: Send {
    /// The error a live read can surface.
    type Error: core::error::Error + Send + Sync + 'static;
    /// The database one load answers.
    type Database: DatabaseLike;

    /// The live shape of the table `table` names, or `None` when the
    /// database has no such table. The guard names a table as its catalog
    /// does, by its optional schema and its name, each with its quoting.
    fn shape<'r>(
        &'r mut self,
        table: TargetName<'r>,
    ) -> impl core::future::Future<Output = Result<Option<TableShape>, Self::Error>> + Send + 'r;

    /// The live shape and rules of the table `table` names, from one load,
    /// or `None` when the database has no such table.
    fn shape_and_rules<'r>(
        &'r mut self,
        table: TargetName<'r>,
    ) -> impl core::future::Future<Output = Result<Option<(TableShape, TableRules)>, Self::Error>>
           + Send
           + 'r;

    /// One load of the tables `tables` names.
    fn rules(
        &mut self,
        tables: Vec<TargetName<'static>>,
    ) -> impl core::future::Future<Output = Result<RulesLoad<Self::Database>, Self::Error>> + Send + '_;
}

/// One load of several tables, the database it answered with the rules of
/// each table named, in the order named, `None` for a table the database
/// lacks.
pub type RulesLoad<D> = (D, Vec<Option<TableRules>>);

/// The rules one live load answered for catalog tables, and the database
/// it loaded.
#[derive(Clone, Debug)]
pub struct LiveRules<D> {
    database: Arc<D>,
    rules: BTreeMap<TableId, TableRules>,
}

impl<D> LiveRules<D> {
    /// The database the load answered.
    #[must_use]
    pub const fn database(&self) -> &Arc<D> {
        &self.database
    }

    /// The rules of each table the database holds, by catalog identity.
    #[must_use]
    pub const fn rules(&self) -> &BTreeMap<TableId, TableRules> {
        &self.rules
    }
}

/// Read the live rules of every `catalog` table in one load, as a consumer
/// does at startup to translate its authorization and to build the
/// [`RulesFollowing`] it hands the guard.
///
/// # Errors
///
/// The reader's error when the load fails.
pub async fn read_live_rules<C, R>(
    catalog: &C,
    reader: &mut R,
) -> Result<LiveRules<R::Database>, R::Error>
where
    C: DatabaseLike,
    R: LiveShapeReader,
{
    read_rules_of(catalog, reader, catalog_tables(catalog)).await
}

/// The rules a consumer runs, which a [`DriftGuard`] follows the live
/// database from.
#[derive(Clone, Debug)]
pub struct RulesFollowing {
    baseline: BTreeMap<TableId, TableRules>,
    marker: Option<TableId>,
}

impl RulesFollowing {
    /// Follow the live rules from `baseline`, the read the consumer
    /// translated its authorization from. Every event of the catalog
    /// table `marker` makes the guard read the rules of every table, and
    /// the guard drops the event.
    #[must_use]
    pub fn new<D>(baseline: LiveRules<D>, marker: Option<TableId>) -> Self {
        Self {
            baseline: baseline.rules,
            marker,
        }
    }
}

/// Errors surfaced by a [`DriftGuard`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DriftGuardError<I, R>
where
    I: core::error::Error + Send,
    R: core::error::Error + Send,
{
    /// The inner source failed.
    #[error("inner source: {0}")]
    Inner(#[source] I),
    /// A live read the guard asked for failed.
    #[error("live shape read: {0}")]
    Shape(#[source] R),
}

/// The refused tables a [`DriftGuard`] has judged, shared with the apply
/// path.
///
/// The guard writes the handle as it judges, and the apply entry points
/// read a cloned handle from another task: a clone reads the guard's
/// current refused set through its atomic snapshot, so the two never
/// block on each other, and the handle keeps naming every new refusal
/// for as long as the guard runs.
#[derive(Clone)]
pub struct RefusedTables {
    snapshot: Arc<ArcSwap<RefusalSnapshot>>,
}

impl Default for RefusedTables {
    fn default() -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::new(Arc::new(RefusalSnapshot::default()))),
        }
    }
}

/// The immutable state a [`RefusedTables`] publishes on each refusal.
#[derive(Clone, Default)]
struct RefusalSnapshot {
    /// The refused tables keyed by catalog id, each with its reasons.
    by_id: BTreeMap<TableId, Vec<Incompatibility>>,
    /// The refused tables' catalog names mapped to their ids, for the
    /// name-based query the apply path asks.
    by_name: BTreeMap<String, TableId>,
}

impl RefusedTables {
    /// Record `table` as refused with its reasons.
    ///
    /// A refusal is one-way: a refused table stays refused until the guard
    /// is dropped, and a restart re-judges every table.
    pub(crate) fn insert(&self, table: TableId, name: &str, reasons: Vec<Incompatibility>) {
        let mut next: RefusalSnapshot = (*self.snapshot.load_full()).clone();
        next.by_id.insert(table, reasons);
        next.by_name.insert(name.to_string(), table);
        self.snapshot.store(Arc::new(next));
    }

    /// Whether the table is refused.
    #[must_use]
    pub fn is_refused(&self, table: TableId) -> bool {
        self.snapshot.load().by_id.contains_key(&table)
    }

    /// The refused tables, each with its reasons, in id order.
    #[must_use]
    pub fn refused_tables(&self) -> Vec<(TableId, Vec<Incompatibility>)> {
        self.snapshot
            .load()
            .by_id
            .iter()
            .map(|(table, reasons)| (*table, reasons.clone()))
            .collect()
    }

    /// The reasons `table` is refused, matching names without ASCII case as
    /// an uploaded table name resolves.
    fn refused_by_name(&self, table: &str) -> Option<Vec<Incompatibility>> {
        let snapshot = self.snapshot.load();
        let (_, id) = snapshot
            .by_name
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(table))?;
        snapshot.by_id.get(id).cloned()
    }
}

impl RefusalView for RefusedTables {
    fn refusal(&self, table: &str) -> Option<Vec<Incompatibility>> {
        self.refused_by_name(table)
    }
}

/// A source that judges every schema change against the catalog and
/// drops the events of refused tables before the consumer sees them.
///
/// `S` is the wrapped source, `C` the consumer's fixed catalog, `R` the
/// live reader for the backend and `T` the timer that starts each
/// reconciliation run. The guard is one [`CdcSource`]
/// itself: events, commits and acks of usable tables pass through
/// unchanged, schema messages come back as [`DriftSignal`] items, and
/// the events of refused tables and of tables absent from the catalog
/// are dropped. The refused set is shared with the apply path through the [`RefusedTables`] handle [`DriftGuard::refusals`] hands out, so an upload naming a refused table is refused before any op applies.
///
/// A failed or cancelled live read is retried by `next_item` before any further output or source items.
/// A source item that arrives during a reconciliation run or a read of every table's rules is held until it ends, so an event of a table the run refuses is dropped.
pub struct DriftGuard<S, C, R, T>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike,
    R: LiveShapeReader,
    T: Timer,
{
    inner: S,
    judge: Judge<C, R, <S::Event as CdcEvent>::Backend>,
    timer: T,
    /// The reconciliation interval in the timer's microseconds.
    interval_micros: u64,
    /// The timer reading at which the next reconciliation run is due.
    next_run_micros: u64,
    /// The next table the run in progress judges, `None` between runs.
    run: Option<TableId>,
    pending_judgment: Option<TableId>,
    /// A source result that arrived during a run, handled once it ends.
    held: Option<Result<Option<SourceItemOf<S>>, S::Error>>,
}

/// The catalog, the live reader and the judgments made so far, under the
/// rules of backend `B`.
///
/// Kept apart from the inner source, so a run judges while the source's
/// pending read stays polled.
struct Judge<C, R: LiveShapeReader, B> {
    catalog: C,
    reader: R,
    /// The refused tables, shared with the apply path through
    /// [`RefusedTables`].
    ///
    /// Kept until the guard is dropped: a refused table stays refused,
    /// and a restart re-judges every table.
    refusals: RefusedTables,
    /// The catalog's table ids, so an event the catalog does not know
    /// is dropped rather than folded.
    known: BTreeSet<TableId>,
    /// Signals the guard produced but the consumer has not taken yet.
    signals: VecDeque<DriftSignal<R::Database>>,
    /// The live rules the guard follows, `None` for a consumer that does
    /// not follow them.
    following: Option<Following>,
    /// The backend whose rules name the catalog shape's columns.
    backend: PhantomData<fn() -> B>,
}

/// The live rules a [`Judge`] follows.
struct Following {
    /// The rules each table had when every table was last read, which a
    /// read of one table compares against.
    last: BTreeMap<TableId, TableRules>,
    /// The table whose events announce a change of rules.
    marker: Option<TableId>,
    /// Whether a read found rules that differ from `last`, or the marker
    /// table changed, so every table is read before the next item.
    stale: bool,
}

impl<S, C, R, T> DriftGuard<S, C, R, T>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike,
    R: LiveShapeReader,
    T: Timer,
{
    /// Judge every catalog table against a live read, before the first
    /// event, then start the first reconciliation interval.
    ///
    /// A table the live database does not conform to is refused up
    /// front, and its [`DriftSignal::Refused`] item is yielded ahead of
    /// the first event, like every later refusal. Each later run starts
    /// `interval` after the previous one ended, by `timer`'s reading.
    ///
    /// With `rules`, every live read compares the table's rules too,
    /// starting from the baseline, and a
    /// [`DriftSignal::RulesChanged`] found at startup precedes the first
    /// event.
    ///
    /// # Errors
    ///
    /// [`DriftGuardError::Shape`] when a live read during the startup
    /// judgment fails.
    pub async fn new(
        inner: S,
        catalog: C,
        reader: R,
        interval: Duration,
        timer: T,
        rules: Option<RulesFollowing>,
    ) -> Result<Self, DriftGuardError<S::Error, R::Error>> {
        let known = catalog_tables(&catalog).collect();
        let mut judge = Judge {
            catalog,
            reader,
            refusals: RefusedTables::default(),
            known,
            signals: VecDeque::new(),
            following: rules.map(|rules| Following {
                last: rules.baseline,
                marker: rules.marker,
                stale: false,
            }),
            backend: PhantomData,
        };
        let mut next = judge.first_table();
        judge
            .judge_from(&mut next)
            .await
            .map_err(DriftGuardError::Shape)?;
        let interval_micros = u64::try_from(interval.as_micros()).unwrap_or(u64::MAX);
        let next_run_micros = timer.now_micros().saturating_add(interval_micros);
        Ok(Self {
            inner,
            judge,
            timer,
            interval_micros,
            next_run_micros,
            run: None,
            pending_judgment: None,
            held: None,
        })
    }

    /// The inner source, for a consumer that drives it.
    pub const fn inner(&mut self) -> &mut S {
        &mut self.inner
    }

    /// The catalog the guard judges against.
    pub const fn catalog(&self) -> &C {
        &self.judge.catalog
    }

    /// Whether the table is refused.
    #[must_use]
    pub fn is_refused(&self, table: TableId) -> bool {
        self.judge.refusals.is_refused(table)
    }

    /// The refused tables, each with its reasons, in id order.
    ///
    /// The read-only view the upload path asks before refusing an upload
    /// with the typed reason.
    #[must_use]
    pub fn refused_tables(&self) -> Vec<(TableId, Vec<Incompatibility>)> {
        self.judge.refusals.refused_tables()
    }

    /// The refused-tables handle the guard shares with the apply path.
    ///
    /// Pass a clone of the handle to an apply entry point, so an upload
    /// naming a table the guard refuses is refused before any op applies,
    /// and the handle keeps naming every new refusal for as long as the
    /// guard runs.
    #[must_use]
    pub fn refusals(&self) -> RefusedTables {
        self.judge.refusals.clone()
    }

    /// Queue resync output and retain the table judgment across live-read errors and cancellation.
    fn on_signal(&mut self, signal: &S::Relation) {
        let Some(table) = signal.table(&self.judge.catalog) else {
            // A table the catalog does not know is not judged, and its
            // events are dropped by the loop.
            return;
        };
        if self.judge.refusals.is_refused(table) {
            // A refused table stays refused until the guard is dropped.
            return;
        }
        if signal.changes_lost() {
            self.judge
                .signals
                .push_back(DriftSignal::ChangesLost { table });
        }
        self.pending_judgment = Some(table);
    }
}

impl<S, C, R, T> CdcSource for DriftGuard<S, C, R, T>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike,
    R: LiveShapeReader,
    T: Timer,
{
    type Event = S::Event;
    type Commit = S::Commit;
    type Relation = DriftSignal<R::Database>;
    type Error = DriftGuardError<S::Error, R::Error>;

    fn next_item(
        &mut self,
    ) -> impl Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send {
        let this = self;
        async move {
            loop {
                if let Some(table) = this.pending_judgment {
                    this.judge
                        .judge_table(table)
                        .await
                        .map_err(DriftGuardError::Shape)?;
                    this.pending_judgment = None;
                }
                if this.run.is_some() || this.timer.now_micros() >= this.next_run_micros {
                    if this.run.is_none() {
                        this.run = this.judge.first_table();
                    }
                    this.judge
                        .judge_from(&mut this.run)
                        .await
                        .map_err(DriftGuardError::Shape)?;
                    this.next_run_micros = next_run(&this.timer, this.interval_micros);
                }
                this.judge
                    .settle_rules()
                    .await
                    .map_err(DriftGuardError::Shape)?;
                if let Some(signal) = this.judge.signals.pop_front() {
                    return Ok(Some(SourceItem::Relation(signal)));
                }
                let pulled = if let Some(pulled) = this.held.take() {
                    pulled
                } else {
                    let mut pending = pin!(this.inner.next_item());
                    loop {
                        let due = this.timer.sleep_until(this.next_run_micros);
                        if let Some(pulled) = race(pending.as_mut(), due).await {
                            break pulled;
                        }
                        this.run = this.judge.first_table();
                        beside(
                            pending.as_mut(),
                            &mut this.held,
                            this.judge.judge_from(&mut this.run),
                        )
                        .await
                        .map_err(DriftGuardError::Shape)?;
                        this.next_run_micros = next_run(&this.timer, this.interval_micros);
                        beside(pending.as_mut(), &mut this.held, this.judge.settle_rules())
                            .await
                            .map_err(DriftGuardError::Shape)?;
                        if let Some(signal) = this.judge.signals.pop_front() {
                            return Ok(Some(SourceItem::Relation(signal)));
                        }
                        if let Some(pulled) = this.held.take() {
                            break pulled;
                        }
                    }
                };
                let Some(item) = pulled.map_err(DriftGuardError::Inner)? else {
                    return Ok(None);
                };
                match item {
                    SourceItem::Relation(signal) => this.on_signal(&signal),
                    SourceItem::Event(event) => {
                        let table = event.table_id(&this.judge.catalog);
                        if let Some(following) = this
                            .judge
                            .following
                            .as_mut()
                            .filter(|following| following.marker == Some(table))
                        {
                            // The marker table's events only announce a
                            // change of rules, which the next iteration
                            // reads.
                            following.stale = true;
                        } else if this.judge.known.contains(&table)
                            && !this.judge.refusals.is_refused(table)
                        {
                            // A refused table's events are dropped as they
                            // arrive, and so are the events of a table the
                            // catalog does not know.
                            return Ok(Some(SourceItem::Event(event)));
                        }
                    }
                    SourceItem::Commit(commit) => return Ok(Some(SourceItem::Commit(commit))),
                }
            }
        }
    }

    fn ack(
        &mut self,
        upto: <Self::Event as CdcEvent>::Checkpoint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = &mut self.inner;
        async move { inner.ack(upto).await.map_err(DriftGuardError::Inner) }
    }
}

impl<C, R, B> Judge<C, R, B>
where
    C: DatabaseLike,
    R: LiveShapeReader,
    B: Backend,
{
    /// The first table a run judges.
    fn first_table(&self) -> Option<TableId> {
        self.known.first().copied()
    }

    /// Judge every table not yet refused from `next` on, in id order.
    ///
    /// `next` names the table still to judge, so a failed or cancelled
    /// read resumes at that table, and it is `None` once every table is
    /// judged.
    async fn judge_from(&mut self, next: &mut Option<TableId>) -> Result<(), R::Error> {
        while let Some(table) = *next {
            if !self.refusals.is_refused(table) {
                self.judge_table(table).await?;
            }
            *next = self
                .known
                .range((Bound::Excluded(table), Bound::Unbounded))
                .next()
                .copied();
        }
        Ok(())
    }

    /// Re-read the table's live shape, with its rules when the judge
    /// follows them, and judge it.
    ///
    /// A refusal records the table and queues its [`DriftSignal::Refused`]
    /// item, and rules that differ from the last read of every table mark
    /// the rules stale.
    async fn judge_table(&mut self, table: TableId) -> Result<(), R::Error> {
        // Every judged id comes from the catalog, so neither lookup fails.
        let Some((catalog_table, catalog_shape)) =
            catalog_table(&self.catalog, table).and_then(|catalog_table| {
                TableShape::from_table::<B, C>(catalog_table, &self.catalog)
                    .ok()
                    .map(|shape| (catalog_table, shape))
            })
        else {
            return Ok(());
        };
        let target = crate::catalog_helpers::written_target(catalog_table);
        let live = match &mut self.following {
            None => self.reader.shape(target).await?,
            Some(following) => self
                .reader
                .shape_and_rules(target)
                .await?
                .map(|(shape, rules)| {
                    following.stale |= following.last.get(&table) != Some(&rules);
                    shape
                }),
        };
        if let Some(reasons) = refusal_reasons(&catalog_shape, live.as_ref()) {
            self.signals.push_back(DriftSignal::Refused {
                table,
                reasons: reasons.clone(),
            });
            self.refusals
                .insert(table, catalog_table.table_name(), reasons);
        }
        Ok(())
    }

    /// When the rules are stale, read the rules of every table not refused
    /// in one load, adopt them as the last read, and queue one
    /// [`DriftSignal::RulesChanged`] naming each table whose rules changed.
    ///
    /// A failed or cancelled read leaves the rules stale, so the next call
    /// reads again.
    async fn settle_rules(&mut self) -> Result<(), R::Error> {
        let Some(following) = self.following.as_mut().filter(|following| following.stale) else {
            return Ok(());
        };
        let tables = self
            .known
            .iter()
            .copied()
            .filter(|table| !self.refusals.is_refused(*table));
        let LiveRules { database, rules } =
            read_rules_of(&self.catalog, &mut self.reader, tables).await?;
        let changed: Vec<TableId> = rules
            .iter()
            .filter(|(table, read)| following.last.get(table) != Some(read))
            .map(|(table, _)| *table)
            .collect();
        following.last.extend(rules);
        following.stale = false;
        if !changed.is_empty() {
            self.signals.push_back(DriftSignal::RulesChanged {
                tables: changed,
                database,
            });
        }
        Ok(())
    }
}

/// The identity of every `catalog` table.
fn catalog_tables<C: DatabaseLike>(catalog: &C) -> impl Iterator<Item = TableId> + '_ {
    catalog
        .tables()
        .filter_map(|table| table.table_id(catalog))
        .map(|index| u32::try_from(index).unwrap_or(u32::MAX))
}

/// The `catalog` table `table` identifies.
fn catalog_table<C: DatabaseLike>(catalog: &C, table: TableId) -> Option<&C::Table> {
    usize::try_from(table)
        .ok()
        .and_then(|index| catalog.table_by_id(index))
}

/// Read the rules of the `catalog` tables `tables` in one load.
async fn read_rules_of<C, R>(
    catalog: &C,
    reader: &mut R,
    tables: impl Iterator<Item = TableId>,
) -> Result<LiveRules<R::Database>, R::Error>
where
    C: DatabaseLike,
    R: LiveShapeReader,
{
    let (ids, names): (Vec<TableId>, Vec<TargetName<'static>>) = tables
        .filter_map(|table| {
            catalog_table(catalog, table).map(|found| {
                (
                    table,
                    crate::catalog_helpers::written_target(found).into_owned(),
                )
            })
        })
        .unzip();
    let (database, rules) = reader.rules(names).await?;
    Ok(LiveRules {
        database: Arc::new(database),
        rules: ids
            .into_iter()
            .zip(rules)
            .filter_map(|(table, rules)| rules.map(|rules| (table, rules)))
            .collect(),
    })
}

/// The timer reading one interval from now, when the next run is due.
fn next_run<T: Timer>(timer: &T, interval_micros: u64) -> u64 {
    timer.now_micros().saturating_add(interval_micros)
}

/// The source's result, or `None` once `due` completes first.
async fn race<O>(
    mut pending: Pin<&mut impl Future<Output = O>>,
    due: impl Future<Output = ()>,
) -> Option<O> {
    let mut due = pin!(due);
    poll_fn(|context| {
        if let Poll::Ready(pulled) = pending.as_mut().poll(context) {
            return Poll::Ready(Some(pulled));
        }
        due.as_mut().poll(context).map(|()| None)
    })
    .await
}

/// Drive `work` while still polling the source's pending read, holding its
/// result in `held` until `work` ends.
async fn beside<O, W: Future>(
    mut pending: Pin<&mut impl Future<Output = O>>,
    held: &mut Option<O>,
    work: W,
) -> W::Output {
    let mut work = pin!(work);
    poll_fn(|context| {
        if held.is_none() {
            if let Poll::Ready(pulled) = pending.as_mut().poll(context) {
                *held = Some(pulled);
            }
        }
        work.as_mut().poll(context)
    })
    .await
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "The protocol fixtures must be valid.")]
mod tests {
    use super::*;
    use alloc::string::{String, ToString};
    use alloc::sync::Arc;
    use core::future::ready;
    use core::task::{Context, Waker};
    use parking_lot::Mutex;
    use sql_traits::structs::{IdentifierCase, ParserDB};
    use sqlparser::dialect::PostgreSqlDialect;

    use crate::backend::{Postgres, RowKind, Value};
    use crate::catalog_helpers;
    use crate::clock::ManualClock;
    use crate::testing::{block_on, TestEvent};
    use crate::wal::PgCommit;
    use crate::{PgCommitPosition, PgLsn, PgXid};

    const INTERVAL: Duration = Duration::from_mins(1);
    /// The smallest step a manual clock takes.
    const TICK: Duration = Duration::from_micros(1);

    type Guard<S> = DriftGuard<S, ParserDB, LiveReader, Arc<ManualClock>>;

    enum TableSignal {
        SchemaChanged(&'static str),
        ChangesLost(&'static str),
    }

    impl DriftTableSignal for TableSignal {
        fn table<DB: DatabaseLike>(&self, catalog: &DB) -> Option<TableId> {
            let (Self::SchemaChanged(table) | Self::ChangesLost(table)) = self;
            catalog_helpers::table_id::<Postgres, DB>(catalog, table)
        }

        fn changes_lost(&self) -> bool {
            matches!(self, Self::ChangesLost(_))
        }
    }

    #[derive(Default)]
    struct ReadGate {
        held: bool,
        waiter: Option<Waker>,
    }

    /// What a test changes and observes of a [`LiveReader`].
    #[derive(Clone)]
    struct LiveProbe {
        live: Arc<Mutex<ParserDB>>,
        gate: Arc<Mutex<ReadGate>>,
        /// Every table a read completed for, in order.
        reads: Arc<Mutex<Vec<String>>>,
        /// The table whose next read fails.
        failing: Arc<Mutex<Option<&'static str>>>,
    }

    impl LiveProbe {
        fn new(live: ParserDB) -> Self {
            Self {
                live: Arc::new(Mutex::new(live)),
                gate: Arc::default(),
                reads: Arc::default(),
                failing: Arc::default(),
            }
        }

        fn set_live(&self, ddl: &str) {
            *self.live.lock() = parse(ddl);
        }

        fn hold_reads(&self) {
            self.gate.lock().held = true;
        }

        /// Open the gate and wake the read it held.
        fn release_reads(&self) {
            let waiter = {
                let mut gate = self.gate.lock();
                gate.held = false;
                gate.waiter.take().unwrap()
            };
            waiter.wake();
        }

        fn reads(&self) -> Vec<String> {
            self.reads.lock().clone()
        }
    }

    /// Reads the probe's live catalog once its gate opens.
    struct LiveReader(LiveProbe);

    impl LiveReader {
        /// Wait for the gate to open, then record the read `name`, failing
        /// it when the probe fails that name.
        async fn pass_gate(&self, name: String) -> Result<(), std::io::Error> {
            let probe = &self.0;
            poll_fn(|context| {
                let mut gate = probe.gate.lock();
                if gate.held {
                    gate.waiter = Some(context.waker().clone());
                    drop(gate);
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            let failed = probe
                .failing
                .lock()
                .take_if(|failing| *failing == name)
                .is_some();
            probe.reads.lock().push(name);
            if failed {
                return Err(std::io::Error::other("live read failed"));
            }
            Ok(())
        }
    }

    impl LiveShapeReader for LiveReader {
        type Error = std::io::Error;
        type Database = ParserDB;

        async fn shape<'r>(
            &'r mut self,
            table: TargetName<'r>,
        ) -> Result<Option<TableShape>, Self::Error> {
            self.pass_gate(table.to_string()).await?;
            let catalog = self.0.live.lock();
            let shape = catalog
                .table_by_target(table, IdentifierCase::AsWritten)
                .unwrap()
                .map(|table| TableShape::from_table::<Postgres, _>(table, &*catalog).unwrap());
            drop(catalog);
            Ok(shape)
        }

        async fn shape_and_rules<'r>(
            &'r mut self,
            table: TargetName<'r>,
        ) -> Result<Option<(TableShape, TableRules)>, Self::Error> {
            self.pass_gate(table.to_string()).await?;
            let catalog = self.0.live.lock();
            let read = catalog
                .table_by_target(table, IdentifierCase::AsWritten)
                .unwrap()
                .map(|table| {
                    (
                        TableShape::from_table::<Postgres, _>(table, &*catalog).unwrap(),
                        TableRules::of(table, &*catalog),
                    )
                });
            drop(catalog);
            Ok(read)
        }

        async fn rules(
            &mut self,
            tables: Vec<TargetName<'static>>,
        ) -> Result<RulesLoad<ParserDB>, Self::Error> {
            let names: Vec<String> = tables.iter().map(ToString::to_string).collect();
            self.pass_gate(format!("rules({})", names.join(", ")))
                .await?;
            let catalog = self.0.live.lock().clone();
            let rules = tables
                .into_iter()
                .map(|table| {
                    catalog
                        .table_by_target(table, IdentifierCase::AsWritten)
                        .unwrap()
                        .map(|found| TableRules::of(found, &catalog))
                })
                .collect();
            Ok((catalog, rules))
        }
    }

    /// A one-transaction window reopens only after its commit is acknowledged.
    struct TransactionSource {
        items: VecDeque<SourceItemOf<Self>>,
        awaiting_ack: Option<PgCommitPosition>,
    }

    impl CdcSource for TransactionSource {
        type Event = TestEvent<Postgres, PgCommitPosition>;
        type Commit = PgCommit;
        type Relation = TableSignal;
        type Error = std::io::Error;

        fn next_item(
            &mut self,
        ) -> impl Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send {
            if self.awaiting_ack.is_some() {
                return ready(Err(std::io::Error::other("commit is not acknowledged")));
            }
            let item = self.items.pop_front();
            if let Some(SourceItem::Commit(commit)) = &item {
                self.awaiting_ack = Some(commit.position());
            }
            ready(Ok(item))
        }

        fn ack(
            &mut self,
            upto: PgCommitPosition,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            if self.awaiting_ack != Some(upto) {
                return ready(Err(std::io::Error::other("unexpected commit checkpoint")));
            }
            self.awaiting_ack = None;
            ready(Ok(()))
        }
    }

    /// A stream that stays pending while its queue is empty, as an idle
    /// replication stream does.
    struct IdleSource(Arc<Mutex<VecDeque<SourceItemOf<Self>>>>);

    impl CdcSource for IdleSource {
        type Event = TestEvent<Postgres, PgCommitPosition>;
        type Commit = PgCommit;
        type Relation = TableSignal;
        type Error = std::io::Error;

        fn next_item(
            &mut self,
        ) -> impl Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send {
            let queue = &self.0;
            poll_fn(move |_| {
                queue
                    .lock()
                    .pop_front()
                    .map_or(Poll::Pending, |item| Poll::Ready(Ok(Some(item))))
            })
        }

        fn ack(
            &mut self,
            _upto: PgCommitPosition,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            ready(Ok(()))
        }
    }

    fn parse(ddl: &str) -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap()
    }

    fn orders_and_ledger() -> ParserDB {
        parse(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);
             CREATE TABLE ledger (id INTEGER PRIMARY KEY);",
        )
    }

    fn table(catalog: &ParserDB, name: &str) -> TableId {
        catalog_helpers::table_id::<Postgres, _>(catalog, name).unwrap()
    }

    fn build_guard<S>(
        source: S,
        catalog: ParserDB,
        probe: &LiveProbe,
        clock: &Arc<ManualClock>,
    ) -> Guard<S>
    where
        S: CdcSource<Event = TestEvent<Postgres, PgCommitPosition>, Relation = TableSignal>,
    {
        block_on(DriftGuard::new(
            source,
            catalog,
            LiveReader(probe.clone()),
            INTERVAL,
            Arc::clone(clock),
            None,
        ))
        .unwrap()
    }

    /// Tables under row security, a policy reaching one helper through
    /// another, a grant, and the `policy_changes` marker table.
    const RULES: &str = "
        CREATE ROLE app_user;
        CREATE TABLE policy_changes (id INTEGER PRIMARY KEY);
        CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
        CREATE TABLE notes (id INTEGER PRIMARY KEY);
        CREATE FUNCTION is_member(d INTEGER) RETURNS BOOLEAN LANGUAGE sql
            AS $$ SELECT d > 0 $$;
        CREATE FUNCTION can_read(d INTEGER) RETURNS BOOLEAN LANGUAGE sql
            AS $$ SELECT is_member(d) $$;
        ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
        CREATE POLICY docs_read ON docs FOR SELECT TO app_user USING (can_read(id));
        GRANT SELECT ON docs TO app_user;
    ";

    /// The one read of every table's rules a guard over `RULES` makes.
    const EVERY_TABLE: &str = "rules(docs, notes, policy_changes)";

    /// `RULES` with `from` replaced by `to`.
    fn rules_changed(from: &str, to: &str) -> String {
        assert!(RULES.contains(from), "the fixture lacks {from}");
        RULES.replace(from, to)
    }

    /// A guard over `RULES` following the rules the live database has
    /// when it is built, with `policy_changes` as the marker table.
    fn following_guard<S>(source: S, probe: &LiveProbe, clock: &Arc<ManualClock>) -> Guard<S>
    where
        S: CdcSource<Event = TestEvent<Postgres, PgCommitPosition>, Relation = TableSignal>,
    {
        let catalog = parse(RULES);
        let mut reader = LiveReader(probe.clone());
        let baseline = block_on(read_live_rules(&catalog, &mut reader)).unwrap();
        let marker = Some(table(&catalog, "policy_changes"));
        block_on(DriftGuard::new(
            source,
            catalog,
            reader,
            INTERVAL,
            Arc::clone(clock),
            Some(RulesFollowing::new(baseline, marker)),
        ))
        .unwrap()
    }

    /// The rules of the table `name` in `database`.
    fn rules_in(database: &ParserDB, name: &str) -> TableRules {
        let found = database
            .table_by_target(TargetName::parse(name).unwrap(), IdentifierCase::AsWritten)
            .unwrap()
            .unwrap();
        TableRules::of(found, database)
    }

    /// Assert `item` is a `RulesChanged` naming `changed`, whose database
    /// holds the rules `live` declares for each table.
    fn assert_rules_changed(
        item: Option<&SourceItemOf<Guard<TransactionSource>>>,
        live: &str,
        changed: &[TableId],
    ) {
        let expected = parse(live);
        let holds_live_rules = |database: &ParserDB| {
            ["policy_changes", "docs", "notes"]
                .into_iter()
                .all(|name| rules_in(database, name) == rules_in(&expected, name))
        };
        assert!(
            matches!(
                item,
                Some(SourceItem::Relation(DriftSignal::RulesChanged { tables, database }))
                    if tables == changed && holds_live_rules(database)
            ),
            "the item must name {changed:?} with the live rules: {item:?}"
        );
    }

    fn transaction(items: Vec<SourceItemOf<TransactionSource>>) -> TransactionSource {
        TransactionSource {
            items: VecDeque::from(items),
            awaiting_ack: None,
        }
    }

    fn commit_at(lsn: u64) -> PgCommit {
        PgCommit::new(
            PgCommitPosition::at_commit(PgLsn(lsn), PgXid(1)),
            PgLsn(lsn + 10),
        )
    }

    fn insert_at(table: TableId, lsn: u64) -> SourceItemOf<TransactionSource> {
        SourceItem::Event(
            TestEvent::insert(table, vec![Value::Int(1)]).with_checkpoint(PgCommitPosition::new(
                PgLsn(lsn),
                PgXid(1),
                1,
            )),
        )
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn fully_filtered_transactions_commit_and_ack_before_healthy_transactions() {
        let catalog = orders_and_ledger();
        let orders = table(&catalog, "orders");
        let ledger = table(&catalog, "ledger");
        let first_commit = PgCommit::new(
            PgCommitPosition::at_commit(PgLsn(100), PgXid(1)),
            PgLsn(110),
        );
        let second_commit = PgCommit::new(
            PgCommitPosition::at_commit(PgLsn(200), PgXid(2)),
            PgLsn(210),
        );
        let healthy_position = PgCommitPosition::new(PgLsn(200), PgXid(2), 1);
        let source = TransactionSource {
            items: VecDeque::from([
                SourceItem::Relation(TableSignal::SchemaChanged("orders")),
                SourceItem::Event(
                    TestEvent::insert(orders, vec![Value::Int(7)])
                        .with_checkpoint(PgCommitPosition::new(PgLsn(100), PgXid(1), 1)),
                ),
                SourceItem::Relation(TableSignal::SchemaChanged("untracked")),
                SourceItem::Event(
                    TestEvent::insert(u32::MAX, vec![Value::Int(8)])
                        .with_checkpoint(PgCommitPosition::new(PgLsn(100), PgXid(1), 2)),
                ),
                SourceItem::Commit(first_commit),
                SourceItem::Event(
                    TestEvent::insert(ledger, vec![Value::Int(9)])
                        .with_checkpoint(healthy_position),
                ),
                SourceItem::Commit(second_commit),
            ]),
            awaiting_ack: None,
        };
        let probe = LiveProbe::new(catalog.clone());
        let mut guard = build_guard(source, catalog, &probe, &Arc::new(ManualClock::new(0)));
        assert!(!guard.is_refused(orders));
        probe.set_live(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY);
             CREATE TABLE ledger (id INTEGER PRIMARY KEY);",
        );

        let Some(SourceItem::Relation(signal)) = block_on(guard.next_item()).unwrap() else {
            panic!("the refusal must precede transaction progress");
        };
        assert_eq!(
            signal,
            DriftSignal::Refused {
                table: orders,
                reasons: vec![Incompatibility::ColumnDropped {
                    column: "amount".into(),
                }],
            }
        );
        assert!(guard.is_refused(orders));
        let Some(SourceItem::Commit(commit)) = block_on(guard.next_item()).unwrap() else {
            panic!("a fully filtered transaction must still commit");
        };
        assert_eq!(commit, first_commit);
        block_on(guard.ack(commit.position())).unwrap();

        let Some(SourceItem::Event(event)) = block_on(guard.next_item()).unwrap() else {
            panic!("the healthy transaction must follow the acknowledged commit");
        };
        assert_eq!(event.table_id(guard.catalog()), ledger);
        assert_eq!(event.checkpoint(), Some(healthy_position));
        assert_eq!(
            event.value_at(guard.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(9)
        );
        let Some(SourceItem::Commit(commit)) = block_on(guard.next_item()).unwrap() else {
            panic!("the healthy transaction must commit");
        };
        assert_eq!(commit, second_commit);
        block_on(guard.ack(commit.position())).unwrap();
        assert!(block_on(guard.next_item()).unwrap().is_none());
    }

    #[test]
    fn cancelled_shape_reads_retain_judgment_and_emit_changes_lost_once() {
        let catalog = parse("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);");
        let orders = table(&catalog, "orders");
        let commit = PgCommit::new(
            PgCommitPosition::at_commit(PgLsn(300), PgXid(3)),
            PgLsn(310),
        );
        let source = TransactionSource {
            items: VecDeque::from([
                SourceItem::Relation(TableSignal::ChangesLost("orders")),
                SourceItem::Event(
                    TestEvent::insert(orders, vec![Value::Int(7)])
                        .with_checkpoint(PgCommitPosition::new(PgLsn(300), PgXid(3), 1)),
                ),
                SourceItem::Commit(commit),
            ]),
            awaiting_ack: None,
        };
        let probe = LiveProbe::new(catalog.clone());
        let mut guard = build_guard(source, catalog, &probe, &Arc::new(ManualClock::new(0)));
        probe.set_live("CREATE TABLE orders (id INTEGER PRIMARY KEY);");
        probe.hold_reads();

        assert!(poll_once(pin!(guard.next_item())).is_pending());
        assert!(!guard.is_refused(orders));
        probe.release_reads();

        let Some(SourceItem::Relation(signal)) = block_on(guard.next_item()).unwrap() else {
            panic!("the recovered judgment must report the lost changes");
        };
        assert_eq!(signal, DriftSignal::ChangesLost { table: orders });
        assert!(
            guard.is_refused(orders),
            "resync output must follow the completed judgment"
        );
        let Some(SourceItem::Relation(signal)) = block_on(guard.next_item()).unwrap() else {
            panic!("the recovered judgment must refuse the incompatible table");
        };
        assert_eq!(
            signal,
            DriftSignal::Refused {
                table: orders,
                reasons: vec![Incompatibility::ColumnDropped {
                    column: "amount".into(),
                }],
            }
        );
        let Some(SourceItem::Commit(delivered)) = block_on(guard.next_item()).unwrap() else {
            panic!("the filtered transaction must commit without repeated signals");
        };
        assert_eq!(delivered, commit);
        block_on(guard.ack(delivered.position())).unwrap();
        assert!(block_on(guard.next_item()).unwrap().is_none());
    }

    #[test]
    fn an_idle_stream_refuses_a_dropped_table_one_interval_after_the_last_run() {
        let catalog = orders_and_ledger();
        let ledger = table(&catalog, "ledger");
        let probe = LiveProbe::new(catalog.clone());
        let clock = Arc::new(ManualClock::new(0));
        let mut guard = build_guard(IdleSource(Arc::default()), catalog, &probe, &clock);
        probe.set_live("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);");
        let early = INTERVAL.checked_sub(TICK).unwrap();
        {
            let mut next = pin!(guard.next_item());
            clock.advance(early);
            assert!(poll_once(next.as_mut()).is_pending());
            clock.advance(TICK);
            assert_eq!(
                poll_once(next.as_mut())
                    .map(|item| item.unwrap().and_then(SourceItem::into_relation)),
                Poll::Ready(Some(DriftSignal::Refused {
                    table: ledger,
                    reasons: vec![Incompatibility::TableMissing],
                })),
                "the run refuses the dropped table while the stream is idle"
            );
        }
        assert_eq!(probe.reads(), ["ledger", "orders", "ledger", "orders"]);

        let mut next = pin!(guard.next_item());
        clock.advance(early);
        assert!(poll_once(next.as_mut()).is_pending());
        assert_eq!(probe.reads().len(), 4, "the next run waits a full interval");
        clock.advance(TICK);
        assert!(poll_once(next.as_mut()).is_pending());
        assert_eq!(
            probe.reads(),
            ["ledger", "orders", "ledger", "orders", "orders"],
            "a refused table is not read again"
        );
    }

    #[test]
    fn tables_sharing_a_name_in_two_schemas_are_read_and_refused_apart() {
        const BOTH: &str = "CREATE SCHEMA sales;
             CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);
             CREATE TABLE sales.orders (id INTEGER PRIMARY KEY, region TEXT);";
        let catalog = parse(BOTH);
        let sales_orders = table(&catalog, "sales.orders");
        let probe = LiveProbe::new(parse(BOTH));
        let clock = Arc::new(ManualClock::new(0));
        let mut guard = build_guard(IdleSource(Arc::default()), catalog, &probe, &clock);
        assert_eq!(
            guard.refused_tables(),
            Vec::new(),
            "each table reads its own shape"
        );
        let mut reads = probe.reads();
        reads.sort();
        assert_eq!(reads, ["orders", "sales.orders"]);

        probe.set_live(
            "CREATE SCHEMA sales;
             CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);
             CREATE TABLE sales.orders (id INTEGER PRIMARY KEY);",
        );
        let region_dropped = vec![Incompatibility::ColumnDropped {
            column: "region".into(),
        }];
        {
            let mut next = pin!(guard.next_item());
            clock.advance(INTERVAL);
            assert_eq!(
                poll_once(next.as_mut())
                    .map(|item| item.unwrap().and_then(SourceItem::into_relation)),
                Poll::Ready(Some(DriftSignal::Refused {
                    table: sales_orders,
                    reasons: region_dropped.clone(),
                }))
            );
        }
        assert_eq!(guard.refused_tables(), [(sales_orders, region_dropped)]);
    }

    #[test]
    fn a_cancelled_run_keeps_its_progress_and_the_item_that_arrived_during_it() {
        let catalog = orders_and_ledger();
        let orders = table(&catalog, "orders");
        let commit = PgCommit::new(
            PgCommitPosition::at_commit(PgLsn(400), PgXid(4)),
            PgLsn(410),
        );
        let probe = LiveProbe::new(catalog.clone());
        let clock = Arc::new(ManualClock::new(0));
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let mut guard = build_guard(IdleSource(Arc::clone(&queue)), catalog, &probe, &clock);
        probe.set_live(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY);
             CREATE TABLE ledger (id INTEGER PRIMARY KEY);",
        );
        probe.hold_reads();
        {
            let mut next = pin!(guard.next_item());
            assert!(poll_once(next.as_mut()).is_pending());
            clock.advance(INTERVAL);
            assert!(poll_once(next.as_mut()).is_pending());
            queue.lock().extend([
                SourceItem::Event(
                    TestEvent::insert(orders, vec![Value::Int(7)])
                        .with_checkpoint(PgCommitPosition::new(PgLsn(400), PgXid(4), 1)),
                ),
                SourceItem::Commit(commit),
            ]);
            assert!(poll_once(next.as_mut()).is_pending());
        }
        assert_eq!(
            queue.lock().len(),
            1,
            "the run holds the event that arrived"
        );
        probe.release_reads();

        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_relation),
            Some(DriftSignal::Refused {
                table: orders,
                reasons: vec![Incompatibility::ColumnDropped {
                    column: "amount".into(),
                }],
            }),
            "the resumed run refuses the changed table"
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_commit),
            Some(commit),
            "the held event of the refused table is dropped"
        );
        assert_eq!(probe.reads(), ["ledger", "orders", "ledger", "orders"]);
    }

    #[test]
    fn an_item_that_arrives_during_a_run_that_refuses_nothing_follows_the_run() {
        let catalog = orders_and_ledger();
        let ledger = table(&catalog, "ledger");
        let probe = LiveProbe::new(catalog.clone());
        let clock = Arc::new(ManualClock::new(0));
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let mut guard = build_guard(IdleSource(Arc::clone(&queue)), catalog, &probe, &clock);
        probe.hold_reads();
        let position = PgCommitPosition::new(PgLsn(450), PgXid(5), 1);
        {
            let mut next = pin!(guard.next_item());
            assert!(poll_once(next.as_mut()).is_pending());
            clock.advance(INTERVAL);
            assert!(poll_once(next.as_mut()).is_pending());
            queue.lock().push_back(SourceItem::Event(
                TestEvent::insert(ledger, vec![Value::Int(3)]).with_checkpoint(position),
            ));
            assert!(poll_once(next.as_mut()).is_pending());
            assert!(
                queue.lock().is_empty(),
                "the run holds the event that arrived"
            );
            probe.release_reads();
            assert_eq!(
                poll_once(next.as_mut()).map(|item| item
                    .unwrap()
                    .and_then(SourceItem::into_event)
                    .and_then(|event| event.checkpoint())),
                Poll::Ready(Some(position)),
                "the held event arrives once the run ends"
            );
        }
        assert_eq!(probe.reads().len(), 4, "the run judged both tables first");
        block_on(guard.ack(position)).unwrap();
    }

    #[test]
    fn a_read_failing_during_a_run_beside_the_source_keeps_the_held_item_and_resumes_there() {
        let catalog = orders_and_ledger();
        let ledger = table(&catalog, "ledger");
        let probe = LiveProbe::new(catalog.clone());
        let clock = Arc::new(ManualClock::new(0));
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let mut guard = build_guard(IdleSource(Arc::clone(&queue)), catalog, &probe, &clock);
        probe.hold_reads();
        let position = PgCommitPosition::new(PgLsn(470), PgXid(6), 1);
        {
            let mut next = pin!(guard.next_item());
            assert!(poll_once(next.as_mut()).is_pending());
            clock.advance(INTERVAL);
            assert!(poll_once(next.as_mut()).is_pending());
            queue.lock().push_back(SourceItem::Event(
                TestEvent::insert(ledger, vec![Value::Int(4)]).with_checkpoint(position),
            ));
            assert!(poll_once(next.as_mut()).is_pending());
            *probe.failing.lock() = Some("ledger");
            probe.release_reads();
            assert!(matches!(
                poll_once(next.as_mut()),
                Poll::Ready(Err(DriftGuardError::Shape(_)))
            ));
        }
        assert!(
            queue.lock().is_empty(),
            "the failed run kept the held event"
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_event)
                .and_then(|event| event.checkpoint()),
            Some(position),
            "the resumed run ends and the held event follows"
        );
        assert_eq!(
            probe.reads(),
            ["ledger", "orders", "ledger", "ledger", "orders"],
            "the resumed run starts again at the table whose read failed"
        );
    }

    #[test]
    fn a_due_run_resumes_at_the_table_whose_read_failed_before_pulling_the_source() {
        let catalog = orders_and_ledger();
        let orders = table(&catalog, "orders");
        let probe = LiveProbe::new(catalog.clone());
        let clock = Arc::new(ManualClock::new(0));
        let queue =
            Arc::new(Mutex::new(VecDeque::from([SourceItem::Event(
                TestEvent::insert(orders, vec![Value::Int(9)])
                    .with_checkpoint(PgCommitPosition::new(PgLsn(500), PgXid(5), 1)),
            )])));
        let mut guard = build_guard(IdleSource(Arc::clone(&queue)), catalog, &probe, &clock);
        probe.set_live("CREATE TABLE ledger (id INTEGER PRIMARY KEY);");
        *probe.failing.lock() = Some("orders");
        clock.advance(INTERVAL);

        assert!(matches!(
            block_on(guard.next_item()),
            Err(DriftGuardError::Shape(_))
        ));
        assert_eq!(
            queue.lock().len(),
            1,
            "a due run starts before the source is read"
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_relation),
            Some(DriftSignal::Refused {
                table: orders,
                reasons: vec![Incompatibility::TableMissing],
            }),
            "the resumed run refuses the dropped table"
        );
        assert_eq!(
            probe.reads(),
            ["ledger", "orders", "ledger", "orders", "orders"],
            "the resumed run reads only the table whose read failed"
        );
        assert!(poll_once(pin!(guard.next_item())).is_pending());
        assert!(
            queue.lock().is_empty(),
            "the refused table's event is dropped"
        );
    }

    /// An upload naming a refused table under any ASCII case is refused.
    #[test]
    fn a_refused_table_is_refused_under_any_ascii_case_of_its_name() {
        let refusals = RefusedTables::default();
        refusals.insert(0, "Orders", vec![Incompatibility::TableMissing]);
        for uploaded in ["Orders", "orders", "ORDERS"] {
            assert_eq!(
                refusals.refusal(uploaded),
                Some(vec![Incompatibility::TableMissing]),
                "an upload naming `{uploaded}` is refused"
            );
        }
        assert_eq!(refusals.refusal("ledger"), None);
    }

    /// pgoutput names a relation by the schema and name PostgreSQL stores,
    /// so a quoted mixed-case name matches only as stored and a relation
    /// in another schema matches no catalog table.
    #[test]
    fn a_pgoutput_relation_names_the_catalog_table_it_stores() {
        let catalog = parse(
            "CREATE SCHEMA sales;
             CREATE TABLE sales.orders (id INTEGER PRIMARY KEY);
             CREATE TABLE sales.\"Ledger\" (id INTEGER PRIMARY KEY);",
        );
        let relation = |schema: &str, name: &str| {
            crate::wal::PgRelation::new(
                PgLsn(1),
                1,
                schema.into(),
                name.into(),
                pg_walstream::ReplicaIdentity::Full,
                Vec::new(),
            )
        };
        for (schema, name, expected) in [
            ("sales", "orders", Some(table(&catalog, "sales.orders"))),
            ("sales", "Ledger", Some(table(&catalog, "sales.\"Ledger\""))),
            ("sales", "LEDGER", None),
            ("sales", "ledger", None),
            ("public", "orders", None),
        ] {
            let signal = relation(schema, name);
            assert_eq!(
                DriftTableSignal::table(&signal, &catalog),
                expected,
                "{schema}.{name}"
            );
            assert!(!signal.changes_lost());
        }
    }

    #[test]
    fn a_schema_message_finding_changed_rules_signals_them_ahead_of_the_tables_events() {
        let docs = table(&parse(RULES), "docs");
        let commit = commit_at(500);
        let probe = LiveProbe::new(parse(RULES));
        let source = transaction(vec![
            SourceItem::Relation(TableSignal::SchemaChanged("docs")),
            insert_at(docs, 500),
            SourceItem::Commit(commit),
        ]);
        let mut guard = following_guard(source, &probe, &Arc::new(ManualClock::new(0)));
        let live = rules_changed("USING (can_read(id))", "USING (can_read(id) AND id > 0)");
        probe.set_live(&live);

        assert_rules_changed(
            block_on(guard.next_item()).unwrap().as_ref(),
            &live,
            &[docs],
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_event)
                .map(|event| event.table_id(guard.catalog())),
            Some(docs),
            "the table's event must follow the changed rules"
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_commit),
            Some(commit)
        );
        assert_eq!(
            probe.reads()[1..],
            ["docs", "notes", "policy_changes", "docs", EVERY_TABLE],
            "the startup judgment reads each table, then the schema message one table and every table's rules once"
        );
    }

    #[test]
    fn a_marker_event_reads_every_tables_rules_and_never_reaches_the_consumer() {
        let catalog = parse(RULES);
        let marker = table(&catalog, "policy_changes");
        let notes = table(&catalog, "notes");
        let commit = commit_at(600);
        let probe = LiveProbe::new(catalog);
        let source = transaction(vec![insert_at(marker, 600), SourceItem::Commit(commit)]);
        let mut guard = following_guard(source, &probe, &Arc::new(ManualClock::new(0)));
        let live = format!("{RULES} GRANT SELECT ON notes TO app_user;");
        probe.set_live(&live);

        assert_rules_changed(
            block_on(guard.next_item()).unwrap().as_ref(),
            &live,
            &[notes],
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_commit),
            Some(commit),
            "the marker table's event is dropped"
        );
    }

    #[test]
    fn a_marker_event_over_unchanged_rules_yields_only_the_commit() {
        let catalog = parse(RULES);
        let marker = table(&catalog, "policy_changes");
        let commit = commit_at(700);
        let probe = LiveProbe::new(catalog);
        let source = transaction(vec![insert_at(marker, 700), SourceItem::Commit(commit)]);
        let mut guard = following_guard(source, &probe, &Arc::new(ManualClock::new(0)));

        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_commit),
            Some(commit)
        );
        assert_eq!(probe.reads().last().map(String::as_str), Some(EVERY_TABLE));
    }

    #[test]
    fn an_idle_run_finds_a_changed_helper_a_policy_reaches_through_another() {
        let docs = table(&parse(RULES), "docs");
        let probe = LiveProbe::new(parse(RULES));
        let clock = Arc::new(ManualClock::new(0));
        let mut guard = following_guard(IdleSource(Arc::default()), &probe, &clock);
        let live = rules_changed("AS $$ SELECT d > 0 $$", "AS $$ SELECT d > 1 $$");
        probe.set_live(&live);
        let mut next = pin!(guard.next_item());
        assert!(poll_once(next.as_mut()).is_pending());
        clock.advance(INTERVAL);

        assert!(
            matches!(
                poll_once(next.as_mut()),
                Poll::Ready(Ok(Some(SourceItem::Relation(DriftSignal::RulesChanged {
                    tables, ..
                })))) if tables == [docs]
            ),
            "the run must find the changed helper"
        );
    }

    #[test]
    fn a_failed_rules_read_in_an_idle_run_is_returned_and_read_again() {
        let docs = table(&parse(RULES), "docs");
        let probe = LiveProbe::new(parse(RULES));
        let clock = Arc::new(ManualClock::new(0));
        let mut guard = following_guard(IdleSource(Arc::default()), &probe, &clock);
        let live = rules_changed("AS $$ SELECT d > 0 $$", "AS $$ SELECT d > 1 $$");
        probe.set_live(&live);
        *probe.failing.lock() = Some(EVERY_TABLE);
        {
            let mut next = pin!(guard.next_item());
            assert!(poll_once(next.as_mut()).is_pending());
            clock.advance(INTERVAL);
            assert_eq!(
                poll_once(next.as_mut()).map(|read| read.is_err()),
                Poll::Ready(true),
                "the run returns the failed rules read"
            );
        }

        assert!(
            matches!(
                block_on(guard.next_item()),
                Ok(Some(SourceItem::Relation(DriftSignal::RulesChanged { tables, .. })))
                    if tables == [docs]
            ),
            "the next item reads the rules again"
        );
    }

    #[test]
    fn a_failed_shape_and_rules_read_is_returned_and_read_again() {
        let docs = table(&parse(RULES), "docs");
        let probe = LiveProbe::new(parse(RULES));
        let clock = Arc::new(ManualClock::new(0));
        let mut guard = following_guard(IdleSource(Arc::default()), &probe, &clock);
        let live = rules_changed("USING (can_read(id))", "USING (can_read(id) AND id > 0)");
        probe.set_live(&live);
        *probe.failing.lock() = Some("docs");
        clock.advance(INTERVAL);

        assert!(
            block_on(guard.next_item()).is_err(),
            "the run returns the failed read"
        );
        assert!(
            matches!(
                block_on(guard.next_item()),
                Ok(Some(SourceItem::Relation(DriftSignal::RulesChanged { tables, .. })))
                    if tables == [docs]
            ),
            "the resumed run reads the table again and finds its changed policy"
        );
    }

    #[test]
    fn rules_changed_before_the_guard_starts_precede_the_first_event() {
        let catalog = parse(RULES);
        let docs = table(&catalog, "docs");
        let probe = LiveProbe::new(parse(RULES));
        let mut reader = LiveReader(probe.clone());
        let baseline = block_on(read_live_rules(&catalog, &mut reader)).unwrap();
        let live = rules_changed("GRANT SELECT ON docs", "GRANT SELECT, UPDATE ON docs");
        probe.set_live(&live);
        let source = transaction(vec![
            insert_at(docs, 800),
            SourceItem::Commit(commit_at(800)),
        ]);
        let mut guard = block_on(DriftGuard::new(
            source,
            catalog,
            reader,
            INTERVAL,
            Arc::new(ManualClock::new(0)),
            Some(RulesFollowing::new(baseline, None)),
        ))
        .unwrap();

        assert_rules_changed(
            block_on(guard.next_item()).unwrap().as_ref(),
            &live,
            &[docs],
        );
        assert!(block_on(guard.next_item())
            .unwrap()
            .and_then(SourceItem::into_event)
            .is_some());
    }

    #[test]
    fn a_failed_rules_read_is_read_again_before_any_item() {
        let catalog = parse(RULES);
        let marker = table(&catalog, "policy_changes");
        let notes = table(&catalog, "notes");
        let commit = commit_at(900);
        let probe = LiveProbe::new(catalog);
        let source = transaction(vec![insert_at(marker, 900), SourceItem::Commit(commit)]);
        let mut guard = following_guard(source, &probe, &Arc::new(ManualClock::new(0)));
        let live = format!("{RULES} GRANT SELECT ON notes TO app_user;");
        probe.set_live(&live);
        *probe.failing.lock() = Some(EVERY_TABLE);

        assert!(
            block_on(guard.next_item()).is_err(),
            "the failed rules read is returned"
        );
        assert_rules_changed(
            block_on(guard.next_item()).unwrap().as_ref(),
            &live,
            &[notes],
        );
        assert_eq!(
            block_on(guard.next_item())
                .unwrap()
                .and_then(SourceItem::into_commit),
            Some(commit)
        );
    }

    #[test]
    fn a_refused_table_is_left_out_of_the_rules_read_and_the_signal() {
        let catalog = parse(RULES);
        let docs = table(&catalog, "docs");
        let notes = table(&catalog, "notes");
        let probe = LiveProbe::new(catalog);
        let source = transaction(vec![SourceItem::Relation(TableSignal::SchemaChanged(
            "docs",
        ))]);
        let mut guard = following_guard(source, &probe, &Arc::new(ManualClock::new(0)));
        let live = format!(
            "{} GRANT SELECT ON notes TO app_user;",
            rules_changed("title TEXT", "title INTEGER")
                .replace("USING (can_read(id))", "USING (true)")
        );
        probe.set_live(&live);

        assert!(
            matches!(
                block_on(guard.next_item()).unwrap(),
                Some(SourceItem::Relation(DriftSignal::Refused { table, .. })) if table == docs
            ),
            "the changed shape must refuse the table first"
        );
        assert!(
            matches!(
                block_on(guard.next_item()).unwrap(),
                Some(SourceItem::Relation(DriftSignal::RulesChanged { tables, .. }))
                    if tables == [notes]
            ),
            "the changed grant must follow"
        );
        assert_eq!(
            probe.reads().last().map(String::as_str),
            Some("rules(notes, policy_changes)")
        );
    }

    #[test]
    fn the_live_rules_are_the_rules_of_each_catalog_table_the_one_load_found() {
        let catalog = parse(RULES);
        let live = rules_changed("CREATE TABLE notes (id INTEGER PRIMARY KEY);", "");
        let probe = LiveProbe::new(parse(&live));
        let read = block_on(read_live_rules(&catalog, &mut LiveReader(probe.clone()))).unwrap();
        let mut held = [table(&catalog, "policy_changes"), table(&catalog, "docs")];
        held.sort_unstable();
        assert_eq!(read.rules().keys().copied().collect::<Vec<_>>(), held);
        for name in ["policy_changes", "docs"] {
            assert_eq!(
                read.rules().get(&table(&catalog, name)),
                Some(&rules_in(read.database(), name))
            );
        }
        assert_eq!(probe.reads(), [EVERY_TABLE]);
    }

    #[test]
    fn rules_changed_signals_are_equal_when_they_name_the_same_tables_from_one_read() {
        let read = Arc::new(parse(RULES));
        let docs = table(&read, "docs");
        let changed = |database: &Arc<ParserDB>| DriftSignal::RulesChanged {
            tables: vec![docs],
            database: Arc::clone(database),
        };
        assert_eq!(changed(&read), changed(&read));
        assert_ne!(changed(&read), changed(&Arc::new(parse(RULES))));
        assert_ne!(changed(&read), DriftSignal::ChangesLost { table: docs });
    }
}
