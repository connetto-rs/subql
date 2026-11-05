//! The drift guard: a [`CdcSource`] that judges every schema change
//! against the catalog and drops the events of refused tables.
//!
//! The guard wraps any [`CdcSource`] whose schema messages name a table
//! ([`DriftTableSignal`]), a fixed [`DatabaseLike`] catalog, and one
//! [`LiveShapeReader`] per backend. Before the first event it judges
//! every catalog table against a live read. On a schema message it
//! re-judges the table, and it drops the events of refused tables and of
//! tables the catalog does not know, so the fold, dispatch and apply
//! never meet a table the consumer cannot read. It yields one
//! [`DriftSignal::Refused`] item per newly refused table, naming the
//! table and every reason, and one [`DriftSignal::ChangesLost`] item
//! per table whose undrained changes the source lost, so the consumer
//! can resync it. Commits and acks pass through, so acks advance across
//! dropped events.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::vec::Vec;

use sql_traits::prelude::{DatabaseLike, TableLike};

use super::shape::{SqlparserDatabase, TableShape};
use super::verdict::{refusal_reasons, Incompatibility};
use crate::backend::CdcEvent;
use crate::catalog_helpers;
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

/// The item a [`DriftGuard`] surfaces in place of the inner source's
/// schema message.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DriftSignal {
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
}

/// Reads the live shape of a table from the live database.
///
/// The stream carries only names and ordinals, so the facts a judgment
/// needs that the stream does not carry come from here, one read per
/// table per schema message.
pub trait LiveShapeReader: Send {
    /// The error a live read can surface.
    type Error: core::error::Error + Send + Sync + 'static;

    /// The live shape of `table`, or `None` when the database has no
    /// such table.
    fn shape<'r>(
        &'r mut self,
        table: &'r str,
    ) -> impl core::future::Future<Output = Result<Option<TableShape>, Self::Error>> + Send + 'r;
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

/// A source that judges every schema change against the catalog and
/// drops the events of refused tables before the consumer sees them.
///
/// `S` is the wrapped source, `C` the consumer's fixed catalog and `R`
/// the live reader for the backend. The guard is one [`CdcSource`]
/// itself: events, commits and acks of usable tables pass through
/// unchanged, schema messages come back as [`DriftSignal`] items, and
/// the events of refused tables and of tables absent from the catalog
/// are dropped.
pub struct DriftGuard<S, C, R>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike,
    R: LiveShapeReader,
{
    inner: S,
    catalog: C,
    reader: R,
    /// The refused tables, keyed by catalog id, with their reasons.
    ///
    /// Kept until the guard is dropped: a refused table stays refused,
    /// and a restart re-judges every table.
    refused: BTreeMap<TableId, Vec<Incompatibility>>,
    /// The catalog's table ids, so an event the catalog does not know
    /// is dropped rather than folded.
    known: BTreeSet<TableId>,
    /// Items the guard produced but the consumer has not taken yet.
    pending: VecDeque<SourceItem<S::Event, S::Commit, DriftSignal>>,
}

impl<S, C, R> DriftGuard<S, C, R>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike,
    R: LiveShapeReader,
{
    /// Judge every catalog table against a live read, before the first
    /// event.
    ///
    /// A table the live database does not conform to is refused up
    /// front, and its [`DriftSignal::Refused`] item is yielded ahead of
    /// the first event, like every later refusal.
    ///
    /// # Errors
    ///
    /// [`DriftGuardError::Shape`] when a live read during the startup
    /// judgment fails.
    pub async fn new(
        inner: S,
        catalog: C,
        mut reader: R,
    ) -> Result<Self, DriftGuardError<S::Error, R::Error>>
    where
        C: SqlparserDatabase,
    {
        let mut known = BTreeSet::new();
        let mut refused = BTreeMap::new();
        let mut pending = VecDeque::new();
        for table in catalog.tables() {
            let name = table.table_name().to_string();
            let Some(index) = table.table_id(&catalog) else {
                continue;
            };
            let id = u32::try_from(index).unwrap_or(u32::MAX);
            known.insert(id);
            let live = reader.shape(&name).await.map_err(DriftGuardError::Shape)?;
            let Ok(catalog_shape) = TableShape::from_table(table, &catalog) else {
                continue;
            };
            if let Some(reasons) = refusal_reasons(&catalog_shape, live.as_ref()) {
                pending.push_back(SourceItem::Relation(DriftSignal::Refused {
                    table: id,
                    reasons: reasons.clone(),
                }));
                refused.insert(id, reasons);
            }
        }
        Ok(Self {
            inner,
            catalog,
            reader,
            refused,
            known,
            pending,
        })
    }

    /// The inner source, for a consumer that drives it.
    pub const fn inner(&mut self) -> &mut S {
        &mut self.inner
    }

    /// The catalog the guard judges against.
    pub const fn catalog(&self) -> &C {
        &self.catalog
    }

    /// Whether the table is refused.
    #[must_use]
    pub fn is_refused(&self, table: TableId) -> bool {
        self.refused.contains_key(&table)
    }

    /// The refused tables, each with its reasons, in id order.
    ///
    /// The read-only view the upload path asks before refusing an upload
    /// with the typed reason.
    pub fn refused_tables(&self) -> impl Iterator<Item = (TableId, &[Incompatibility])> {
        self.refused
            .iter()
            .map(|(id, reasons)| (*id, reasons.as_slice()))
    }
}

impl<S, C, R> CdcSource for DriftGuard<S, C, R>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike + SqlparserDatabase,
    R: LiveShapeReader,
{
    type Event = S::Event;
    type Commit = S::Commit;
    type Relation = DriftSignal;
    type Error = DriftGuardError<S::Error, R::Error>;

    fn next_item(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send
    {
        let this = self;
        async move {
            loop {
                if let Some(item) = this.pending.pop_front() {
                    return Ok(Some(item));
                }
                let pulled = this
                    .inner
                    .next_item()
                    .await
                    .map_err(DriftGuardError::Inner)?;
                let Some(item) = pulled else {
                    return Ok(None);
                };
                match item {
                    SourceItem::Relation(signal) => this.on_signal(&signal).await?,
                    SourceItem::Event(event) => {
                        let table = event.table_id(&this.catalog);
                        // A refused table's events are dropped as they
                        // arrive, and so are the events of a table the
                        // catalog does not know.
                        if this.known.contains(&table) && !this.refused.contains_key(&table) {
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
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        let inner = &mut self.inner;
        async move { inner.ack(upto).await.map_err(DriftGuardError::Inner) }
    }
}

impl<S, C, R> DriftGuard<S, C, R>
where
    S: CdcSource,
    S::Relation: DriftTableSignal,
    C: DatabaseLike + SqlparserDatabase,
    R: LiveShapeReader,
{
    /// One schema message, judged.
    ///
    /// A changes-lost message yields a resync item for the table unless
    /// it is already refused, whose [`DriftSignal::Refused`] item
    /// resyncs it. Both kinds of message then re-judge the table, since
    /// either can carry a definition change.
    async fn on_signal(
        &mut self,
        signal: &S::Relation,
    ) -> Result<(), DriftGuardError<S::Error, R::Error>> {
        let Some(table) = signal.table(&self.catalog) else {
            // A table the catalog does not know is not judged, and its
            // events are dropped by the loop.
            return Ok(());
        };
        if self.refused.contains_key(&table) {
            // A refused table stays refused until the guard is dropped.
            return Ok(());
        }
        if signal.changes_lost() {
            self.pending
                .push_back(SourceItem::Relation(DriftSignal::ChangesLost { table }));
        }
        self.judge_table(table).await?;
        Ok(())
    }

    /// Re-read the table's live shape and judge it.
    ///
    /// A refusal records the table in the refused set and yields its
    /// [`DriftSignal::Refused`] item; unchanged and allowed additions
    /// record nothing.
    async fn judge_table(
        &mut self,
        table: TableId,
    ) -> Result<(), DriftGuardError<S::Error, R::Error>> {
        let Some(name) = catalog_helpers::table_name(&self.catalog, table) else {
            return Ok(());
        };
        let live = self
            .reader
            .shape(&name)
            .await
            .map_err(DriftGuardError::Shape)?;
        let index = usize::try_from(table).ok();
        let Some(catalog_table) = index.and_then(|index| self.catalog.table_by_id(index)) else {
            return Ok(());
        };
        let Ok(catalog_shape) = TableShape::from_table(catalog_table, &self.catalog) else {
            return Ok(());
        };
        if let Some(reasons) = refusal_reasons(&catalog_shape, live.as_ref()) {
            self.pending
                .push_back(SourceItem::Relation(DriftSignal::Refused {
                    table,
                    reasons: reasons.clone(),
                }));
            self.refused.insert(table, reasons);
        }
        Ok(())
    }
}
