//! Pull-based Postgres CDC source driven by `pg_walstream`'s typed
//! logical replication stream.
//!
//! Owns a `pg_walstream::LogicalReplicationStream` bound to the
//! pre-existing replication slot. Each row change surfaces as a
//! [`crate::PgChangeEvent`] through the [`crate::CdcSource`] trait,
//! placed in commit order by its transaction's commit position and its
//! ordinal in that transaction, and then the transaction's
//! [`crate::PgCommit`]. Acks flow back so the slot's
//! `confirmed_flush_lsn` moves to the end of each commit the consumer
//! has acknowledged, and no further.

use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use pg_walstream::error::ReplicationError;
use pg_walstream::{
    LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, SharedLsnFeedback,
};
use sql_traits::prelude::DatabaseLike;
use tokio_util::sync::CancellationToken;

use super::{
    PgChangeEvent, PgCommit, PgOutputOrder, PgSourceItem, ReleaseQueue, SourceItem,
    TransactionOrderError,
};
use crate::{PgCommitPosition, PgLsn};

mod wire_format_helpers;
use wire_format_helpers::ensure_replication_param;

/// Configuration for a [`PgStreamingCdcSource`].
///
/// The caller creates the publication and the replication slot before
/// constructing the source, and `connect` auto-creates neither.
///
/// `#[non_exhaustive]` so future fields can be added without breaking
/// downstream call sites. Construct via [`PgStreamingConfig::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PgStreamingConfig {
    /// libpq connection string (URL form or key=value form). The source
    /// appends `?replication=database` if the caller did not.
    pub url: String,
    /// Name of the pre-created logical replication slot
    /// (`pg_create_logical_replication_slot(slot, 'pgoutput')`).
    pub slot_name: String,
    /// Name of the publication the slot should follow
    /// (`CREATE PUBLICATION pub FOR TABLE ...`).
    pub publication_name: String,
    /// Optional resume position. The source delivers only the items
    /// after it, and `None` starts from the slot's current
    /// `confirmed_flush_lsn`.
    ///
    /// Resuming after a row of a transaction redelivers the later rows
    /// of that transaction and its commit.
    pub start: Option<PgCommitPosition>,
    /// Minimum interval between changed-LSN feedback checks while the reader is polled.
    pub status_interval: Duration,
}

impl PgStreamingConfig {
    /// Build a config with the defaults `start = None` and
    /// `status_interval = 10s`.
    #[must_use]
    pub fn new(
        url: impl Into<String>,
        slot_name: impl Into<String>,
        publication_name: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            slot_name: slot_name.into(),
            publication_name: publication_name.into(),
            start: None,
            status_interval: Duration::from_secs(10),
        }
    }

    /// Override the resume position. `None` means the slot's current
    /// `confirmed_flush_lsn`.
    #[must_use]
    pub const fn start(mut self, start: Option<PgCommitPosition>) -> Self {
        self.start = start;
        self
    }

    /// Override the periodic feedback cadence (default 10s).
    #[must_use]
    pub const fn status_interval(mut self, status_interval: Duration) -> Self {
        self.status_interval = status_interval;
        self
    }
}

/// Errors surfaced by [`PgStreamingCdcSource`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgStreamingError {
    /// Underlying `pg_walstream` failure (transport, auth, server error).
    #[error("postgres error: {0}")]
    Postgres(#[from] ReplicationError),
    /// The server's response to a replication command did not match what
    /// the spec mandates, such as an unreadable `IDENTIFY_SYSTEM` row.
    #[error("replication protocol error: {0}")]
    Protocol(String),
    /// The source ended, either dropped or after a terminal reader,
    /// ordering, or feedback error.
    #[error("streaming source shut down")]
    SourceClosed,
    /// The stream's transaction frames were out of place, so its rows
    /// could not be placed in commit order.
    #[error("replication stream out of order: {0}")]
    TransactionOrder(#[from] TransactionOrderError),
}

/// One point where a later timeline branched from `timeline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineSwitch {
    /// The ancestor timeline, the history line's first field.
    pub timeline: u32,
    /// Position where the branch left the ancestor.
    pub switch_lsn: PgLsn,
}

/// The history a [`PgStreamingCdcSource`] streams from, which a WAL
/// position is only meaningful within.
///
/// A promoted point-in-time restore keeps `system_id` and moves to a new
/// `timeline` branching below earlier positions, while a dump restored
/// into a fresh cluster changes `system_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterIdentity {
    /// `IDENTIFY_SYSTEM`'s `systemid`.
    pub system_id: u64,
    /// `IDENTIFY_SYSTEM`'s `timeline`, the one the server is writing.
    pub timeline: u32,
    /// One entry per ancestor timeline in ascending order, empty on timeline 1.
    pub history: Vec<TimelineSwitch>,
}

/// Pull-based Postgres CDC source whose caller must keep polling for idle keepalive replies.
pub struct PgStreamingCdcSource {
    config: PgStreamingConfig,
    /// The owned stream. Dropping it closes the replication socket.
    reader: LogicalReplicationStream,
    /// Cancellation token required by the reader's typed API.
    cancel: CancellationToken,
    delivery: Delivery,
    /// Delivered row events counted since `connect`.
    events_received: u64,
    /// The slot's `confirmed_flush_lsn` at `connect`, the retention
    /// gauges' lower bound and the feedback tracker's seed.
    base_lsn: u64,
    /// The released commit end last reported as applied, seeded at the
    /// slot's own position.
    acked_lsn: u64,
    /// Whether a caller has acknowledged anything, which the position
    /// alone cannot say now that it starts at the slot's own.
    acked_seen: bool,
    /// Set on a terminal reader, ordering, or feedback error.
    terminal: bool,
    cluster_identity: ClusterIdentity,
}

impl PgStreamingCdcSource {
    /// Read identity, timeline history and the slot's confirmed flush seed through the owned replication connection, then start streaming.
    ///
    /// # Pre-conditions
    ///
    /// - The publication `config.publication_name` must already exist.
    /// - The logical replication slot `config.slot_name` must already
    ///   exist with the `pgoutput` plugin.
    /// - `config.url` is a libpq conninfo string. The source appends
    ///   `replication=database` if the caller did not.
    ///
    /// The engine resolves events against its own catalog at dispatch time, so this constructor releases `catalog` before creating its future.
    ///
    /// # Errors
    ///
    /// Returns an error if connection setup, identity discovery, slot lookup or replication startup fails.
    pub fn connect<DB: DatabaseLike + 'static>(
        config: PgStreamingConfig,
        catalog: DB,
    ) -> impl core::future::Future<Output = Result<Self, PgStreamingError>> {
        drop(catalog);
        async move {
            let conninfo = ensure_replication_param(&config.url);

            // One connection attempt. The consumer owns reconnection.
            let stream_config = ReplicationStreamConfig::builder(
                config.slot_name.clone(),
                config.publication_name.clone(),
            )
            .with_protocol_version(1)
            .with_feedback_interval(config.status_interval)
            .with_retry_config(RetryConfig {
                max_attempts: 1,
                ..RetryConfig::default()
            })
            .with_existing_slot_required(true);

            let reader = LogicalReplicationStream::new(&conninfo, stream_config).await?;
            Self::start_reader(config, reader).await
        }
    }

    // Separate initialization bounds the public constructor's future frame.
    async fn start_reader(
        config: PgStreamingConfig,
        mut reader: LogicalReplicationStream,
    ) -> Result<Self, PgStreamingError> {
        let start = config.start;
        let identity = reader.server_identity();
        let system_id = identity.system_id();
        let timeline = identity.timeline();
        let history = reader
            .timeline_history()
            .await?
            .into_iter()
            .map(|switch| TimelineSwitch {
                timeline: switch.timeline(),
                switch_lsn: PgLsn(switch.switchpoint().value()),
            })
            .collect();
        // The slot position seeds the distance gauges rather than an absolute LSN.
        let base_lsn = reader
            .slot_confirmed_flush_lsn()
            .await?
            .map_or(0, |lsn| lsn.value());
        let cluster_identity = ClusterIdentity {
            system_id,
            timeline,
            history,
        };
        reader
            .start(start.map(|position| position.commit_lsn().0))
            .await?;

        // A row-ordinal resume does not acknowledge its transaction.
        reader.shared_lsn_feedback = SharedLsnFeedback::new_shared();
        reader.shared_lsn_feedback.update_applied_lsn(base_lsn);
        Ok(Self {
            config,
            reader,
            cancel: CancellationToken::new(),
            delivery: Delivery::new(start),
            events_received: 0,
            base_lsn,
            acked_lsn: base_lsn,
            acked_seen: false,
            terminal: false,
            cluster_identity,
        })
    }

    /// Cumulative number of [`PgChangeEvent`]s the source has delivered
    /// since `connect`. Symmetric with
    /// [`crate::polling::PollingPgCdcSource::events_received`].
    #[must_use]
    pub const fn events_received(&self) -> u64 {
        self.events_received
    }

    /// Applied position queued for reader feedback, or `None` until the first acknowledgement.
    #[must_use]
    pub fn acknowledged_position(&self) -> Option<PgLsn> {
        self.acked_seen.then_some(PgLsn(self.acked_lsn))
    }

    /// Distance from received WAL to locally acknowledged progress, seeded at the slot's confirmed flush position.
    #[must_use]
    pub fn unacknowledged_bytes(&self) -> u64 {
        self.reader
            .current_lsn()
            .max(self.base_lsn)
            .saturating_sub(self.acked_lsn)
    }

    /// Borrow the configuration the source was built with.
    #[must_use]
    pub const fn config(&self) -> &PgStreamingConfig {
        &self.config
    }

    /// The cluster identity read at `connect`, which a fresh `connect` re-reads.
    #[must_use]
    pub const fn cluster_identity(&self) -> &ClusterIdentity {
        &self.cluster_identity
    }

    /// Queue a released commit end for the reader's feedback.
    fn publish_applied(&mut self, end: PgLsn) {
        self.acked_lsn = self.acked_lsn.max(end.0);
        self.reader.shared_lsn_feedback.update_applied_lsn(end.0);
    }
}

impl crate::CdcSource for PgStreamingCdcSource {
    type Event = PgChangeEvent;
    type Commit = PgCommit;
    type Error = PgStreamingError;

    async fn next_item(&mut self) -> Result<Option<PgSourceItem>, Self::Error> {
        loop {
            if let Some(row) = self.delivery.next_row() {
                self.events_received += 1;
                return Ok(Some(SourceItem::Event(row)));
            }
            if let Some((commit, released)) = self.delivery.next_commit() {
                if !self.terminal {
                    if let Some(end) = released {
                        self.publish_applied(end);
                    }
                }
                return Ok(Some(SourceItem::Commit(commit)));
            }
            if self.terminal {
                return Err(PgStreamingError::SourceClosed);
            }
            let staged = match self.reader.next_event(&self.cancel).await {
                Ok(change) => self.delivery.stage(change).map_err(PgStreamingError::from),
                Err(error) => Err(PgStreamingError::from(error)),
            };
            if let Err(error) = staged {
                self.terminal = true;
                return Err(error);
            }
        }
    }

    async fn ack(&mut self, upto: PgCommitPosition) -> Result<(), Self::Error> {
        if self.terminal {
            return Err(PgStreamingError::SourceClosed);
        }
        if let Some(end) = self.delivery.acknowledge(upto) {
            self.publish_applied(end);
            if let Err(error) = self.reader.send_feedback().await {
                self.terminal = true;
                return Err(error.into());
            }
        }
        self.acked_seen = true;
        Ok(())
    }
}

/// Row delivery for the source. The transaction frame, the resume filter,
/// the staged fanout of one decoded event, and the commits the slot still
/// holds.
struct Delivery {
    order: PgOutputOrder,
    rows: VecDeque<PgChangeEvent>,
    commit: Option<PgCommit>,
    releases: ReleaseQueue,
    start: Option<PgCommitPosition>,
}

impl Delivery {
    const fn new(start: Option<PgCommitPosition>) -> Self {
        Self {
            order: PgOutputOrder::new(),
            rows: VecDeque::new(),
            commit: None,
            releases: ReleaseQueue::new(),
            start,
        }
    }

    /// Place `change` in the transaction frame and stage its fanout, the
    /// rows first and a commit ending a transaction after them.
    fn stage(&mut self, change: pg_walstream::ChangeEvent) -> Result<(), TransactionOrderError> {
        self.order
            .apply(change, &mut self.rows)
            .map(|committed| self.commit = committed)
    }

    /// The next staged row past the resume filter, if any.
    fn next_row(&mut self) -> Option<PgChangeEvent> {
        while let Some(row) = self.rows.pop_front() {
            if self.unseen(row.position()) {
                return Some(row);
            }
        }
        None
    }

    /// The staged commit, if any, registered with the release queue when
    /// past the resume filter, alongside the flush end its delivery
    /// releases.
    fn next_commit(&mut self) -> Option<(PgCommit, Option<PgLsn>)> {
        let commit = self.commit.take()?;
        if !self.unseen(commit.position()) {
            return None;
        }
        self.releases.committed(commit);
        // The consumer may have acknowledged past this commit before it
        // was returned.
        Some((commit, self.releases.release()))
    }

    /// Record the consumer's acknowledgement, returning the flush end it
    /// releases, if any.
    fn acknowledge(&mut self, upto: PgCommitPosition) -> Option<PgLsn> {
        self.releases.acknowledge(upto);
        self.releases.release()
    }

    fn unseen(&self, position: PgCommitPosition) -> bool {
        self.start.is_none_or(|start| position > start)
    }
}
