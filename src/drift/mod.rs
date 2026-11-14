//! Drift judgment: one table's live shape against the consumer's catalog.
//!
//! A subql consumer holds a fixed catalog and a live database that can
//! change under it. A [`shape::TableShape`] captures what one side says
//! about one table, columns in order with their declared type, key
//! columns, `CHECK` expressions and replica identity where the backend
//! has one. [`verdict::judge`] compares a live shape, or a missing live
//! table, against the catalog's and answers unchanged, allowed
//! additions, or incompatible with every reason, so one table's change
//! names itself instead of stopping the whole stream.
//!
//! The [`guard::DriftGuard`] behind `std` wraps a [`crate::CdcSource`]
//! and applies the judgment. It drops the events of refused tables and
//! surfaces one item per newly refused table and per changes-lost table,
//! so the fold, dispatch and apply never meet a table the consumer
//! cannot read. The apply entry points ask
//! the guard's refused set through a [`refusal::RefusalView`], so an
//! upload naming a refused table is refused with the typed reason before
//! any op applies. The [`live` module](live) turns a connection that loads
//! its own catalog into the guard's live reader, and the
//! [`sqlite` module](sqlite) implements that load for SQLite behind the
//! `sqlite-cdc` feature.

#[cfg(feature = "std")]
pub mod guard;
#[cfg(feature = "std")]
pub mod live;
pub mod refusal;
pub mod shape;
#[cfg(feature = "sqlite-cdc")]
pub mod sqlite;
pub mod verdict;

#[cfg(feature = "std")]
pub use guard::{
    DriftGuard, DriftGuardError, DriftSignal, DriftTableSignal, LiveShapeReader, RefusedTables,
};
#[cfg(feature = "pg-streaming")]
pub use live::{BlockingLiveReader, BlockingReadError};
#[cfg(feature = "std")]
pub use live::{InlineLiveReader, LiveCatalog};
pub use refusal::{NoRefusals, RefusalView};
pub use shape::{ColumnShape, Publication, PublishedChange, ReplicaIdentity, TableShape};
#[cfg(feature = "sqlite-cdc")]
pub use sqlite::SqliteShapeError;
pub use verdict::{judge, refusal_reasons, Incompatibility, Verdict};
