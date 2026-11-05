//! The view the apply path asks of the refused tables.
//!
//! A [`crate::DriftGuard`] judges the live schema against the catalog and
//! keeps the tables it refuses. The apply entry points take a
//! [`RefusalView`] as an explicit parameter, so an upload naming a refused
//! table is refused as a whole, with the typed reasons, before any op
//! applies. The core carries the trait and the empty [`NoRefusals`] view;
//! the guard's shared handle, [`RefusedTables`](crate::drift::guard::RefusedTables),
//! is behind `std` with the guard.

use alloc::vec::Vec;

use super::verdict::Incompatibility;

/// A view of the tables a [`crate::DriftGuard`] has refused, which the
/// apply entry points consult before applying an upload.
///
/// The view is keyed by table name, since an uploaded diffset names its
/// tables by the text the SQLite session extension wrote, and the check
/// runs before any op's shape is resolved against the catalog. The
/// reasons come back owned, since the view's state is a snapshot a read
/// holds only for the call.
pub trait RefusalView {
    /// The reasons `table` is refused, or `None` when the view does not
    /// refuse it.
    fn refusal(&self, table: &str) -> Option<Vec<Incompatibility>>;
}

/// A view with no refused tables, for a consumer that applies uploads
/// without a [`crate::DriftGuard`].
#[derive(Clone, Copy, Debug, Default)]
pub struct NoRefusals;

impl RefusalView for NoRefusals {
    fn refusal(&self, _table: &str) -> Option<Vec<Incompatibility>> {
        None
    }
}
