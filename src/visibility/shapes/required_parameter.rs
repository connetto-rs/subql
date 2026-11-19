use alloc::string::String;
use alloc::vec::Vec;

use rls2fga_types::{BoundQuery, RecordDescription, RequestNormalization};

use crate::ColumnId;

/// A condition parameter every question's context has to carry.
///
/// rls2fga reports these because a model carrying a request-gated grant is
/// unusable without them: a question that omits one is refused by the service
/// outright rather than answered no.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredParameter {
    /// The name a context uses for it.
    pub parameter: String,
    /// The session setting it mirrors, so a watcher can be asked for it.
    /// [`None`] means no watcher can answer it, which today is the clock.
    pub setting_key: Option<String>,
    /// Whether it holds a set rather than one value, read off the note's own
    /// list report, since a JSON-array set names no separator to read it off.
    pub list: bool,
    /// The function the policy applies to the setting's value. A watcher
    /// sends that function's result under [`parameter`](Self::parameter), as
    /// the database the policies run in computes it, since the result follows
    /// that database's collation.
    pub normalization: Option<RequestNormalization>,
}

impl RequiredParameter {
    /// Whether a watcher can be asked for this one.
    #[must_use]
    pub const fn watcher_supplied(&self) -> bool {
        self.setting_key.is_some()
    }
}

// TableShapes

/// Shapes reaching one table's rows, resolved to subql ids once.
#[derive(Debug, Default)]
pub struct TableShapes {
    /// Shapes whose records the difference reads off a row of this table.
    ///
    /// Excludes every shape a group maintains. A group's reconcile is the one
    /// authoritative operation over its region, and differencing beside it
    /// would be a second one: the difference reports what *this* producer
    /// stopped stating, which deletes a fact a sibling producer still states.
    pub differenced: Vec<RecordDescription>,
    /// Queries to replay when a row of this table changes, with the columns
    /// each one binds, in the order its placeholders take them.
    ///
    /// Empty for a shape under a group's authority, for the same reason.
    pub requeries: Vec<(Vec<ColumnId>, BoundQuery)>,
    /// Groups a change to this table obliges, as positions in
    /// [`Shapes::materialisations`](crate::visibility::shapes::Shapes::materialisations).
    pub groups: Vec<usize>,
}
