//! One cell of one row image that refuses to decode, the shape the change
//! stream takes after a column's declared type changes under it. Every
//! other cell decodes, which is what lets a test stop exactly one
//! subscription and watch its siblings keep folding.

use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Postgres, RowKind, ScalarFamily, Value};
use subql::{
    catalog_helpers, ColumnId, DefaultIds, EventKind, SubscriptionEngine, TableId, ValueError,
};

/// An event wrapper with one undecodable cell. With `altered` clear the
/// wrapper is a pass-through, so the same event type serves the clean
/// dispatches around the failing one.
pub struct AlteredCell<E> {
    inner: E,
    /// The row image carrying the undecodable cell.
    row: RowKind,
    /// The column that refuses to decode.
    column: ColumnId,
    /// The family the cell no longer decodes into.
    family: ScalarFamily,
    /// Whether the named cell refuses to decode.
    altered: bool,
}

impl<E: CdcEvent> AlteredCell<E> {
    pub const fn new(
        inner: E,
        row: RowKind,
        column: ColumnId,
        family: ScalarFamily,
        altered: bool,
    ) -> Self {
        Self {
            inner,
            row,
            column,
            family,
            altered,
        }
    }
}

impl<E: CdcEvent> CdcEvent for AlteredCell<E> {
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
        if self.altered && row == self.row && col == self.column {
            return Err(ValueError::Builtin {
                column: col,
                kind: self.family,
            });
        }
        self.inner.value_at(db, row, col)
    }
}

/// One `orders` engine over a Postgres-cataloged table, for whatever event
/// type a test wraps `TestEvent` into.
pub fn postgres_engine<E: CdcEvent<Backend = Postgres>>(
    ddl: &str,
) -> (SubscriptionEngine<E, DefaultIds, ParserDB>, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).expect("parse DDL");
    let table =
        catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        table,
    )
}
