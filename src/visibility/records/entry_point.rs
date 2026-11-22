use alloc::vec::Vec;
use core::cell::Cell;

#[cfg(feature = "visibility-openfga")]
use rls2fga_types::ResultingRow;
use rls2fga_types::{
    records_from_row, ColumnKind, ColumnRead, Guard, Record, RecordDerivation, RecordDescription,
    RecordError, TypeName, ValueSource,
};
use rls2fga_types::{AttributeLiteral, AttributeOperator, AttributePredicate, AttributeTest};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{ScalarFamily, ScalarKindOf};
use crate::catalog_helpers;
use crate::visibility::RowView;
use crate::TableId;

use super::adapter::RowValuesView;
use super::RowRecordError;

/// The records `description` says `row` implies.
///
/// # Errors
///
/// [`RowRecordError::Refused`] for a description rls2fga will not evaluate,
/// [`RowRecordError::UnsupportedValueSource`] for a shape whose columns a
/// row view cannot read, and [`RowRecordError::Undecodable`] for a cell
/// the row carried but could not decode. None is an empty record set, on
/// purpose.
pub fn records_from_row_view<R, DB>(
    description: &RecordDescription,
    row: &R,
    db: &DB,
) -> Result<Vec<Record>, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    if let Some(refusal) = unsupported_description::<R::Backend, DB>(description, db) {
        return Err(refusal);
    }
    read_row(row, db, |view| records_from_row(description, view))
}

/// The records a check about the row a write produces carries for `row`, as
/// `resulting` states them.
///
/// # Errors
///
/// [`RowRecordError::Refused`] for a record rls2fga cannot produce from `row`,
/// and [`RowRecordError::Undecodable`] for a cell the row carried but could not
/// decode.
#[cfg(feature = "visibility-openfga")]
pub fn resulting_records<R, DB>(
    resulting: &ResultingRow,
    row: &R,
    db: &DB,
) -> Result<Vec<Record>, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    read_row(row, db, |view| resulting.records(view))
}

/// What `read` makes of `row`, refused where a cell it read could not be
/// decoded.
fn read_row<R, DB, T>(
    row: &R,
    db: &DB,
    read: impl FnOnce(&RowValuesView<'_, R, DB>) -> Result<T, RecordError>,
) -> Result<T, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    let view = RowValuesView {
        row,
        db,
        undecodable: Cell::new(None),
    };
    let read = read(&view)?;
    if let Some(error) = view.undecodable.into_inner() {
        return Err(RowRecordError::Undecodable(error));
    }
    Ok(read)
}

/// Whether [`records_from_row_view`] can evaluate `description` at all.
///
/// Depends only on the description's shape and the catalog, never on a
/// row, so a caller holding the descriptions for a whole schema settles
/// this once at setup rather than rediscovering the refusal on every
/// changed row.
#[must_use]
pub fn is_evaluable<B: crate::backend::Backend, DB: DatabaseLike>(
    description: &RecordDescription,
    db: &DB,
) -> bool {
    unsupported_description::<B, DB>(description, db).is_none()
}

/// The type whose every member `description` grants, when it states one
/// unconditional wildcard record per row it admits.
///
/// [`None`] for a named subject, and for a wildcard under a condition, since
/// the request completes that one and taking it as everyone is a wrong allow.
#[must_use]
pub fn grants_everyone(description: &RecordDescription) -> Option<&TypeName> {
    match &description.derivation {
        RecordDerivation::FromRow { template, .. }
            if template.subject_key.is_wildcard() && template.context.is_none() =>
        {
            Some(&template.subject_type)
        }
        _ => None,
    }
}

/// Why a row view cannot answer `description`, or [`None`].
fn unsupported_description<B: crate::backend::Backend, DB: DatabaseLike>(
    description: &RecordDescription,
    db: &DB,
) -> Option<RowRecordError> {
    let RecordDerivation::FromRow {
        table,
        template,
        guards,
        ..
    } = &description.derivation
    else {
        return None;
    };
    let Some(table) = catalog_helpers::contract_table_id(db, table) else {
        return Some(RowRecordError::UnreadableColumn(alloc::format!(
            "any column of {table:?}, a table the catalog does not know"
        )));
    };
    template
        .object_key
        .parts()
        .iter()
        .find_map(|part| unsupported_value::<B, DB>(part, db, table))
        .or_else(|| {
            template
                .subject_key
                .parts()
                .find_map(|part| unsupported_value::<B, DB>(part, db, table))
        })
        .or_else(|| {
            guards
                .iter()
                .find_map(|guard| unsupported_guard::<B, DB>(guard, db, table))
        })
        .or_else(|| {
            template.context.as_ref().and_then(|context| {
                context
                    .entries
                    .iter()
                    .find_map(|entry| unsupported_value::<B, DB>(&entry.value, db, table))
            })
        })
}

fn unsupported_value<B: crate::backend::Backend, DB: DatabaseLike>(
    source: &ValueSource,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match source {
        ValueSource::Column(column) => direct_column_read::<B, DB>(column, db, table),
        ValueSource::JsonPath { column, .. } => {
            expect_column_kind::<B, DB>(column, db, table, ColumnKind::Json)
        }
        ValueSource::Literal(_) => None,
        ValueSource::ListElements(column) => match element_read_kind::<B, DB>(column, db, table) {
            Ok(kind) => direct_kind_read(column, kind),
            Err(refusal) => Some(refusal),
        },
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised")),
    }
}

fn unsupported_guard<B: crate::backend::Backend, DB: DatabaseLike>(
    guard: &Guard,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match guard {
        Guard::NotNull(column) => direct_column_read::<B, DB>(column, db, table),
        Guard::IsTrue(column) => expect_column_kind::<B, DB>(column, db, table, ColumnKind::Bool),
        Guard::Compare { column, predicate } => {
            comparison_read::<B, DB>(column, predicate, db, table)
        }
        Guard::CompareColumns {
            column,
            operator,
            other,
        } => column_comparison_read::<B, DB>(column, *operator, other, db, table),
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised guard")),
    }
}

fn column_comparison_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    operator: AttributeOperator,
    other: &ColumnRead,
    database: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, database, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    let other_kind = match column_read_kind::<B, DB>(other, database, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    match (kind, other_kind) {
        (ColumnKind::Bool, ColumnKind::Bool)
        | (ColumnKind::Integer | ColumnKind::Decimal, ColumnKind::Integer | ColumnKind::Decimal) => {
            None
        }
        (ColumnKind::Text, ColumnKind::Text)
            if matches!(operator, AttributeOperator::Eq | AttributeOperator::NotEq) =>
        {
            None
        }
        (ColumnKind::Text, ColumnKind::Text) => Some(RowRecordError::UnreadableColumn(
            alloc::format!("comparison between columns {column} and {other} needs a query"),
        )),
        _ => Some(RowRecordError::UnsupportedValueSource(
            "unrecognised comparison",
        )),
    }
}

fn comparison_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    predicate: &AttributePredicate,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    match &predicate.test {
        AttributeTest::Compare { operator, value } => {
            literal_comparison_read(column, kind, *operator, value)
        }
        AttributeTest::CompareColumn { .. } => Some(RowRecordError::UnsupportedValueSource(
            "unrecognised comparison",
        )),
        AttributeTest::In(values)
            if matches!(
                kind,
                ColumnKind::Bool | ColumnKind::Integer | ColumnKind::Decimal | ColumnKind::Text
            ) =>
        {
            values.iter().find_map(|value| {
                literal_comparison_read(column, kind, AttributeOperator::Eq, value)
            })
        }
        AttributeTest::In(_) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "membership over column {column} needs a query"
        ))),
        AttributeTest::IsNull | AttributeTest::IsNotNull => direct_kind_read(column, kind),
    }
}

fn literal_comparison_read(
    column: &ColumnRead,
    kind: ColumnKind,
    operator: AttributeOperator,
    value: &AttributeLiteral,
) -> Option<RowRecordError> {
    match (value, kind) {
        (AttributeLiteral::Boolean(_), ColumnKind::Bool)
        | (AttributeLiteral::Number(_), ColumnKind::Integer | ColumnKind::Decimal) => None,
        (AttributeLiteral::Text(_), ColumnKind::Text)
            if matches!(operator, AttributeOperator::Eq | AttributeOperator::NotEq) =>
        {
            None
        }
        (AttributeLiteral::Text(_), ColumnKind::Text) => Some(RowRecordError::UnreadableColumn(
            alloc::format!("comparison over column {column} needs a query"),
        )),
        (
            AttributeLiteral::Boolean(_) | AttributeLiteral::Number(_) | AttributeLiteral::Text(_),
            _,
        ) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} cannot be compared as {value:?}"
        ))),
        _ => Some(RowRecordError::UnsupportedValueSource(
            "unrecognised comparison",
        )),
    }
}

fn direct_column_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => direct_kind_read(column, kind),
        Err(refusal) => Some(refusal),
    }
}

fn direct_kind_read(column: &ColumnRead, kind: ColumnKind) -> Option<RowRecordError> {
    if direct_kind(kind) {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} has unsupported kind {kind:?}"
        )))
    }
}

fn expect_column_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
    expected: ColumnKind,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if kind == expected {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {kind:?}, not {expected:?}"
        )))
    }
}

pub(super) const fn direct_kind(kind: ColumnKind) -> bool {
    !matches!(kind, ColumnKind::Json | ColumnKind::Unsupported)
}

fn column_read_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Result<ColumnKind, RowRecordError> {
    declared_read_kind::<B, DB>(
        column,
        db,
        table,
        catalog_helpers::column_scalar_kind::<B, DB>,
    )
}

/// The kind of each element of the array `column` reads, checked as
/// [`column_read_kind`] checks a scalar.
fn element_read_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Result<ColumnKind, RowRecordError> {
    declared_read_kind::<B, DB>(
        column,
        db,
        table,
        catalog_helpers::column_array_element_kind::<B, DB>,
    )
}

fn declared_read_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
    declared: fn(&DB, TableId, crate::ColumnId) -> Option<ScalarKindOf<B>>,
) -> Result<ColumnKind, RowRecordError> {
    let refuse = || {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column}, which the catalog does not know or cannot type"
        ))
    };
    let id = catalog_helpers::contract_column_id::<B, _>(db, table, column.as_str())
        .ok_or_else(refuse)?;
    let scalar = declared(db, table, id).ok_or_else(refuse)?;
    // A custom type answers `None`: it has no renderable column kind, so a
    // shape that reads this column is refused rather than served a spelling
    // subql cannot prove (R1).
    let actual = column_kind_from_scalar::<B>(scalar).ok_or_else(|| {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} holds a custom type, which has no spelling this side can prove"
        ))
    })?;
    if actual == column.kind() {
        Ok(actual)
    } else {
        Err(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {actual:?} in the catalog but the shape reads {:?}",
            column.kind()
        )))
    }
}

pub(super) fn column_kind_from_scalar<B: crate::backend::Backend>(
    kind: ScalarKindOf<B>,
) -> Option<ColumnKind> {
    // A custom type has no column kind here on purpose: rendering it would
    // mean asserting a text form subql cannot prove matches the loading SQL
    // (R1), so a shape that renders such a column is reported uncovered.
    Some(match kind.family()? {
        ScalarFamily::Bool => ColumnKind::Bool,
        ScalarFamily::Int => ColumnKind::Integer,
        ScalarFamily::Float | ScalarFamily::Interval => ColumnKind::Unsupported,
        ScalarFamily::String => ColumnKind::Text,
        ScalarFamily::Bytes => ColumnKind::Bytea,
        ScalarFamily::Uuid => ColumnKind::Uuid,
        ScalarFamily::Timestamp => ColumnKind::Timestamp,
        ScalarFamily::TimestampTz => ColumnKind::TimestampTz,
        ScalarFamily::Date => ColumnKind::Date,
        ScalarFamily::Time => ColumnKind::Time,
        ScalarFamily::Decimal => ColumnKind::Decimal,
        ScalarFamily::Json | ScalarFamily::Jsonb => ColumnKind::Json,
    })
}
