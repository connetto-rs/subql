//! Which interval arithmetic is computed in process, and at which kinds its
//! operands and result are read.
//!
//! PostgreSQL's operators, as it resolves them: an untyped quoted literal
//! beside a timestamp, a time or an interval is an interval, except after a
//! timestamp's `-`, where it is the timestamp being subtracted. A
//! `timestamptz` moves by an interval's months and days in the session's time
//! zone, which subql does not know, so it is moved in process only by a
//! literal interval with neither.

use crate::backend::{Backend, Interval, ScalarFamily};
use crate::compiler::literals::{interval_literal, resolve_column_ref, value_column};
use crate::compiler::sql_shape;
use crate::{RegisterError, TableId};
use alloc::string::ToString;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue, ValueWithSpan};

/// The kinds one temporal operation reads and answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TemporalPlan {
    /// The kind the left operand's literals are read at.
    pub left: ScalarFamily,
    /// The kind the right operand's literals are read at.
    pub right: ScalarFamily,
    /// The kind the operation answers.
    pub result: ScalarFamily,
}

/// What one operand of an arithmetic operation is.
#[derive(Clone, Copy, Debug)]
enum Operand {
    /// A value of a date, time or interval kind. `literal` holds a literal
    /// interval's value, which decides whether a `timestamptz` can move by it.
    Temporal {
        kind: ScalarFamily,
        literal: Option<Interval>,
    },
    /// A number of this kind.
    Number(ScalarFamily),
    /// An untyped quoted literal or `NULL`, typed by its operator. A quoted
    /// literal that reads as an interval carries that value.
    Unknown(Option<Interval>),
    /// Anything else, which no temporal operator takes.
    Other,
}

const fn is_temporal(kind: ScalarFamily) -> bool {
    matches!(
        kind,
        ScalarFamily::Interval
            | ScalarFamily::Timestamp
            | ScalarFamily::TimestampTz
            | ScalarFamily::Date
            | ScalarFamily::Time
    )
}

fn operand<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    depth: usize,
) -> Operand {
    if depth >= sql_shape::MAX_EXPR_DEPTH {
        return Operand::Other;
    }
    if let Some(column) = resolve_column_ref::<B, DB>(value_column(expr), table_id, database) {
        return match crate::catalog_helpers::column_scalar_kind::<B, DB>(database, table_id, column)
            .and_then(|kind| kind.family())
        {
            Some(kind) if is_temporal(kind) => Operand::Temporal {
                kind,
                literal: None,
            },
            Some(kind @ (ScalarFamily::Int | ScalarFamily::Float | ScalarFamily::Decimal)) => {
                Operand::Number(kind)
            }
            _ => Operand::Other,
        };
    }
    match expr {
        Expr::Interval(interval) => Operand::Temporal {
            kind: ScalarFamily::Interval,
            literal: interval_literal(interval).ok(),
        },
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Number(digits, _) if digits.contains(['.', 'e', 'E']) => {
                Operand::Number(ScalarFamily::Float)
            }
            SqlValue::Number(..) => Operand::Number(ScalarFamily::Int),
            SqlValue::SingleQuotedString(text) => Operand::Unknown(Interval::parse(text)),
            SqlValue::Null => Operand::Unknown(None),
            _ => Operand::Other,
        },
        Expr::Nested(inner) => operand::<B, DB>(inner, table_id, database, depth + 1),
        // Only an interval and a number have a sign to flip, and PostgreSQL
        // has no unary `+ interval` operator, so a plus over an interval is
        // `Other` below.
        Expr::UnaryOp {
            op: sign @ (UnaryOperator::Plus | UnaryOperator::Minus),
            expr: inner,
        } => match operand::<B, DB>(inner, table_id, database, depth + 1) {
            Operand::Temporal {
                kind: ScalarFamily::Interval,
                literal,
            } if matches!(sign, UnaryOperator::Minus) => Operand::Temporal {
                kind: ScalarFamily::Interval,
                literal: literal.and_then(Interval::checked_neg),
            },
            number @ Operand::Number(_) => number,
            _ => Operand::Other,
        },
        Expr::BinaryOp { left, op, right } => {
            match plan::<B, DB>(op, left, right, table_id, database, depth + 1) {
                Ok(Some(plan)) => Operand::Temporal {
                    kind: plan.result,
                    literal: None,
                },
                Ok(None) => numeric_operation::<B, DB>(left, right, table_id, database, depth),
                Err(_) => Operand::Other,
            }
        }
        _ => Operand::Other,
    }
}

/// Arithmetic over numbers alone, typed by its first number, or `Other`.
fn numeric_operation<B: Backend, DB: DatabaseLike>(
    left: &Expr,
    right: &Expr,
    table_id: TableId,
    database: &DB,
    depth: usize,
) -> Operand {
    match (
        operand::<B, DB>(left, table_id, database, depth + 1),
        operand::<B, DB>(right, table_id, database, depth + 1),
    ) {
        (Operand::Number(kind), Operand::Number(_) | Operand::Unknown(None))
        | (Operand::Unknown(None), Operand::Number(kind)) => Operand::Number(kind),
        _ => Operand::Other,
    }
}

/// The temporal operation `left op right` is, `None` when neither operand is
/// a date, time or interval.
///
/// # Errors
///
/// [`RegisterError::UnsupportedSql`] for a temporal operation computed only
/// by the database: one PostgreSQL has no operator for or cannot resolve,
/// one over an engine without an interval, and a `timestamptz` moved by an
/// interval its time zone could move.
pub(super) fn plan<B: Backend, DB: DatabaseLike>(
    op: &BinaryOperator,
    left: &Expr,
    right: &Expr,
    table_id: TableId,
    database: &DB,
    depth: usize,
) -> Result<Option<TemporalPlan>, RegisterError> {
    if !matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    ) {
        return Ok(None);
    }
    let (left, right) = (
        operand::<B, DB>(left, table_id, database, depth),
        operand::<B, DB>(right, table_id, database, depth),
    );
    if left.kind().is_none() && right.kind().is_none() {
        return Ok(None);
    }
    let resolved = if B::HAS_INTERVAL {
        match op {
            BinaryOperator::Plus => {
                sum(left, right).or_else(|| sum(right, left).map(TemporalPlan::swapped))
            }
            BinaryOperator::Minus => difference(left, right),
            BinaryOperator::Multiply => {
                scaled(left, right).or_else(|| scaled(right, left).map(TemporalPlan::swapped))
            }
            BinaryOperator::Divide => scaled(left, right),
            _ => None,
        }
    } else {
        None
    };
    resolved.map(Some).ok_or_else(|| {
        RegisterError::UnsupportedSql(
            "this arithmetic over a date, time or interval is read from the database".to_string(),
        )
    })
}

impl TemporalPlan {
    const fn new(left: ScalarFamily, right: ScalarFamily, result: ScalarFamily) -> Self {
        Self {
            left,
            right,
            result,
        }
    }

    /// The same operation with its operands written the other way round.
    const fn swapped(self) -> Self {
        Self::new(self.right, self.left, self.result)
    }
}

impl Operand {
    /// The date, time or interval kind this operand is.
    const fn kind(self) -> Option<ScalarFamily> {
        match self {
            Self::Temporal { kind, .. } => Some(kind),
            _ => None,
        }
    }

    /// Whether PostgreSQL reads this operand as an interval beside a date,
    /// time or interval.
    const fn is_interval(self) -> bool {
        matches!(
            self,
            Self::Temporal {
                kind: ScalarFamily::Interval,
                ..
            } | Self::Unknown(_)
        )
    }

    /// Whether this operand is a literal interval a `timestamptz` moves by
    /// alike in every time zone, one without months or days.
    fn is_zoneless(self) -> bool {
        match self {
            Self::Temporal {
                kind: ScalarFamily::Interval,
                literal,
            }
            | Self::Unknown(literal) => {
                literal.is_some_and(|literal| literal.months() == 0 && literal.days() == 0)
            }
            _ => false,
        }
    }
}

/// `moved + by`, in the operand order PostgreSQL's `+` lists first.
fn sum(moved: Operand, by: Operand) -> Option<TemporalPlan> {
    use ScalarFamily as F;
    match moved.kind()? {
        F::Interval if by.is_interval() => {
            Some(TemporalPlan::new(F::Interval, F::Interval, F::Interval))
        }
        kind @ (F::Timestamp | F::Time) if by.is_interval() => {
            Some(TemporalPlan::new(kind, F::Interval, kind))
        }
        // `date + 'text'` matches several operators, which PostgreSQL refuses.
        F::Date if by.kind() == Some(F::Interval) => {
            Some(TemporalPlan::new(F::Date, F::Interval, F::Timestamp))
        }
        F::TimestampTz if by.is_zoneless() => Some(TemporalPlan::new(
            F::TimestampTz,
            F::Interval,
            F::TimestampTz,
        )),
        _ => None,
    }
}

/// `left - right`.
fn difference(left: Operand, right: Operand) -> Option<TemporalPlan> {
    use ScalarFamily as F;
    match (left.kind(), right.kind()) {
        (Some(F::Interval), _) if right.is_interval() => {
            Some(TemporalPlan::new(F::Interval, F::Interval, F::Interval))
        }
        (None, Some(F::Interval)) if left.is_interval() => {
            Some(TemporalPlan::new(F::Interval, F::Interval, F::Interval))
        }
        // An untyped literal after a timestamp's `-` is the timestamp
        // subtracted, and after a time's it is a time, which is read.
        (Some(kind @ (F::Timestamp | F::TimestampTz)), other)
            if other == Some(kind) || matches!(right, Operand::Unknown(_)) =>
        {
            Some(TemporalPlan::new(kind, kind, F::Interval))
        }
        (Some(kind @ (F::Timestamp | F::Time)), Some(F::Interval)) => {
            Some(TemporalPlan::new(kind, F::Interval, kind))
        }
        (Some(F::Date), Some(F::Interval)) => {
            Some(TemporalPlan::new(F::Date, F::Interval, F::Timestamp))
        }
        (Some(F::TimestampTz), Some(F::Interval)) if right.is_zoneless() => Some(
            TemporalPlan::new(F::TimestampTz, F::Interval, F::TimestampTz),
        ),
        _ => None,
    }
}

/// `span * factor` or `span / divisor`.
fn scaled(span: Operand, by: Operand) -> Option<TemporalPlan> {
    match (span.kind()?, by) {
        (ScalarFamily::Interval, Operand::Number(number)) => Some(TemporalPlan::new(
            ScalarFamily::Interval,
            number,
            ScalarFamily::Interval,
        )),
        _ => None,
    }
}

/// Whether `expr` is an interval, which unary `-` negates.
pub(super) fn is_interval<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    depth: usize,
) -> bool {
    B::HAS_INTERVAL
        && matches!(
            operand::<B, DB>(expr, table_id, database, depth),
            Operand::Temporal {
                kind: ScalarFamily::Interval,
                ..
            }
        )
}
