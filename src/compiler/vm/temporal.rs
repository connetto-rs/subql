//! Arithmetic over PostgreSQL intervals and the dates and times they move.
//!
//! Registration admits these pairs only on the engine that has an interval,
//! and only those whose answer does not depend on the session's time zone.
//! A timestamp PostgreSQL answers that `chrono` does not carry, an infinite
//! one or one past the year 262142, is refused for the row with
//! [`EvaluationRefusal::DatetimeUnrepresentable`].

use alloc::string::ToString;
use core::any::Any;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use super::refusal::{ArithmeticOp, EvaluationRefusal};
use crate::backend::{
    shift_time, shift_timestamp, timestamp_difference, Backend, Interval, TimestampShift, Value,
};

/// `a + b` over an interval and a date, time or interval.
///
/// # Errors
///
/// [`EvaluationRefusal::DatetimeOutOfRange`] where PostgreSQL raises,
/// [`EvaluationRefusal::DatetimeUnrepresentable`] for a timestamp `chrono`
/// does not carry, and [`EvaluationRefusal::OperandKinds`] for a pair it is
/// not computed over.
pub(super) fn add<B: Backend>(a: Value<B>, b: Value<B>) -> Result<Value<B>, EvaluationRefusal> {
    let operation = ArithmeticOp::Add;
    match (a, b) {
        (Value::Interval(x), Value::Interval(y)) => interval(x.checked_add(y), operation),
        (Value::Interval(span), other) | (other, Value::Interval(span)) => {
            shifted(other, span, false, operation)
        }
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}

/// `a - b`: an interval from an interval or a date or time, or two
/// timestamps apart.
///
/// # Errors
///
/// As [`add`].
pub(super) fn subtract<B: Backend>(
    a: Value<B>,
    b: Value<B>,
) -> Result<Value<B>, EvaluationRefusal> {
    let operation = ArithmeticOp::Subtract;
    match (a, b) {
        (Value::Interval(x), Value::Interval(y)) => interval(x.checked_sub(y), operation),
        (other, Value::Interval(span)) => shifted(other, span, true, operation),
        (Value::Timestamp(x), Value::Timestamp(y)) => {
            let (x, y) = (naive::<B>(&x, operation)?, naive::<B>(&y, operation)?);
            interval(timestamp_difference(x, y), operation)
        }
        (Value::TimestampTz(x), Value::TimestampTz(y)) => {
            let (x, y) = (instant::<B>(&x, operation)?, instant::<B>(&y, operation)?);
            interval(
                timestamp_difference(x.naive_utc(), y.naive_utc()),
                operation,
            )
        }
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}

/// `interval * number` in either order, as `interval_mul` scales it.
///
/// # Errors
///
/// As [`add`], and [`EvaluationRefusal::DecimalOutsideFloatRange`] for a
/// decimal factor no double holds.
pub(super) fn multiply<B: Backend>(
    a: Value<B>,
    b: Value<B>,
) -> Result<Value<B>, EvaluationRefusal> {
    let operation = ArithmeticOp::Multiply;
    match (a, b) {
        (Value::Interval(span), factor) | (factor, Value::Interval(span)) => {
            interval(span.times(float_factor(&factor, operation)?), operation)
        }
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}

/// `interval / number`, as `interval_div` divides.
///
/// # Errors
///
/// As [`multiply`], and [`EvaluationRefusal::DivisionByZero`] for a zero
/// divisor, which PostgreSQL raises on.
pub(super) fn divide<B: Backend>(a: Value<B>, b: Value<B>) -> Result<Value<B>, EvaluationRefusal> {
    let operation = ArithmeticOp::Divide;
    match (a, b) {
        (Value::Interval(span), divisor) => {
            let divisor = float_factor(&divisor, operation)?;
            if divisor == 0.0 {
                return Err(EvaluationRefusal::DivisionByZero { operation });
            }
            interval(span.divided_by(divisor), operation)
        }
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}

/// `-interval`.
///
/// # Errors
///
/// [`EvaluationRefusal::DatetimeOutOfRange`] for a field at its minimum.
pub(super) fn negate<B: Backend>(a: &Value<B>) -> Result<Value<B>, EvaluationRefusal> {
    let operation = ArithmeticOp::Negate;
    match a {
        Value::Interval(span) => interval(span.checked_neg(), operation),
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}

fn interval<B: Backend>(
    result: Option<Interval>,
    operation: ArithmeticOp,
) -> Result<Value<B>, EvaluationRefusal> {
    result
        .map(Value::Interval)
        .ok_or(EvaluationRefusal::DatetimeOutOfRange { operation })
}

/// A date, time or timestamp moved by `span`, forward or back.
fn shifted<B: Backend>(
    at: Value<B>,
    span: Interval,
    subtract: bool,
    operation: ArithmeticOp,
) -> Result<Value<B>, EvaluationRefusal> {
    let out_of_range = EvaluationRefusal::DatetimeOutOfRange { operation };
    let span = if subtract {
        span.checked_neg().ok_or(out_of_range)?
    } else {
        span
    };
    match at {
        Value::Timestamp(at) => timestamp_value::<B>(
            shift_timestamp(naive::<B>(&at, operation)?, span),
            operation,
        ),
        // A date moves as the timestamp at its midnight, and answers one.
        Value::Date(day) => {
            let day = carrier::<NaiveDate, _>(&day, operation)?;
            timestamp_value::<B>(
                shift_timestamp(day.and_time(NaiveTime::MIN), span),
                operation,
            )
        }
        Value::Time(at) => {
            let at = carrier::<NaiveTime, _>(&at, operation)?;
            let moved = shift_time(at, span, false).ok_or(out_of_range)?;
            same::<B::Time, _>(moved, operation).map(Value::Time)
        }
        // Only an interval without months or days is admitted, whose result
        // no time zone moves.
        Value::TimestampTz(at) if span.months() == 0 && span.days() == 0 => {
            let at = instant::<B>(&at, operation)?;
            let moved =
                shift_timestamp(at.naive_utc(), span).map_err(|error| refusal(error, operation))?;
            same::<B::TimestampTz, _>(moved.and_utc(), operation).map(Value::TimestampTz)
        }
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}

fn timestamp_value<B: Backend>(
    moved: Result<NaiveDateTime, TimestampShift>,
    operation: ArithmeticOp,
) -> Result<Value<B>, EvaluationRefusal> {
    let moved = moved.map_err(|error| refusal(error, operation))?;
    same::<B::Timestamp, _>(moved, operation).map(Value::Timestamp)
}

const fn refusal(error: TimestampShift, operation: ArithmeticOp) -> EvaluationRefusal {
    match error {
        TimestampShift::OutOfRange => EvaluationRefusal::DatetimeOutOfRange { operation },
        TimestampShift::Unrepresentable => EvaluationRefusal::DatetimeUnrepresentable { operation },
    }
}

/// A backend's carrier read as the `chrono` type PostgreSQL's carries.
fn carrier<T: Any + Copy, C: Any>(
    value: &C,
    operation: ArithmeticOp,
) -> Result<T, EvaluationRefusal> {
    (value as &dyn Any)
        .downcast_ref::<T>()
        .copied()
        .ok_or(EvaluationRefusal::OperandKinds { operation })
}

fn naive<B: Backend>(
    value: &B::Timestamp,
    operation: ArithmeticOp,
) -> Result<NaiveDateTime, EvaluationRefusal> {
    carrier::<NaiveDateTime, _>(value, operation)
}

fn instant<B: Backend>(
    value: &B::TimestampTz,
    operation: ArithmeticOp,
) -> Result<DateTime<Utc>, EvaluationRefusal> {
    carrier::<DateTime<Utc>, _>(value, operation)
}

/// A `chrono` result as the backend's own carrier of it.
fn same<C: Any + Clone, T: Any>(value: T, operation: ArithmeticOp) -> Result<C, EvaluationRefusal> {
    (&value as &dyn Any)
        .downcast_ref::<C>()
        .cloned()
        .ok_or(EvaluationRefusal::OperandKinds { operation })
}

/// A number as the `double precision` PostgreSQL scales an interval by: an
/// integer cast as C casts it, a decimal read back from its text as
/// `numeric_float8` reads it.
fn float_factor<B: Backend>(
    value: &Value<B>,
    operation: ArithmeticOp,
) -> Result<f64, EvaluationRefusal> {
    match value {
        // C's `(float8) int8`, rounding past 2^53 as PostgreSQL does.
        #[allow(clippy::cast_precision_loss)]
        Value::Int(int) => carrier::<i64, _>(int, operation).map(|int| int as f64),
        Value::Float(float) => carrier::<f64, _>(float, operation),
        Value::Decimal(decimal) => {
            let decimal = (decimal as &dyn Any)
                .downcast_ref::<bigdecimal::BigDecimal>()
                .ok_or(EvaluationRefusal::OperandKinds { operation })?;
            decimal
                .to_string()
                .parse::<f64>()
                .ok()
                .filter(|float| float.is_finite())
                .ok_or(EvaluationRefusal::DecimalOutsideFloatRange)
        }
        _ => Err(EvaluationRefusal::OperandKinds { operation }),
    }
}
