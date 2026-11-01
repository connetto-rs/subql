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
            let (x, y) = carriers::<NaiveDateTime, _>(&x, &y, operation)?;
            interval(timestamp_difference(x, y), operation)
        }
        (Value::TimestampTz(x), Value::TimestampTz(y)) => {
            let (x, y) = carriers::<DateTime<Utc>, _>(&x, &y, operation)?;
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

/// Two carriers of one type read as the `chrono` type PostgreSQL's carries.
fn carriers<T: Any + Copy, C: Any>(
    x: &C,
    y: &C,
    operation: ArithmeticOp,
) -> Result<(T, T), EvaluationRefusal> {
    let read = |value: &C| (value as &dyn Any).downcast_ref::<T>().copied();
    read(x)
        .zip(read(y))
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

#[cfg(test)]
mod tests {
    //! A `Backend` whose temporal carriers differ from this library's must
    //! never panic inside a temporal operator, and never answer one. Every
    //! interval operation over such a row is refused, so the downcast
    //! fallibility at every carrier read is exercised, not bypassed.

    use alloc::string::String;
    use alloc::vec::Vec;

    use crate::backend::{
        at_float4, declared_type_of, declares_sixty_four_bit_int, Backend, ComparisonContext,
        DeclaredType, DivisionPrecisionIncrement, DivisionRule, FloatOrder, FloatSumOverflow,
        FloatWidth, IntWidth, Interval, MeanRule, NoCustomScalars, NullSafeEquality, Pg18,
        ScalarFamily, SumRule, TextOperation, TextResolution, TextRule, TextWidth, Value,
        VarianceSeed,
    };
    use crate::compiler::vm::refusal::{
        ArithmeticOp, DanglingEscape, DivisionByZero, EvaluationRefusal, IntegerOverflow,
    };

    /// A `Postgres` whose temporal carriers and decimal are all the `i64`, and
    /// whose `/` answers a decimal whose scale depends on the declared
    /// `div_precision_increment`. A hand-built catalog can declare no such
    /// setting, so its registration is refused rather than answered.
    struct Tb;

    impl Backend for Tb {
        type Dialect = sqlparser::dialect::PostgreSqlDialect;
        type Custom = NoCustomScalars<Self>;
        type Bool = bool;
        type Int = i64;
        type Float = f64;
        type String = String;
        type Bytes = Vec<u8>;
        type Uuid = uuid::Uuid;
        type Timestamp = i64;
        type TimestampTz = i64;
        type Date = i64;
        type Time = i64;
        type Decimal = i64;
        type Json = serde_json::Value;
        type Jsonb = serde_json::Value;
        type JsonbVersion = Pg18;

        const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');
        const LIKE_DANGLING_ESCAPE: DanglingEscape = DanglingEscape::Fails;
        const COLUMN_NAME_CASE: sql_traits::structs::IdentifierCase =
            sql_traits::structs::IdentifierCase::AsWritten;
        const WRITTEN_TABLE_NAME_CASE: sql_traits::structs::IdentifierCase =
            sql_traits::structs::IdentifierCase::AsWritten;
        const WIRE_TABLE_NAME_CASE: sql_traits::structs::IdentifierCase =
            sql_traits::structs::IdentifierCase::Exact;
        const DIVISION_BY_ZERO: DivisionByZero = DivisionByZero::Fails;
        const INTEGER_OVERFLOW: IntegerOverflow = IntegerOverflow::Fails;
        const DIVISION: DivisionRule = DivisionRule::QuotientsAreDecimalInWords;
        const NULL_SAFE_EQUALITY: NullSafeEquality = NullSafeEquality::DistinctFrom;
        const READS_IS_UNKNOWN: bool = true;
        const HAS_INTERVAL: bool = true;
        const RANGE_WITH_NULL_BOUND_COMPARES_DOUBLES: bool = false;
        const NULL_ARITHMETIC_IS_AMBIGUOUS: bool = true;
        const FLOAT_SUM_OVERFLOW: FloatSumOverflow = FloatSumOverflow::Raises;
        const FLOAT_ORDER: FloatOrder = FloatOrder::NanIsGreatest;
        const VARIANCE_SEED: VarianceSeed = VarianceSeed::EnginesOwn;
        const MEAN: MeanRule = MeanRule::Exact;

        fn like_escape_clause(written: &str) -> Result<Option<char>, &'static str> {
            <crate::backend::Postgres as Backend>::like_escape_clause(written)
        }

        fn float_arithmetic_width(
            left: Option<FloatWidth>,
            right: Option<FloatWidth>,
        ) -> Option<FloatWidth> {
            left.or(right).map(|_| FloatWidth::Double)
        }

        fn hold_float_at_single(value: Self::Float) -> Self::Float {
            at_float4(value)
        }

        fn refine_declared_type(family: ScalarFamily, declared_type: &str) -> DeclaredType {
            declared_type_of(
                family,
                declares_sixty_four_bit_int(declared_type),
                FloatWidth::Double,
                TextWidth::Varying,
            )
        }

        fn text_rule(
            _comparison: &ComparisonContext<'_, Self>,
            _operation: TextOperation,
        ) -> TextResolution {
            TextResolution::Rule(TextRule::EXACT)
        }

        fn numeric_widening(
            _left: ScalarFamily,
            _right: ScalarFamily,
        ) -> Option<crate::backend::NumericWidening> {
            None
        }

        fn numeric_literal_family(text: &str) -> ScalarFamily {
            <crate::backend::Postgres as Backend>::numeric_literal_family(text)
        }

        fn sum_rule(column: DeclaredType) -> SumRule {
            match column {
                DeclaredType::Int(IntWidth::UpToThirtyTwo) => SumRule::Integer,
                DeclaredType::Int(IntWidth::SixtyFour) | DeclaredType::Decimal => {
                    SumRule::Decimal {
                        integer_digits: Some(131_072),
                    }
                }
                _ => SumRule::Double,
            }
        }

        fn decimal_quotient(
            dividend: Self::Decimal,
            divisor: Self::Decimal,
            _quotient: crate::compiler::bytecode::Quotient,
        ) -> Self::Decimal {
            // Not reached by the temporal tests, which refuse first.
            if divisor == 0 {
                0
            } else {
                dividend / divisor
            }
        }

        fn integer_quotient(
            dividend: Self::Int,
            divisor: Self::Int,
            _increment: DivisionPrecisionIncrement,
        ) -> Self::Decimal {
            // Not reached by the temporal tests, which refuse first.
            if divisor == 0 {
                0
            } else {
                dividend / divisor
            }
        }

        fn integer_binary(
            operation: ArithmeticOp,
            left: Self::Int,
            right: Self::Int,
        ) -> Result<Value<Self>, EvaluationRefusal> {
            crate::compiler::vm::arithmetic::checked_integer_binary::<Self>(
                Self::INTEGER_OVERFLOW,
                operation,
                left,
                right,
            )
        }

        fn integer_negate(value: Self::Int) -> Result<Value<Self>, EvaluationRefusal> {
            crate::compiler::vm::arithmetic::checked_integer_negate::<Self>(
                Self::INTEGER_OVERFLOW,
                value,
            )
        }
    }

    impl crate::compiler::literals::SqlLiteralParse for Tb {
        fn parse_literal(
            sql: &sqlparser::ast::Value,
            target: crate::backend::ValueKindOf<Self>,
        ) -> Result<Value<Self>, crate::RegisterError> {
            use crate::backend::{ScalarFamily, Value, ValueKind};
            use sqlparser::ast::Value as SqlValue;

            if matches!(sql, SqlValue::Null) {
                return Ok(Value::Null);
            }
            let family = match target {
                ValueKind::Builtin(family) => family,
                ValueKind::Custom(_) => {
                    return Err(crate::RegisterError::TypeError(
                        "the test backend carries no custom literals".to_string(),
                    ))
                }
            };
            match (family, sql) {
                (ScalarFamily::Int, SqlValue::Number(digits, _)) => {
                    let int: i64 = digits.parse().map_err(|_| {
                        crate::RegisterError::TypeError(format!(
                            "cannot read {sql:?} as an integer"
                        ))
                    })?;
                    Ok(Value::Int(int))
                }
                (ScalarFamily::Interval, SqlValue::SingleQuotedString(text)) => {
                    crate::backend::Interval::parse(text)
                        .map(Value::Interval)
                        .ok_or_else(|| {
                            crate::RegisterError::TypeError(format!(
                                "cannot read {sql:?} as an interval"
                            ))
                        })
                }
                _ => Err(crate::RegisterError::TypeError(format!(
                    "the test backend refuses the literal {sql:?}"
                ))),
            }
        }
    }

    /// Each temporal carrier of `Tb` is an `i64`, so reading it as the
    /// `chrono` type a `timestamp` or `date` shift needs is refused, and a
    /// `decimal` scaling factor is not the `BigDecimal` a `span * 2` needs.
    #[test]
    fn interval_arithmetic_over_foreign_carriers_is_refused() {
        let span = Interval::parse("1 hour").expect("the span parses");

        assert!(matches!(
            super::add::<Tb>(Value::Timestamp(1i64), Value::Interval(span)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Add
            })
        ));
        assert!(matches!(
            super::add::<Tb>(Value::TimestampTz(1i64), Value::Interval(span)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Add
            })
        ));
        assert!(matches!(
            super::add::<Tb>(Value::Date(1i64), Value::Interval(span)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Add
            })
        ));
        assert!(matches!(
            super::add::<Tb>(Value::Time(1i64), Value::Interval(span)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Add
            })
        ));
        // An operand that is no temporal carrier at all falls to the catch-all.
        assert!(matches!(
            super::add::<Tb>(Value::Int(1i64), Value::Interval(span)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Add
            })
        ));
        assert!(matches!(
            super::subtract::<Tb>(Value::Timestamp(1i64), Value::Timestamp(2i64)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Subtract
            })
        ));
        assert!(matches!(
            super::subtract::<Tb>(Value::TimestampTz(1i64), Value::TimestampTz(2i64)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Subtract
            })
        ));
        assert!(matches!(
            super::multiply::<Tb>(Value::Interval(span), Value::Decimal(1i64)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Multiply
            })
        ));
        // A factor that is not an `int`, a `float` or a `decimal` falls to the
        // `float_factor` catch-all.
        assert!(matches!(
            super::multiply::<Tb>(Value::Interval(span), Value::String("x".to_string())),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Multiply
            })
        ));
        assert!(matches!(
            super::divide::<Tb>(Value::Interval(span), Value::Decimal(1i64)),
            Err(EvaluationRefusal::OperandKinds {
                operation: ArithmeticOp::Divide
            })
        ));
    }

    /// The temporal `span / 2` plans a `Quotient` whose resolution needs the
    /// declared `div_precision_increment`. A hand-built catalog declares none,
    /// so `Tb` refuses registration instead of answering a decimal with an
    /// unknown scale.
    #[test]
    fn a_division_rule_without_declared_precision_refuses_temporal_division() {
        use crate::testing::TestEvent;
        use crate::{DefaultIds, SubscriptionEngine, SubscriptionRequest};
        use sql_traits::structs::ParserDB;

        let catalog = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            "CREATE TABLE t (id INT PRIMARY KEY, span INTERVAL)",
        )
        .expect("the catalog parses");
        let mut engine: SubscriptionEngine<TestEvent<Tb>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, sqlparser::dialect::PostgreSqlDialect {});
        let registered = engine.register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE span / 2 > '1 day'",
        ));

        assert!(matches!(
            registered,
            Ok(registered)
                if matches!(
                    registered.not_served_because,
                    Some(crate::NotServed::DivisionPrecisionNotDeclared)
                )
        ));
    }
}
