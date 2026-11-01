//! A test backend whose temporal carriers and decimal are all the `i64`, so
//! reading one as the `chrono` type or `BigDecimal` a temporal operator
//! needs is refused rather than answered. It takes PostgreSQL's rules
//! otherwise, and its `/` answers a decimal whose scale needs the declared
//! `div_precision_increment`, which a hand-built catalog never declares.

use sqlparser::ast::Value as SqlValue;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{
    at_float4, declared_type_of, declares_sixty_four_bit_int, Backend, ComparisonContext,
    DeclaredType, DivisionPrecisionIncrement, DivisionRule, FloatOrder, FloatSumOverflow,
    FloatWidth, IntWidth, Interval, MeanRule, NoCustomScalars, NullSafeEquality, Pg18,
    ScalarFamily, SumRule, TextOperation, TextResolution, TextRule, TextWidth, Value, ValueKind,
    ValueKindOf, VarianceSeed,
};
use subql::compiler::vm::refusal::{
    ArithmeticOp, DanglingEscape, DivisionByZero, EvaluationRefusal, IntegerOverflow,
};
use subql::compiler::SqlLiteralParse;
use subql::RegisterError;

/// PostgreSQL's rules with temporal and decimal carriers the `i64`, and a
/// `/` whose quotient needs the declared `div_precision_increment`.
#[derive(Debug)]
pub struct ForeignCarriers;

impl Backend for ForeignCarriers {
    type Dialect = PostgreSqlDialect;
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
        <subql::backend::Postgres as Backend>::like_escape_clause(written)
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
    ) -> Option<subql::backend::NumericWidening> {
        None
    }

    fn numeric_literal_family(text: &str) -> ScalarFamily {
        <subql::backend::Postgres as Backend>::numeric_literal_family(text)
    }

    fn sum_rule(column: DeclaredType) -> SumRule {
        match column {
            DeclaredType::Int(IntWidth::UpToThirtyTwo) => SumRule::Integer,
            DeclaredType::Int(IntWidth::SixtyFour) | DeclaredType::Decimal => SumRule::Decimal {
                integer_digits: Some(131_072),
            },
            _ => SumRule::Double,
        }
    }

    fn decimal_quotient(
        dividend: Self::Decimal,
        divisor: Self::Decimal,
        _quotient: subql::compiler::bytecode::Quotient,
    ) -> Self::Decimal {
        // Not reached: the temporal tests refuse before a quotient is read.
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
        // Not reached: the temporal tests refuse before a quotient is read.
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
        subql::compiler::vm::arithmetic::checked_integer_binary::<Self>(
            Self::INTEGER_OVERFLOW,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(value: Self::Int) -> Result<Value<Self>, EvaluationRefusal> {
        subql::compiler::vm::arithmetic::checked_integer_negate::<Self>(
            Self::INTEGER_OVERFLOW,
            value,
        )
    }
}

impl SqlLiteralParse for ForeignCarriers {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ValueKind::Builtin(family) => family,
            ValueKind::Custom(_) => {
                return Err(RegisterError::TypeError(
                    "the test backend carries no custom literals".to_string(),
                ))
            }
        };
        match (family, sql) {
            (ScalarFamily::Int, SqlValue::Number(digits, _)) => {
                let int: i64 = digits.parse().map_err(|_| {
                    RegisterError::TypeError(format!("cannot read {sql:?} as an integer"))
                })?;
                Ok(Value::Int(int))
            }
            (ScalarFamily::Interval, SqlValue::SingleQuotedString(text)) => {
                Interval::parse(text).map(Value::Interval).ok_or_else(|| {
                    RegisterError::TypeError(format!("cannot read {sql:?} as an interval"))
                })
            }
            _ => Err(RegisterError::TypeError(format!(
                "the test backend refuses the literal {sql:?}"
            ))),
        }
    }
}
