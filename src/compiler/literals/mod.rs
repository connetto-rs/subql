//! SQL literal parsing: sqlparser AST -> typed [`Value`].
//!
//! The compiler emits `PushLiteral(Value<B>)` and `LoadColumn(col)`
//! against a specific [`crate::backend::Backend`]. To keep those two in
//! step, SQL literals need backend-aware coercion: `WHERE bool_col = true`
//! on Postgres produces `Value::<Postgres>::Bool(true)` (Rust `bool`), on
//! SQLite it produces `Value::<SQLite>::Bool(1_i64)`. The [`SqlLiteralParse`]
//! trait captures that per-backend routing.
//!
//! [`SqlLiteralParse`] is a companion trait to `Backend` rather than an
//! extension of it: CDC-runtime paths that only read events don't touch
//! SQL and don't need the sqlparser dependency in their bounds.

mod backend_impls;
mod column_ref;
mod parse_helpers;

use crate::backend::{Backend, CustomScalars, Value, ValueKind, ValueKindOf};
use crate::RegisterError;
use alloc::format;
use parse_helpers::err_shape;
use sqlparser::ast::Value as SqlValue;

pub(super) use column_ref::{coalesce_arguments, resolve_column_ref, value_column};
pub(super) use parse_helpers::hex_upper;

/// Backend-aware parsing of sqlparser AST literals into typed [`Value`]s.
///
/// Implemented per shipped [`Backend`]. The compiler bounds its entry
/// points on `B: Backend + SqlLiteralParse` so the parser can turn any
/// sqlparser literal into a `Value<B>` targeting a known
/// [`ValueKind`].
///
/// # Contract
///
/// * `sqlparser::ast::Value::Null` maps to [`Value::Null`] regardless of
///   `target`.
/// * When the sqlparser value's shape does not fit `target` (e.g.
///   `Boolean(true)` targeting [`crate::backend::ScalarFamily::Timestamp`]) the result is
///   [`RegisterError::TypeError`] naming the mismatch.
/// * When the shape fits but the payload cannot be parsed
///   (e.g. `SingleQuotedString("not-a-uuid")` targeting
///   [`crate::backend::ScalarFamily::Uuid`]) the result is [`RegisterError::TypeError`]
///   with a message naming the underlying parse failure.
pub trait SqlLiteralParse: Backend + Sized {
    /// Coerce a sqlparser AST literal into a [`Value`] typed to `Self`,
    /// targeting scalar kind `target`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::TypeError`] for shape / payload
    /// mismatches. Returns [`RegisterError::UnsupportedSql`] for
    /// sqlparser variants outside the supported subset (currently
    /// `Placeholder`, `Interval`, `Time`, `Date`, `Boolean` in a
    /// non-boolean context, etc.).
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError>;
}

/// Parse a literal targeting a custom type: read it at the type's carrier
/// shape, then hand that to the type's own conversion.
///
/// The same conversion a change event's cell goes through, so a filter's
/// value and a row's value cannot disagree about what a spelling means. A
/// spelling the conversion declines is a registration error naming the
/// literal, never a silently ignored filter.
pub fn parse_custom_literal<B: SqlLiteralParse>(
    sql: &SqlValue,
    custom: <B::Custom as CustomScalars>::Kind,
) -> Result<Value<B>, RegisterError> {
    let carrier = <B::Custom as CustomScalars>::carrier(custom);
    let raw = B::parse_literal(sql, carrier.into())?;
    let view = raw
        .as_carried()
        .ok_or_else(|| err_shape(sql, ValueKind::<()>::from(carrier)))?;
    <B::Custom as CustomScalars>::convert(custom, view)
        .map(Value::Custom)
        .ok_or_else(|| {
            RegisterError::TypeError(format!(
                "the custom type {custom:?} refused SQL literal {sql:?}"
            ))
        })
}

/// Read PostgreSQL's typed literal `interval '…' [fields] [(precision)]`,
/// which reads its text under the fields it names, so `interval '90' minute`
/// is an hour and a half.
///
/// # Errors
///
/// [`RegisterError::TypeError`] for text PostgreSQL refuses, for a field
/// list its grammar has no production for, or for a value that is not a
/// quoted string.
pub(crate) fn interval_literal(
    interval: &sqlparser::ast::Interval,
) -> Result<crate::backend::Interval, RegisterError> {
    use sql_scalar_text::{IntervalFields, IntervalTypmod};
    use sqlparser::ast::DateTimeField as Field;

    let refused = || RegisterError::TypeError(format!("cannot read {interval} as an interval"));
    let sqlparser::ast::Expr::Value(value) = interval.value.as_ref() else {
        return Err(refused());
    };
    let text = parse_helpers::quoted_string(&value.value).ok_or_else(refused)?;
    let fields = match (&interval.leading_field, &interval.last_field) {
        (None, None) => None,
        (Some(Field::Year), None) => Some(IntervalFields::Year),
        (Some(Field::Month), None) => Some(IntervalFields::Month),
        (Some(Field::Day), None) => Some(IntervalFields::Day),
        (Some(Field::Hour), None) => Some(IntervalFields::Hour),
        (Some(Field::Minute), None) => Some(IntervalFields::Minute),
        (Some(Field::Second), None) => Some(IntervalFields::Second),
        (Some(Field::Year), Some(Field::Month)) => Some(IntervalFields::YearToMonth),
        (Some(Field::Day), Some(Field::Hour)) => Some(IntervalFields::DayToHour),
        (Some(Field::Day), Some(Field::Minute)) => Some(IntervalFields::DayToMinute),
        (Some(Field::Day), Some(Field::Second)) => Some(IntervalFields::DayToSecond),
        (Some(Field::Hour), Some(Field::Minute)) => Some(IntervalFields::HourToMinute),
        (Some(Field::Hour), Some(Field::Second)) => Some(IntervalFields::HourToSecond),
        (Some(Field::Minute), Some(Field::Second)) => Some(IntervalFields::MinuteToSecond),
        _ => return Err(refused()),
    };
    // `SECOND(p)` alone carries its precision as the leading one.
    let precision = match (fields, interval.leading_precision) {
        (Some(IntervalFields::Second), Some(precision))
            if interval.fractional_seconds_precision.is_none() =>
        {
            Some(precision)
        }
        (_, None) => interval.fractional_seconds_precision,
        (_, Some(_)) => return Err(refused()),
    };
    // PostgreSQL clamps a precision above six, so any wider one reads as six.
    let precision = precision.map(|precision| u8::try_from(precision).unwrap_or(u8::MAX));
    let typmod = IntervalTypmod::new(fields, precision).ok_or_else(refused)?;
    sql_scalar_text::parse_interval_typmod(text, typmod)
        .map(crate::backend::Interval::from)
        .ok_or_else(refused)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::SqlLiteralParse;
    use crate::backend::{MySql, Pg18, Postgres, SQLite, ScalarFamily, Value};
    use crate::RegisterError;
    use sqlparser::ast::Value as SqlValue;

    #[test]
    fn postgres_null_is_type_agnostic() {
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&SqlValue::Null, ScalarFamily::Int.into()).unwrap(),
            Value::Null
        );
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&SqlValue::Null, ScalarFamily::String.into()).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn postgres_bool_from_boolean() {
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&SqlValue::Boolean(true), ScalarFamily::Bool.into())
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn postgres_int_from_number() {
        let sql = SqlValue::Number("42".to_string(), false);
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Int.into()).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn postgres_float_from_number() {
        let sql = SqlValue::Number("3.5".to_string(), false);
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Float.into()).unwrap(),
            Value::Float(3.5)
        );
    }

    #[test]
    fn postgres_string_from_quoted() {
        let sql = SqlValue::SingleQuotedString("hi".to_string());
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::String.into()).unwrap(),
            Value::String("hi".to_string())
        );
    }

    #[test]
    fn postgres_uuid_from_quoted() {
        let sql = SqlValue::SingleQuotedString("550e8400-e29b-41d4-a716-446655440000".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Uuid.into()).unwrap();
        assert!(matches!(v, Value::Uuid(_)));
    }

    #[test]
    fn postgres_timestamp_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02T03:04:05".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Timestamp.into()).unwrap();
        assert!(matches!(v, Value::Timestamp(_)));
    }

    #[test]
    fn postgres_date_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Date.into()).unwrap();
        assert!(matches!(v, Value::Date(_)));
    }

    #[test]
    fn postgres_bytes_from_hex_literal() {
        let sql = SqlValue::HexStringLiteral("deadbeef".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Bytes.into()).unwrap();
        assert_eq!(v, Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn postgres_json_equality_is_refused() {
        let sql = SqlValue::SingleQuotedString("{\"k\":1}".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Json.into()),
            Err(RegisterError::TypeError(_))
        ));
    }

    #[test]
    fn postgres_decimal_from_number() {
        let sql = SqlValue::Number("123.456".to_string(), false);
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Decimal.into()).unwrap();
        assert!(matches!(v, Value::Decimal(_)));
    }

    #[test]
    fn postgres_type_mismatch_returns_type_error() {
        let sql = SqlValue::Boolean(true);
        let err = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Int.into()).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn postgres_uuid_invalid_string_errors() {
        let sql = SqlValue::SingleQuotedString("not-a-uuid".to_string());
        let err = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Uuid.into()).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn temporal_literal_maps_representative_values() {
        use chrono::{DateTime, Utc};
        let ts = SqlValue::SingleQuotedString("2026-01-01 00:00:00".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&ts, ScalarFamily::Timestamp.into()),
            Ok(Value::Timestamp(_))
        ));
        assert!(matches!(
            SQLite::parse_literal(&ts, ScalarFamily::Timestamp.into()),
            Ok(Value::Timestamp(_))
        ));
        let tstz = SqlValue::SingleQuotedString("2025-12-31 22:00:00-02".to_string());
        let expected_utc: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&tstz, ScalarFamily::TimestampTz.into()).unwrap(),
            Value::TimestampTz(expected_utc)
        );
        let date = SqlValue::SingleQuotedString("2026-01-01".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&date, ScalarFamily::Date.into()),
            Ok(Value::Date(_))
        ));
        let time = SqlValue::SingleQuotedString("12:34:56.789".to_string());
        assert!(matches!(
            <MySql as SqlLiteralParse>::parse_literal(&time, ScalarFamily::Time.into()),
            Ok(Value::Time(_))
        ));
    }

    #[test]
    fn temporal_literal_rejects_key_boundaries() {
        let no_offset = SqlValue::SingleQuotedString("2026-01-01 00:00:00".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&no_offset, ScalarFamily::TimestampTz.into()),
            Err(RegisterError::TypeError(_))
        ));
        let with_offset = SqlValue::SingleQuotedString("2026-01-01 00:00:00+00".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&with_offset, ScalarFamily::Timestamp.into()),
            Err(RegisterError::TypeError(_))
        ));
    }

    fn typed_interval(sql: &str) -> Result<alloc::string::String, RegisterError> {
        use alloc::string::ToString;
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let expr = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(sql)
            .unwrap()
            .parse_expr()
            .unwrap();
        let interval = match expr {
            sqlparser::ast::Expr::Interval(interval) => interval,
            other => {
                return Err(RegisterError::TypeError(format!(
                    "{sql} is not an interval literal, it reads as {other}"
                )))
            }
        };
        super::interval_literal(&interval).map(|value| value.to_string())
    }

    /// Each field list PostgreSQL's grammar has reads its text as
    /// `interval_in` does under that typmod, as PostgreSQL 16 answers it.
    #[test]
    fn a_typed_interval_reads_under_the_fields_it_names() {
        for (sql, printed) in [
            ("interval '1 day 02:03:04.5'", "1 day 02:03:04.5"),
            ("interval '3' year", "3 years"),
            ("interval '1-2' year", "1 year"),
            ("interval '14' month", "1 year 2 mons"),
            ("interval '3' day", "3 days"),
            ("interval '90' hour", "90:00:00"),
            ("interval '90' minute", "01:30:00"),
            ("interval '1.5' second", "00:00:01.5"),
            ("interval '1.2345' second(2)", "00:00:01.23"),
            ("interval '1-2' year to month", "1 year 2 mons"),
            ("interval '1 2' day to hour", "1 day 02:00:00"),
            ("interval '1 2:03' day to minute", "1 day 02:03:00"),
            ("interval '1 2:03:04.5' day to second(0)", "1 day 02:03:05"),
            ("interval '2:03:04' hour to minute", "02:03:00"),
            ("interval '2:03:04.5' hour to second", "02:03:04.5"),
            ("interval '3:04.5' minute to second(0)", "00:03:05"),
        ] {
            assert_eq!(typed_interval(sql).unwrap(), printed, "{sql}");
        }
    }

    /// What PostgreSQL's grammar or `interval_in` refuses is refused,
    /// including a value that is not a literal at all and an expression
    /// that is not an interval, and what it reads is read rather than
    /// refused with it.
    #[test]
    fn a_typed_interval_postgres_refuses_is_refused() {
        for (sql, refused) in [
            ("interval '1' hour(2)", true),
            ("interval '1 fortnight'", true),
            ("interval 5 hour", true),
            ("interval x hour", true),
            ("1 + 1", true),
            ("interval '1 day 02:03:04.5'", false),
        ] {
            assert_eq!(
                matches!(typed_interval(sql), Err(RegisterError::TypeError(_))),
                refused,
                "{sql}"
            );
        }
    }

    #[test]
    fn mysql_and_sqlite_keep_a_valid_uuid_as_its_text() {
        const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
        let valid = SqlValue::SingleQuotedString(UUID.to_string());
        let invalid = SqlValue::SingleQuotedString("not-a-uuid".to_string());
        assert_eq!(
            <MySql>::parse_literal(&valid, ScalarFamily::Uuid.into()).unwrap(),
            Value::Uuid(UUID.to_string())
        );
        assert_eq!(
            SQLite::parse_literal(&valid, ScalarFamily::Uuid.into()).unwrap(),
            Value::Uuid(UUID.to_string())
        );
        assert!(matches!(
            <MySql>::parse_literal(&invalid, ScalarFamily::Uuid.into()),
            Err(RegisterError::TypeError(_))
        ));
        assert!(matches!(
            SQLite::parse_literal(&invalid, ScalarFamily::Uuid.into()),
            Err(RegisterError::TypeError(_))
        ));
    }

    #[test]
    fn mysql_json_literals_keep_the_family_they_target() {
        let sql = SqlValue::SingleQuotedString(r#"{"k":1}"#.to_string());
        let document = serde_json::json!({"k": 1});
        assert_eq!(
            <MySql>::parse_literal(&sql, ScalarFamily::Json.into()).unwrap(),
            Value::Json(document.clone())
        );
        assert_eq!(
            <MySql>::parse_literal(&sql, ScalarFamily::Jsonb.into()).unwrap(),
            Value::Jsonb(document)
        );
    }

    #[test]
    fn sqlite_json_literals_keep_their_text_once_it_parses() {
        use crate::backend::SqliteJson;
        let text = r#"{"k": 1}"#;
        let sql = SqlValue::SingleQuotedString(text.to_string());
        assert_eq!(
            SQLite::parse_literal(&sql, ScalarFamily::Json.into()).unwrap(),
            Value::Json(SqliteJson::text(text.to_string()))
        );
        assert_eq!(
            SQLite::parse_literal(&sql, ScalarFamily::Jsonb.into()).unwrap(),
            Value::Jsonb(SqliteJson::text(text.to_string()))
        );
        let broken = SqlValue::SingleQuotedString("{".to_string());
        assert!(matches!(
            SQLite::parse_literal(&broken, ScalarFamily::Json.into()),
            Err(RegisterError::TypeError(_))
        ));
    }

    /// A fractional precision on a field list that does not end in `SECOND`
    /// is refused, whatever shape the parser hands over.
    #[test]
    fn a_precision_without_seconds_is_refused() {
        let interval = sqlparser::ast::Interval {
            value: alloc::boxed::Box::new(sqlparser::ast::Expr::value(
                SqlValue::SingleQuotedString("1".into()),
            )),
            leading_field: Some(sqlparser::ast::DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: Some(3),
        };
        assert!(matches!(
            super::interval_literal(&interval),
            Err(RegisterError::TypeError(_))
        ));
    }
}
