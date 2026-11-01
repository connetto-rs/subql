//! `SqlLiteralParse` implementations for the three shipped backends, which differ only in their
//! `BOOL`, `UUID`, `JSON`, `JSONB` and `INTERVAL` literals.

use super::parse_helpers::{
    err_shape, parse_date_literal, parse_decimal_literal, parse_f64_literal, parse_hex_bytes,
    parse_i64_literal, parse_interval_literal, parse_json, parse_time_literal,
    parse_timestamp_literal, parse_timestamp_tz_literal, parse_uuid, parse_uuid_as_string,
    quoted_string,
};
use super::{parse_custom_literal, SqlLiteralParse};
use crate::backend::{
    MySql, Postgres, SQLite, ScalarFamily, SqliteJson, Value, ValueKind, ValueKindOf,
};
use crate::RegisterError;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sqlparser::ast::Value as SqlValue;

/// Parse `sql` as `target` for a backend on the standard carriers, asking `specific` first for
/// the families whose literal the backend spells its own way.
fn parse_standard_literal<B>(
    sql: &SqlValue,
    target: ValueKindOf<B>,
    specific: impl FnOnce(ScalarFamily) -> Option<Result<Value<B>, RegisterError>>,
) -> Result<Value<B>, RegisterError>
where
    B: SqlLiteralParse<
        Int = i64,
        Float = f64,
        String = String,
        Bytes = Vec<u8>,
        Timestamp = chrono::NaiveDateTime,
        TimestampTz = chrono::DateTime<chrono::Utc>,
        Date = chrono::NaiveDate,
        Time = chrono::NaiveTime,
        Decimal = bigdecimal::BigDecimal,
    >,
{
    if matches!(sql, SqlValue::Null) {
        return Ok(Value::Null);
    }
    let family = match target {
        ValueKind::Builtin(family) => family,
        ValueKind::Custom(custom) => return parse_custom_literal::<B>(sql, custom),
    };
    if let Some(parsed) = specific(family) {
        return parsed;
    }
    match (family, sql) {
        (ScalarFamily::Int, SqlValue::Number(n, _)) => Ok(Value::Int(parse_i64_literal(n, sql)?)),
        (ScalarFamily::Float, SqlValue::Number(n, _)) => {
            Ok(Value::Float(parse_f64_literal(n, sql)?))
        }
        (ScalarFamily::String, _) => quoted_string(sql)
            .map(|s| Value::String(s.to_string()))
            .ok_or_else(|| err_shape(sql, target)),
        (ScalarFamily::Bytes, SqlValue::HexStringLiteral(s)) => {
            Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
        }
        (ScalarFamily::Timestamp, _) => quoted_string(sql)
            .ok_or_else(|| err_shape(sql, target))
            .and_then(|s| parse_timestamp_literal(s, sql).map(Value::Timestamp)),
        (ScalarFamily::TimestampTz, _) => quoted_string(sql)
            .ok_or_else(|| err_shape(sql, target))
            .and_then(|s| parse_timestamp_tz_literal(s, sql).map(Value::TimestampTz)),
        (ScalarFamily::Date, _) => quoted_string(sql)
            .ok_or_else(|| err_shape(sql, target))
            .and_then(|s| parse_date_literal(s, sql).map(Value::Date)),
        (ScalarFamily::Time, _) => quoted_string(sql)
            .ok_or_else(|| err_shape(sql, target))
            .and_then(|s| parse_time_literal(s, sql).map(Value::Time)),
        (ScalarFamily::Decimal, SqlValue::Number(n, _)) => {
            Ok(Value::Decimal(parse_decimal_literal(n, sql)?))
        }
        _ => Err(err_shape(sql, target)),
    }
}

impl<V: postgres_jsonb_canonical::PgVersion + 'static> SqlLiteralParse for Postgres<V> {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        parse_standard_literal(sql, target, |family| match (family, sql) {
            (ScalarFamily::Bool, SqlValue::Boolean(b)) => Some(Ok(Value::Bool(*b))),
            (ScalarFamily::Uuid, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_uuid(s, sql).map(Value::Uuid)),
            ),
            (ScalarFamily::Interval, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_interval_literal(s, sql).map(Value::Interval)),
            ),
            (ScalarFamily::Json, _) => Some(Err(RegisterError::TypeError(
                "PostgreSQL json has no equality operator".to_string(),
            ))),
            (ScalarFamily::Jsonb, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_json(s, sql))
                    .and_then(|value| {
                        // The only path a `jsonb` value enters other than a row, so its refusal lives here.
                        postgres_jsonb_canonical::encode::<V>(&value)
                            .map_err(|error| super::parse_helpers::err_parse(sql, family, error))?;
                        Ok(Value::Jsonb(value))
                    }),
            ),
            _ => None,
        })
    }
}

impl<C: crate::backend::MySqlTableNameCase> SqlLiteralParse for MySql<C> {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        parse_standard_literal(sql, target, |family| match (family, sql) {
            (ScalarFamily::Bool, SqlValue::Boolean(b)) => Some(Ok(Value::Bool(*b))),
            // MySQL has no UUID type, so the validated text is the value.
            (ScalarFamily::Uuid, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_uuid_as_string(s, sql).map(Value::Uuid)),
            ),
            (ScalarFamily::Json | ScalarFamily::Jsonb, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_json(s, sql))
                    .map(|value| {
                        if matches!(family, ScalarFamily::Json) {
                            Value::Json(value)
                        } else {
                            Value::Jsonb(value)
                        }
                    }),
            ),
            _ => None,
        })
    }
}

impl SqlLiteralParse for SQLite {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        parse_standard_literal(sql, target, |family| match (family, sql) {
            // SQLite has no BOOL, so a boolean column holds 0 or 1.
            (ScalarFamily::Bool, SqlValue::Boolean(b)) => Some(Ok(Value::Bool(i64::from(*b)))),
            // SQLite has no UUID type, so the validated text is the value.
            (ScalarFamily::Uuid, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_uuid_as_string(s, sql).map(Value::Uuid)),
            ),
            (ScalarFamily::Json | ScalarFamily::Jsonb, _) => Some(
                quoted_string(sql)
                    .ok_or_else(|| err_shape(sql, target))
                    .and_then(|s| parse_json(s, sql).map(|_| SqliteJson::text(s.to_string())))
                    .map(|value| {
                        if matches!(family, ScalarFamily::Json) {
                            Value::Json(value)
                        } else {
                            Value::Jsonb(value)
                        }
                    }),
            ),
            _ => None,
        })
    }
}
