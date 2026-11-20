//! The replica's storage form of a value subql evaluated rather than read
//! off the wire: a group value, an aggregate result or a computed row's
//! cell.
//!
//! Each value goes through the decoders [`pgoutput_patchset`] uses for
//! rows, so it lands exactly as a row holding it would and every
//! [`WireType`] keeps one definition. A typed value is written in
//! PostgreSQL's binary send form and decoded through [`pgbinary_adapter`].
//! A text value is PostgreSQL's text output of its type, which is the form
//! subql holds an array, a vector or a type it does not model in, and
//! decodes as a text cell through [`pgoutput_adapter`].
//!
//! [`pgoutput_patchset`]: super::pgoutput_patchset

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use bigdecimal::num_bigint::Sign;
use bigdecimal::BigDecimal;
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgBinaryStream, PgWalstreamColumn};
use sqlite_diff_rs::{
    DecodeError, PgBinary, PgBinaryColumn, TypeMap, Value as WireValue, WireAdapter, WireType,
};

use super::{pgbinary_adapter, pgoutput_adapter, scalar_kind_to_wire_type, StorageType};
use crate::backend::{Interval, Postgres, ScalarFamily, Value};
use crate::{AggValue, NumericValue};

/// Why a value has no storage form under the type it was asked in.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// [`Value::Missing`], a cell the source never carried.
    #[error("a cell the source did not carry has no storage form")]
    Missing,
    /// The value's family is not one a column of the type holds.
    #[error("a {family:?} value is not stored as {wire_type:?}")]
    Mismatch {
        /// The family of the value.
        family: ScalarFamily,
        /// The type it was asked to be stored as.
        wire_type: WireType,
    },
    /// The value lies outside what PostgreSQL's binary form of its type
    /// carries, such as a decimal scale above 16383 or a `jsonb` number
    /// exponent past `i64`.
    #[error("a {0:?} value outside PostgreSQL's range for its type")]
    OutOfRange(ScalarFamily),
    /// The decoder refused the value, as it would refuse the same value in
    /// a row.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

/// Encodes evaluated values in the form the replica stores rows in.
///
/// Holds the decoder registries, so build one and reuse it.
pub struct StorageEncoder {
    binary: TypeMap<PgBinary, String, Vec<u8>>,
    text: PgBinaryStream<String, Vec<u8>>,
}

impl Default for StorageEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEncoder {
    /// An encoder over [`pgbinary_adapter`] and [`pgoutput_adapter`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            binary: pgbinary_adapter(),
            text: pgoutput_adapter(),
        }
    }

    /// `value` as a column of `storage_type` stores it.
    ///
    /// `None` is an output no catalog column stands behind, stored as the
    /// value's family maps under [`scalar_kind_to_wire_type`], so a
    /// decimal keeps its text. SQL NULL is NULL under any type.
    ///
    /// A `json` value is printed from the parsed document, so it holds the
    /// same content as the row's text but not that text's spacing or key
    /// order.
    pub fn value<V: postgres_jsonb_canonical::PgVersion + 'static>(
        &self,
        value: &Value<Postgres<V>>,
        storage_type: Option<StorageType>,
    ) -> Result<WireValue<String, Vec<u8>>, StorageError> {
        let cell = match value {
            Value::Missing => return Err(StorageError::Missing),
            Value::Null => return Ok(WireValue::Null),
            Value::Bool(flag) => Cell::Bool(*flag),
            Value::Int(integer) => Cell::Int(*integer),
            Value::Float(double) => Cell::Float(*double),
            Value::String(text) => Cell::Text(text),
            Value::Bytes(bytes) => Cell::Bytes(bytes),
            Value::Uuid(uuid) => Cell::Uuid(uuid.as_bytes()),
            Value::Timestamp(timestamp) => Cell::Timestamp(*timestamp),
            Value::TimestampTz(timestamp) => Cell::TimestampTz(timestamp.naive_utc()),
            Value::Date(date) => Cell::Date(*date),
            Value::Time(time) => Cell::Time(*time),
            Value::Decimal(decimal) => Cell::Decimal(decimal),
            Value::Json(json) => Cell::Json(json),
            Value::Jsonb(jsonb) => Cell::Jsonb(jsonb),
            Value::Interval(interval) => Cell::Interval(*interval),
            #[expect(
                clippy::uninhabited_references,
                reason = "no `NoCustom` exists, so the arm never runs"
            )]
            Value::Custom(never) => match *never {},
        };
        self.store(cell, storage_type)
    }

    /// An aggregate's folded `value` as a column of `storage_type` stores
    /// it, under the same rules as [`Self::value`].
    pub fn aggregate(
        &self,
        value: &AggValue,
        storage_type: Option<StorageType>,
    ) -> Result<WireValue<String, Vec<u8>>, StorageError> {
        let cell = match value {
            AggValue::CountStar(count) | AggValue::CountColumn(count) => Cell::Int(*count),
            AggValue::Sum(number) | AggValue::Avg(number) => match number {
                None => return Ok(WireValue::Null),
                Some(NumericValue::Integer(integer)) => Cell::Int(*integer),
                Some(NumericValue::Decimal(decimal)) => Cell::Decimal(decimal),
                Some(NumericValue::Double(double)) => Cell::Float(*double),
                Some(NumericValue::Interval(interval)) => Cell::Interval(*interval),
            },
            AggValue::VarPop(real)
            | AggValue::VarSamp(real)
            | AggValue::StddevPop(real)
            | AggValue::StddevSamp(real) => match real {
                None => return Ok(WireValue::Null),
                Some(real) => Cell::Float(*real),
            },
        };
        self.store(cell, storage_type)
    }

    fn store(
        &self,
        cell: Cell<'_>,
        storage_type: Option<StorageType>,
    ) -> Result<WireValue<String, Vec<u8>>, StorageError> {
        let family = cell.family();
        let storage_type =
            storage_type.unwrap_or_else(|| StorageType::new(scalar_kind_to_wire_type(family)));
        let wire_type = storage_type.wire_type();
        match (wire_type, cell) {
            (
                WireType::Array { .. } | WireType::Vector { .. } | WireType::Opaque,
                Cell::Text(text),
            ) => {
                let data = ColumnValue::text(text);
                Ok(self.text.decode(PgWalstreamColumn {
                    column_name: "",
                    wire_type,
                    data: &data,
                })?)
            }
            (_, cell) if holds(wire_type, family) => {
                let sent = cell.send(storage_type.is_single_precision())?;
                Ok(self.binary.decode(PgBinaryColumn {
                    column_name: "",
                    wire_type,
                    raw: Some(sent.as_slice()),
                })?)
            }
            _ => Err(StorageError::Mismatch { family, wire_type }),
        }
    }
}

/// Whether a column of `wire_type` holds values of `family`.
const fn holds(wire_type: WireType, family: ScalarFamily) -> bool {
    matches!(
        (wire_type, family),
        (WireType::Bool, ScalarFamily::Bool)
            | (WireType::Int, ScalarFamily::Int)
            | (WireType::Real, ScalarFamily::Float)
            | (WireType::Text, ScalarFamily::String)
            | (WireType::Bytes, ScalarFamily::Bytes)
            | (WireType::Uuid, ScalarFamily::Uuid)
            | (
                WireType::Decimal | WireType::ScaledDecimal { .. },
                ScalarFamily::Decimal
            )
            | (WireType::Timestamp, ScalarFamily::Timestamp)
            | (WireType::TimestampTz, ScalarFamily::TimestampTz)
            | (WireType::Date, ScalarFamily::Date)
            | (WireType::Time, ScalarFamily::Time)
            | (WireType::Interval, ScalarFamily::Interval)
            | (WireType::Json, ScalarFamily::Json)
            | (WireType::Jsonb, ScalarFamily::Jsonb)
    )
}

/// One value, borrowed, in the shape its send form is written from.
enum Cell<'a> {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(&'a str),
    Bytes(&'a [u8]),
    Uuid(&'a [u8; 16]),
    Timestamp(chrono::NaiveDateTime),
    /// The instant in UTC, which is what `timestamptz_send` counts from.
    TimestampTz(chrono::NaiveDateTime),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Decimal(&'a BigDecimal),
    Json(&'a serde_json::Value),
    Jsonb(&'a serde_json::Value),
    Interval(Interval),
}

/// A send form, borrowed where the value already is one.
enum Sent<'a> {
    Borrowed(&'a [u8]),
    Fixed { bytes: [u8; 16], len: usize },
    Owned(Vec<u8>),
}

impl Sent<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Fixed { bytes, len } => &bytes[..*len],
            Self::Owned(bytes) => bytes,
        }
    }

    fn fixed(parts: &[&[u8]]) -> Self {
        let mut bytes = [0; 16];
        let mut len = 0;
        for part in parts {
            bytes[len..len + part.len()].copy_from_slice(part);
            len += part.len();
        }
        Self::Fixed { bytes, len }
    }
}

/// PostgreSQL's epoch, which its binary temporal forms count from.
const PG_EPOCH: chrono::NaiveDate = match chrono::NaiveDate::from_ymd_opt(2000, 1, 1) {
    Some(date) => date,
    None => panic!("2000-01-01 is a date"),
};

impl<'a> Cell<'a> {
    const fn family(&self) -> ScalarFamily {
        match self {
            Self::Bool(_) => ScalarFamily::Bool,
            Self::Int(_) => ScalarFamily::Int,
            Self::Float(_) => ScalarFamily::Float,
            Self::Text(_) => ScalarFamily::String,
            Self::Bytes(_) => ScalarFamily::Bytes,
            Self::Uuid(_) => ScalarFamily::Uuid,
            Self::Timestamp(_) => ScalarFamily::Timestamp,
            Self::TimestampTz(_) => ScalarFamily::TimestampTz,
            Self::Date(_) => ScalarFamily::Date,
            Self::Time(_) => ScalarFamily::Time,
            Self::Decimal(_) => ScalarFamily::Decimal,
            Self::Json(_) => ScalarFamily::Json,
            Self::Jsonb(_) => ScalarFamily::Jsonb,
            Self::Interval(_) => ScalarFamily::Interval,
        }
    }

    /// The value in its type's PostgreSQL send form, a float in `float4`'s
    /// when `single_precision`.
    fn send(&self, single_precision: bool) -> Result<Sent<'a>, StorageError> {
        use chrono::{Datelike as _, Timelike as _};

        Ok(match *self {
            Self::Bool(flag) => Sent::fixed(&[&[u8::from(flag)]]),
            Self::Int(integer) => Sent::fixed(&[&integer.to_be_bytes()]),
            Self::Float(double) if single_precision => {
                // Rounds to the nearest float4 as `at_float4` does. A value
                // subql folded is already on that grid, and a value a read
                // parsed at double precision lands where PostgreSQL held it.
                #[expect(clippy::cast_possible_truncation, reason = "float4 rounding")]
                let single = double as f32;
                Sent::fixed(&[&single.to_be_bytes()])
            }
            Self::Float(double) => Sent::fixed(&[&double.to_be_bytes()]),
            Self::Text(text) => Sent::Borrowed(text.as_bytes()),
            Self::Bytes(bytes) => Sent::Borrowed(bytes),
            Self::Uuid(uuid) => Sent::Borrowed(uuid),
            // chrono's range of dates is narrower than what `i64`
            // microseconds and `i32` days reach, so neither count overflows.
            Self::Timestamp(timestamp) | Self::TimestampTz(timestamp) => {
                let micros = timestamp.and_utc().timestamp_micros()
                    - PG_EPOCH
                        .and_time(chrono::NaiveTime::MIN)
                        .and_utc()
                        .timestamp_micros();
                Sent::fixed(&[&micros.to_be_bytes()])
            }
            Self::Date(date) => {
                let days = date.num_days_from_ce() - PG_EPOCH.num_days_from_ce();
                Sent::fixed(&[&days.to_be_bytes()])
            }
            Self::Time(time) => {
                let micros = i64::from(time.num_seconds_from_midnight()) * 1_000_000
                    + i64::from(time.nanosecond() / 1_000);
                Sent::fixed(&[&micros.to_be_bytes()])
            }
            Self::Interval(interval) => Sent::fixed(&[
                &interval.microseconds().to_be_bytes(),
                &interval.days().to_be_bytes(),
                &interval.months().to_be_bytes(),
            ]),
            Self::Decimal(decimal) => Sent::Owned(numeric_send(decimal)?),
            Self::Json(json) => Sent::Owned(json.to_string().into_bytes()),
            Self::Jsonb(jsonb) => {
                // `jsonb_send` is version byte 1 and then `jsonb_out`'s text.
                let mut sent = String::from('\u{1}');
                jsonb_out(jsonb, &mut sent)?;
                Sent::Owned(sent.into_bytes())
            }
        })
    }
}

/// Appends `jsonb_out`'s text of `value`: keys ordered by byte length and
/// then bytes, `", "` between items, `": "` after a key, and each number as
/// `numeric_out` prints the `numeric` jsonb stores it as.
fn jsonb_out(value: &serde_json::Value, out: &mut String) -> Result<(), StorageError> {
    match value {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        serde_json::Value::Number(number) => {
            let decimal: BigDecimal = number
                .as_str()
                .parse()
                .map_err(|_| StorageError::OutOfRange(ScalarFamily::Jsonb))?;
            // Writing to a `String` cannot fail.
            let _ = decimal.write_plain_string(out);
        }
        serde_json::Value::String(text) => escape_json(text, out),
        serde_json::Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                jsonb_out(item, out)?;
            }
            out.push(']');
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| {
                left.len().cmp(&right.len()).then_with(|| left.cmp(right))
            });
            out.push('{');
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                escape_json(key, out);
                out.push_str(": ");
                jsonb_out(item, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// Appends `text` quoted as PostgreSQL's `escape_json` writes it.
fn escape_json(text: &str, out: &mut String) {
    use core::fmt::Write as _;
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if control < ' ' => {
                // Writing to a `String` cannot fail.
                let _ = write!(out, "\\u{:04x}", u32::from(control));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

/// `numeric_send` of `decimal`: digit count, weight, sign and display
/// scale, then base-10000 digits from the most significant.
fn numeric_send(decimal: &BigDecimal) -> Result<Vec<u8>, StorageError> {
    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;
    const NUMERIC_MAX_DISPLAY_SCALE: i64 = 0x3FFF;
    let out_of_range = || StorageError::OutOfRange(ScalarFamily::Decimal);

    let (unscaled, exponent) = decimal.as_bigint_and_exponent();
    let mut digits = unscaled.magnitude().to_string().into_bytes();
    // A negative exponent counts zeros the unscaled digits leave off.
    let scale = if exponent < 0 {
        let zeros = usize::try_from(exponent.unsigned_abs()).map_err(|_| out_of_range())?;
        digits.resize(digits.len() + zeros, b'0');
        0
    } else {
        exponent
    };
    if scale > NUMERIC_MAX_DISPLAY_SCALE {
        return Err(out_of_range());
    }
    let length = i64::try_from(digits.len()).map_err(|_| out_of_range())?;
    // Digits ahead of the decimal point, which is zero or negative for a
    // value under one.
    let point = length - scale;
    // Leading zeros that put the point on a base-10000 boundary.
    let lead = (4 - point.rem_euclid(4)) % 4;
    let mut weight = (point + lead) / 4 - 1;
    let total = length + lead;
    let mut groups: Vec<i16> = Vec::with_capacity(usize::try_from(total / 4 + 1).unwrap_or(0));
    let mut at = -lead;
    while at < length {
        let mut group = 0_i16;
        for offset in at..at + 4 {
            let digit = usize::try_from(offset)
                .ok()
                .and_then(|index| digits.get(index))
                .map_or(0, |digit| i16::from(digit - b'0'));
            group = group * 10 + digit;
        }
        groups.push(group);
        at += 4;
    }
    let leading = groups.iter().take_while(|group| **group == 0).count();
    groups.drain(..leading);
    weight -= i64::try_from(leading).map_err(|_| out_of_range())?;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    let sign = if groups.is_empty() {
        weight = 0;
        NUMERIC_POS
    } else if unscaled.sign() == Sign::Minus {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };
    let count = i16::try_from(groups.len()).map_err(|_| out_of_range())?;
    let weight = i16::try_from(weight).map_err(|_| out_of_range())?;
    let scale = i16::try_from(scale).map_err(|_| out_of_range())?;
    let mut sent = Vec::with_capacity(8 + groups.len() * 2);
    sent.extend_from_slice(&count.to_be_bytes());
    sent.extend_from_slice(&weight.to_be_bytes());
    sent.extend_from_slice(&sign.to_be_bytes());
    sent.extend_from_slice(&scale.to_be_bytes());
    for group in groups {
        sent.extend_from_slice(&group.to_be_bytes());
    }
    Ok(sent)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use core::fmt::Write as _;

    use alloc::string::String;
    use alloc::vec::Vec;

    use bigdecimal::BigDecimal;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, RowData};
    use sqlite_diff_rs::{ParsedDiffSet, PatchsetOp, Value as WireValue};
    use sqlparser::dialect::PostgreSqlDialect;

    use super::{StorageEncoder, StorageError};
    use crate::backend::{Postgres, ScalarFamily, Value};
    use crate::emit::{pgoutput_patchset, StorageClass, StorageLayout, StorageType};
    use crate::{catalog_helpers, AggValue, NumericValue};

    /// One column per stored type, each with a value in PostgreSQL's text
    /// output, as pgoutput sends it. `json` is absent because subql holds it
    /// as a parsed document, which cannot give back the text the row keeps.
    const COLUMNS: &[(&str, &str)] = &[
        ("boolean", "t"),
        ("smallint", "-7"),
        ("integer", "42"),
        ("bigint", "-9007199254740993"),
        ("real", "1.5"),
        ("double precision", "0.1"),
        ("numeric", "-12.50"),
        ("numeric", "0.000120"),
        ("numeric", "0.00"),
        ("numeric", "123456789012345678901234567890"),
        ("numeric(12,2)", "12.50"),
        ("text", "h\u{e9}llo"),
        ("varchar(10)", "x"),
        ("char(3)", "ab "),
        ("bytea", "\\xff00"),
        ("bytea", "\\xfe00"),
        ("uuid", "550e8400-e29b-41d4-a716-446655440000"),
        ("timestamp", "2026-11-19 12:34:57"),
        ("timestamp", "1999-12-31 23:59:59.123456"),
        ("timestamptz", "2026-11-19 13:34:57.5+01"),
        ("date", "2026-11-19"),
        ("date", "1970-01-01"),
        ("time", "12:34:57.5"),
        ("interval", "1 year 2 mons -3 days +04:05:06.789"),
        (
            "jsonb",
            r#"{"a": 1, "c": {"d": 0.0, "e": 100}, "bb": [1.50, "x\ny", null, true]}"#,
        ),
        ("jsonb", r#"{"s": "q\"b\\s\bf\fr\rt\tc\u0001"}"#),
        ("integer[]", "{1,2,3}"),
        ("vector(3)", "[1,2,3]"),
        ("inet", "10.0.0.1/8"),
    ];

    fn database() -> ParserDB {
        let mut ddl = String::from("CREATE EXTENSION vector; CREATE TABLE t (id INT PRIMARY KEY");
        for (index, (declared, _)) in COLUMNS.iter().enumerate() {
            write!(ddl, ", c{index} {declared}").unwrap();
        }
        ddl.push_str(");");
        ParserDB::parse::<PostgreSqlDialect>(&ddl).unwrap()
    }

    /// What the replica stores for each column of one pgoutput insert.
    fn row(database: &ParserDB) -> Vec<WireValue<String, Vec<u8>>> {
        let mut cells = alloc::vec![(String::from("id"), ColumnValue::text("1"))];
        for (index, (_, text)) in COLUMNS.iter().enumerate() {
            cells.push((alloc::format!("c{index}"), ColumnValue::text(text)));
        }
        let event = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "t".into(),
                relation_oid: 1,
                data: RowData::from_pairs(
                    cells
                        .iter()
                        .map(|(name, cell)| (name.as_str(), cell.clone()))
                        .collect(),
                ),
            },
            lsn: Lsn::new(1),
            metadata: None,
            xid: None,
        };
        let bytes = pgoutput_patchset(database, core::slice::from_ref(&event)).unwrap();
        let ParsedDiffSet::Patchset(diff) = ParsedDiffSet::parse(&bytes).unwrap() else {
            panic!("expected a patchset");
        };
        let ops: Vec<_> = diff.iter().collect();
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert, got {:?}", ops[0]);
        };
        values[1..].to_vec()
    }

    /// The value subql holds for a cell: the row decode of its declared
    /// type, and the text a read hands back for a type it does not model.
    fn held(database: &ParserDB, column: u16, text: &str) -> Value<Postgres> {
        catalog_helpers::column_scalar_kind::<Postgres, _>(database, 0, column).map_or_else(
            || Value::String(text.into()),
            |kind| {
                crate::wal::pg_type::text_to_pg_value_by_kind(text, kind.declared_type().unwrap())
            },
        )
    }

    fn layout(database: &ParserDB, sql: &str) -> StorageLayout {
        let statement =
            crate::compiler::sql_shape::parse_single_statement(sql, &PostgreSqlDialect {}).unwrap();
        crate::compiler::storage_layout::storage_layout::<Postgres, _>(&statement, database, true)
            .unwrap()
    }

    const fn class(value: &WireValue<String, Vec<u8>>) -> Option<StorageClass> {
        match value {
            WireValue::Integer(_) => Some(StorageClass::Integer),
            WireValue::Real(_) => Some(StorageClass::Real),
            WireValue::Text(_) => Some(StorageClass::Text),
            WireValue::Blob(_) => Some(StorageClass::Blob),
            WireValue::Null => None,
        }
    }

    /// A value lands byte-identical as a row's cell, as a group value and as
    /// a `max` result, in the storage class its type declares.
    #[test]
    fn group_values_and_extremes_store_as_the_row_does() {
        let database = database();
        let encoder = StorageEncoder::new();
        let stored = row(&database);
        for (index, (declared, text)) in COLUMNS.iter().enumerate() {
            let column = u16::try_from(index + 1).unwrap();
            let value = held(&database, column, text);
            let grouped = layout(
                &database,
                &alloc::format!("SELECT count(*), c{index} FROM t GROUP BY c{index}"),
            );
            let group_type = grouped.outputs()[grouped.groups()[0].unwrap()];
            let extreme = layout(&database, &alloc::format!("SELECT max(c{index}) FROM t"));
            let max_type = extreme.outputs()[extreme.aggregate().unwrap()];
            assert_eq!(group_type, max_type, "{declared}");
            let as_group = encoder.value(&value, group_type).unwrap();
            let as_max = encoder.value(&value, max_type).unwrap();
            assert_eq!(
                as_group, stored[index],
                "{declared} {text} as a group value"
            );
            assert_eq!(as_max, stored[index], "{declared} {text} as a max result");
            assert_eq!(
                class(&as_group),
                group_type.map(StorageType::storage_class),
                "{declared} storage class"
            );
        }
    }

    /// Two byte strings that differ only past their first byte stay two
    /// values, which is what keeps two groups apart.
    #[test]
    fn distinct_bytea_groups_stay_distinct() {
        let encoder = StorageEncoder::new();
        let bytea = Some(StorageType::new(sqlite_diff_rs::WireType::Bytes));
        let first = encoder
            .value(&Value::<Postgres>::Bytes(alloc::vec![0xff, 0x00]), bytea)
            .unwrap();
        let second = encoder
            .value(&Value::<Postgres>::Bytes(alloc::vec![0xfe, 0x00]), bytea)
            .unwrap();
        assert_eq!(first, WireValue::Blob(alloc::vec![0xff, 0x00]));
        assert_eq!(second, WireValue::Blob(alloc::vec![0xfe, 0x00]));
    }

    /// A folded result lands as a row of the type its backend answers holds
    /// the same value.
    #[test]
    fn folded_results_store_as_their_answer_type_does() {
        let database = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE t (id INT PRIMARY KEY, i INT, n NUMERIC, r REAL, d DOUBLE PRECISION, \
             v INTERVAL);",
        )
        .unwrap();
        let encoder = StorageEncoder::new();
        let decimal = |text: &str| text.parse::<BigDecimal>().unwrap();
        let cases = [
            ("count(*)", AggValue::CountStar(3), WireValue::Integer(3)),
            (
                "sum(i)",
                AggValue::Sum(Some(NumericValue::Integer(-5))),
                WireValue::Integer(-5),
            ),
            (
                "sum(n)",
                AggValue::Sum(Some(NumericValue::Decimal(decimal("12.50")))),
                WireValue::Text("12.50".into()),
            ),
            (
                "avg(i)",
                AggValue::Avg(Some(NumericValue::Decimal(decimal("2.0000000000000000")))),
                WireValue::Text("2.0000000000000000".into()),
            ),
            (
                "sum(r)",
                AggValue::Sum(Some(NumericValue::Double(1.5))),
                WireValue::Real(1.5),
            ),
            (
                "avg(d)",
                AggValue::Avg(Some(NumericValue::Double(0.1))),
                WireValue::Real(0.1),
            ),
            (
                "var_pop(d)",
                AggValue::VarPop(Some(0.25)),
                WireValue::Real(0.25),
            ),
            (
                "sum(v)",
                AggValue::Sum(Some(NumericValue::Interval(
                    crate::backend::Interval::parse("1 day 02:00:00").unwrap(),
                ))),
                WireValue::Text("1 day 02:00:00".into()),
            ),
            (
                "sum(n)",
                AggValue::Sum(Some(NumericValue::Decimal(decimal("12E+2")))),
                WireValue::Text("1200".into()),
            ),
            ("sum(n)", AggValue::Sum(None), WireValue::Null),
            ("var_pop(d)", AggValue::VarPop(None), WireValue::Null),
        ];
        for (aggregate, value, expected) in cases {
            let layout = layout(&database, &alloc::format!("SELECT {aggregate} FROM t"));
            let storage_type = layout.outputs()[layout.aggregate().unwrap()];
            assert!(storage_type.is_some(), "{aggregate} has a type");
            assert_eq!(
                encoder.aggregate(&value, storage_type).unwrap(),
                expected,
                "{aggregate}"
            );
        }
    }

    /// A `json` value stores the document's compact text.
    #[test]
    fn json_stores_its_document_as_text() {
        let document = serde_json::from_str(r#"{ "a" : [1, "x"] }"#).unwrap();
        let stored = StorageEncoder::new()
            .value(
                &Value::<Postgres>::Json(document),
                Some(StorageType::new(sqlite_diff_rs::WireType::Json)),
            )
            .unwrap();
        assert_eq!(stored, WireValue::Text(r#"{"a":[1,"x"]}"#.into()));
    }

    /// A value with no storage form under its type is refused with the
    /// reason.
    #[test]
    fn values_without_a_storage_form_are_refused() {
        let database =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE a (id INT PRIMARY KEY, x INT[]);")
                .unwrap();
        let array = layout(&database, "SELECT x FROM a").outputs()[0];
        let uuid = Some(StorageType::new(sqlite_diff_rs::WireType::Uuid));
        let jsonb = Some(StorageType::new(sqlite_diff_rs::WireType::Jsonb));
        let encoder = StorageEncoder::default();
        assert_eq!(
            encoder.value(&Value::<Postgres>::Missing, uuid),
            Err(StorageError::Missing)
        );
        assert_eq!(
            encoder.value(&Value::<Postgres>::Int(1), uuid),
            Err(StorageError::Mismatch {
                family: ScalarFamily::Int,
                wire_type: sqlite_diff_rs::WireType::Uuid,
            })
        );
        assert_eq!(
            encoder.value(
                &Value::<Postgres>::Decimal("1E-16384".parse().unwrap()),
                None
            ),
            Err(StorageError::OutOfRange(ScalarFamily::Decimal))
        );
        assert_eq!(
            encoder.value(
                &Value::<Postgres>::Jsonb(
                    serde_json::from_str("[1e99999999999999999999]").unwrap()
                ),
                jsonb
            ),
            Err(StorageError::OutOfRange(ScalarFamily::Jsonb))
        );
        assert!(matches!(
            encoder.value(&Value::<Postgres>::String("{1,".into()), array),
            Err(StorageError::Decode(_))
        ));
    }
}
