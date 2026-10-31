//! Native binds for the PostgreSQL arrays and pgvector vectors a replica
//! uploads.
//!
//! A pg2sqlite replica holds an array as JSON array text and a `vector(n)` as
//! sqlite-vec's float32 BLOB, and PostgreSQL takes neither as a text or bytea
//! bind. Each is rebuilt here in PostgreSQL's binary send format and bound
//! natively. An array binds as the widest type of its element family, such as
//! `int8[]`, and PostgreSQL's assignment cast narrows it to the column's own
//! element type.

use alloc::boxed::Box;
use alloc::string::ToString;
use alloc::vec::Vec;

// diesel's `Output` is written through `std::io::Write`, which its PostgreSQL backend links.
extern crate std;
use std::io::Write;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::pg::data_types::PgNumeric;
use diesel::pg::Pg;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::{
    Array, BigInt, Bool, Date, Double, HasSqlType, Interval as IntervalSql, Json as JsonSql, Jsonb,
    Numeric, Text, Time, Timestamp, Timestamptz,
};
use serde_json::Value as Json;
use sqlite_diff_rs::Binder;

use super::bind_as;
use crate::backend::ScalarFamily;

/// A PostgreSQL array read from a replica's JSON array text, its elements in
/// row-major order.
///
/// `[1,null,3]` is one dimension of three elements, `[[1,2],[3,4]]` two
/// dimensions of two, and `[]` an empty array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgArray {
    dimensions: Vec<i32>,
    elements: Vec<Json>,
}

impl PgArray {
    /// Read `text` as a JSON array, or `None` when it is not one or is not
    /// rectangular, which a PostgreSQL array must be.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let Json::Array(top) = serde_json::from_str::<Json>(text).ok()? else {
            return None;
        };
        let mut dimensions = Vec::new();
        let mut level = &top;
        loop {
            dimensions.push(i32::try_from(level.len()).ok()?);
            match level.first() {
                Some(Json::Array(inner)) => level = inner,
                _ => break,
            }
        }
        let mut elements = Vec::new();
        flatten(&top, &dimensions, &mut elements)?;
        // PostgreSQL holds every empty array as zero-dimensional.
        if elements.is_empty() {
            dimensions.clear();
        }
        Some(Self {
            dimensions,
            elements,
        })
    }

    /// The elements in row-major order, `Json::Null` for a NULL element.
    #[must_use]
    pub fn elements(&self) -> &[Json] {
        &self.elements
    }
}

/// Append `level`'s elements to `out`, refusing a level whose length differs
/// from its dimension.
fn flatten(level: &[Json], dimensions: &[i32], out: &mut Vec<Json>) -> Option<()> {
    let (&length, inner) = dimensions.split_first().unwrap_or((&0, &[]));
    if usize::try_from(length).ok()? != level.len() {
        return None;
    }
    for item in level {
        match (item, inner.is_empty()) {
            (Json::Array(next), false) => flatten(next, inner, out)?,
            (Json::Array(_), true) | (_, false) => return None,
            (scalar, true) => out.push(scalar.clone()),
        }
    }
    Some(())
}

/// Box a native bind of `array` as `ST[]`, each non-null element encoded by
/// `encode` into `ST`'s binary send format, or `None` when an element does
/// not encode.
#[must_use]
pub fn bind_array_as<ST>(
    array: &PgArray,
    encode: impl Fn(&Json) -> Option<Vec<u8>>,
) -> Option<Box<dyn Binder<Pg> + Send>>
where
    ST: Send + 'static,
    Pg: HasSqlType<ST>,
{
    Some(bind_as::<Array<ST>, EncodedArray>(EncodedArray::new(
        array, encode,
    )?))
}

/// An array in PostgreSQL's binary send format, less the element type's OID,
/// which is resolved from the database at execute time.
#[derive(Debug)]
struct EncodedArray {
    /// The dimension count and the has-null flag.
    header: [u8; 8],
    /// Each dimension's length and lower bound, then each element's length and bytes.
    body: Vec<u8>,
}

impl EncodedArray {
    fn new(array: &PgArray, encode: impl Fn(&Json) -> Option<Vec<u8>>) -> Option<Self> {
        let mut body = Vec::new();
        for &length in &array.dimensions {
            body.extend_from_slice(&length.to_be_bytes());
            body.extend_from_slice(&1_i32.to_be_bytes());
        }
        let mut has_null = false;
        for element in &array.elements {
            if element.is_null() {
                has_null = true;
                body.extend_from_slice(&(-1_i32).to_be_bytes());
            } else {
                let bytes = encode(element)?;
                body.extend_from_slice(&i32::try_from(bytes.len()).ok()?.to_be_bytes());
                body.extend_from_slice(&bytes);
            }
        }
        let mut header = [0; 8];
        header[..4].copy_from_slice(&i32::try_from(array.dimensions.len()).ok()?.to_be_bytes());
        header[4..].copy_from_slice(&i32::from(has_null).to_be_bytes());
        Some(Self { header, body })
    }
}

impl<ST> ToSql<Array<ST>, Pg> for EncodedArray
where
    Pg: HasSqlType<ST>,
{
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let element_oid = Pg::metadata(out.metadata_lookup()).oid()?;
        out.write_all(&self.header)?;
        out.write_all(&element_oid.to_be_bytes())?;
        out.write_all(&self.body)?;
        Ok(IsNull::No)
    }
}

/// Bind an array of a builtin `family`, or `None` when the family has no
/// array form in a replica or an element does not encode.
pub(super) fn bind_builtin_array(
    family: ScalarFamily,
    array: &PgArray,
) -> Option<Box<dyn Binder<Pg> + Send>> {
    match family {
        ScalarFamily::Bool => bind_array_as::<Bool>(array, encode_bool),
        ScalarFamily::Int => bind_array_as::<BigInt>(array, encode_int),
        ScalarFamily::Float => bind_array_as::<Double>(array, encode_float),
        ScalarFamily::Decimal => bind_array_as::<Numeric>(array, encode_decimal),
        ScalarFamily::String => bind_array_as::<Text>(array, encode_text),
        ScalarFamily::Date => bind_array_as::<Date>(array, encode_date),
        ScalarFamily::Time => bind_array_as::<Time>(array, encode_time),
        ScalarFamily::Timestamp => bind_array_as::<Timestamp>(array, encode_timestamp),
        ScalarFamily::TimestampTz => bind_array_as::<Timestamptz>(array, encode_timestamptz),
        ScalarFamily::Json => bind_array_as::<JsonSql>(array, encode_json),
        ScalarFamily::Jsonb => bind_array_as::<Jsonb>(array, encode_jsonb),
        ScalarFamily::Interval => bind_array_as::<IntervalSql>(array, encode_interval),
        // A replica holds no array of BLOB-stored elements.
        ScalarFamily::Bytes | ScalarFamily::Uuid => None,
    }
}

fn encode_int(value: &Json) -> Option<Vec<u8>> {
    Some(value.as_i64()?.to_be_bytes().to_vec())
}

fn encode_float(value: &Json) -> Option<Vec<u8>> {
    Some(value.as_f64()?.to_be_bytes().to_vec())
}

fn encode_text(value: &Json) -> Option<Vec<u8>> {
    Some(value.as_str()?.as_bytes().to_vec())
}

fn encode_date(value: &Json) -> Option<Vec<u8>> {
    let days = (sql_scalar_text::parse_date(value.as_str()?)? - pg_epoch_date()).num_days();
    Some(i32::try_from(days).ok()?.to_be_bytes().to_vec())
}

/// An interval element, its binary format being microseconds, days, then months.
fn encode_interval(value: &Json) -> Option<Vec<u8>> {
    let interval = sql_scalar_text::parse_interval(value.as_str()?)?;
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&interval.microseconds.to_be_bytes());
    bytes.extend_from_slice(&interval.days.to_be_bytes());
    bytes.extend_from_slice(&interval.months.to_be_bytes());
    Some(bytes)
}

fn encode_time(value: &Json) -> Option<Vec<u8>> {
    let time = sql_scalar_text::parse_time(value.as_str()?)?;
    Some(
        (time - NaiveTime::MIN)
            .num_microseconds()?
            .to_be_bytes()
            .to_vec(),
    )
}

fn encode_timestamp(value: &Json) -> Option<Vec<u8>> {
    micros_since_pg_epoch(sql_scalar_text::parse_timestamp(value.as_str()?)?)
}

fn encode_timestamptz(value: &Json) -> Option<Vec<u8>> {
    let instant: DateTime<Utc> = sql_scalar_text::parse_timestamp_tz(value.as_str()?)?;
    micros_since_pg_epoch(instant.naive_utc())
}

/// A JSON element, which the replica holds as a JSON string of its text.
fn encode_json(value: &Json) -> Option<Vec<u8>> {
    let text = value.as_str()?;
    serde_json::from_str::<Json>(text).ok()?;
    Some(text.as_bytes().to_vec())
}

/// A `jsonb` element, its binary format being a version byte before the text.
fn encode_jsonb(value: &Json) -> Option<Vec<u8>> {
    let mut bytes = alloc::vec![1];
    bytes.extend(encode_json(value)?);
    Some(bytes)
}

/// `true`/`false`, or the `1`/`0` SQLite's `json_array` writes for a boolean.
fn encode_bool(value: &Json) -> Option<Vec<u8>> {
    let flag = match value {
        Json::Bool(flag) => *flag,
        Json::Number(number) => match number.as_i64()? {
            0 => false,
            1 => true,
            _ => return None,
        },
        _ => return None,
    };
    Some(alloc::vec![u8::from(flag)])
}

/// A decimal element, written as a JSON string or number, in `numeric`'s
/// binary send format.
fn encode_decimal(value: &Json) -> Option<Vec<u8>> {
    let decimal = match value {
        Json::String(text) => sql_scalar_text::parse_decimal(text)?,
        Json::Number(number) => sql_scalar_text::parse_decimal(&number.to_string())?,
        _ => return None,
    };
    let numeric = PgNumeric::from(decimal);
    let sign: u16 = if matches!(numeric, PgNumeric::Negative { .. }) {
        0x4000
    } else {
        0
    };
    let (PgNumeric::Positive {
        weight,
        scale,
        digits,
    }
    | PgNumeric::Negative {
        weight,
        scale,
        digits,
    }) = numeric
    else {
        return None;
    };
    let mut bytes = Vec::with_capacity(8 + 2 * digits.len());
    bytes.extend_from_slice(&u16::try_from(digits.len()).ok()?.to_be_bytes());
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&scale.to_be_bytes());
    for digit in digits {
        bytes.extend_from_slice(&digit.to_be_bytes());
    }
    Some(bytes)
}

/// PostgreSQL's epoch, which its binary date and timestamp formats count from.
fn pg_epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or_default()
}

fn micros_since_pg_epoch(timestamp: NaiveDateTime) -> Option<Vec<u8>> {
    let epoch = pg_epoch_date().and_time(NaiveTime::MIN);
    Some(
        (timestamp - epoch)
            .num_microseconds()?
            .to_be_bytes()
            .to_vec(),
    )
}

/// pgvector's `vector` type, resolved by name so its OID comes from the
/// database the bind runs against.
#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "vector"))]
pub struct VectorType;

/// A single-precision vector in pgvector's binary send format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgVector {
    dimensions: u16,
    /// The dimension count, an unused word, then each element as big-endian `f32`.
    bytes: Vec<u8>,
}

impl PgVector {
    /// Read sqlite-vec's float32 BLOB, the elements as little-endian `f32`,
    /// or `None` when its length is not a whole number of elements or it
    /// holds more elements than pgvector's `u16` dimension count can name.
    #[must_use]
    pub fn from_float32_blob(blob: &[u8]) -> Option<Self> {
        let (elements, rest) = blob.as_chunks::<4>();
        if !rest.is_empty() {
            return None;
        }
        let dimensions = u16::try_from(elements.len()).ok()?;
        let mut bytes = Vec::with_capacity(4 + blob.len());
        bytes.extend_from_slice(&dimensions.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        for &element in elements {
            bytes.extend_from_slice(&f32::from_le_bytes(element).to_be_bytes());
        }
        Some(Self { dimensions, bytes })
    }

    /// The number of elements.
    #[must_use]
    pub const fn dimensions(&self) -> u16 {
        self.dimensions
    }
}

impl ToSql<VectorType, Pg> for PgVector {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(&self.bytes)?;
        Ok(IsNull::No)
    }
}

/// Bind sqlite-vec's float32 BLOB as a pgvector `vector`, or `None` when the
/// BLOB is not a whole number of `f32` elements or not `dimensions` of them.
pub(super) fn bind_vector(
    blob: &[u8],
    dimensions: Option<u32>,
) -> Option<Box<dyn Binder<Pg> + Send>> {
    let vector = PgVector::from_float32_blob(blob)?;
    if dimensions.is_some_and(|expected| expected != u32::from(vector.dimensions())) {
        return None;
    }
    Some(bind_as::<VectorType, PgVector>(vector))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_json_array_reads_as_its_dimensions_and_row_major_elements() {
        let flat = PgArray::parse("[1,null,3]").unwrap();
        assert_eq!(flat.dimensions, [3]);
        assert_eq!(flat.elements(), [json!(1), Json::Null, json!(3)]);

        let grid = PgArray::parse("[[1,2],[3,4]]").unwrap();
        assert_eq!(grid.dimensions, [2, 2]);
        assert_eq!(grid.elements(), [json!(1), json!(2), json!(3), json!(4)]);
    }

    /// PostgreSQL holds an empty array as zero-dimensional, however it nests.
    #[test]
    fn an_empty_array_has_no_dimensions() {
        for text in ["[]", "[[]]", "[[],[]]"] {
            assert_eq!(PgArray::parse(text).unwrap().dimensions, Vec::<i32>::new());
        }
    }

    #[test]
    fn a_ragged_or_non_array_text_is_refused() {
        for text in [
            "[[1],[2,3]]",
            "[[1],2]",
            "[1,[2]]",
            "{}",
            "\"x\"",
            "not json",
        ] {
            assert_eq!(PgArray::parse(text), None, "{text}");
        }
    }

    #[test]
    fn scalar_elements_encode_in_their_binary_send_format() {
        assert_eq!(
            encode_int(&json!(-2)),
            Some((-2_i64).to_be_bytes().to_vec())
        );
        assert_eq!(
            encode_float(&json!(1.5)),
            Some(1.5_f64.to_be_bytes().to_vec())
        );
        assert_eq!(encode_text(&json!("hé")), Some("hé".as_bytes().to_vec()));
        assert_eq!(encode_bool(&json!(true)), Some(alloc::vec![1]));
        assert_eq!(encode_bool(&json!(0)), Some(alloc::vec![0]));
        assert_eq!(encode_bool(&json!(2)), None, "only 0 and 1 are booleans");
        assert_eq!(encode_bool(&json!(0.5)), None, "a fraction is no boolean");
    }

    /// An element of the wrong JSON type, or text its type cannot read,
    /// does not encode, which refuses the whole array.
    #[test]
    fn an_element_that_does_not_read_as_its_type_does_not_encode() {
        type Encode = fn(&Json) -> Option<Vec<u8>>;
        let refusals: [(&str, Encode, [Json; 2]); 12] = [
            ("int", encode_int, [json!(1.5), json!("1")]),
            ("float", encode_float, [json!("1.5"), json!(true)]),
            ("text", encode_text, [json!(1), json!(true)]),
            ("bool", encode_bool, [json!("true"), json!(null)]),
            ("decimal", encode_decimal, [json!("twelve"), json!(true)]),
            ("date", encode_date, [json!(1), json!("yesterday")]),
            ("time", encode_time, [json!(1), json!("noon")]),
            ("timestamp", encode_timestamp, [json!(1), json!("noon")]),
            ("timestamptz", encode_timestamptz, [json!(1), json!("noon")]),
            ("json", encode_json, [json!(1), json!("{")]),
            ("jsonb", encode_jsonb, [json!(1), json!("{")]),
            (
                "interval",
                encode_interval,
                [json!(1), json!("1 fortnight")],
            ),
        ];
        for (name, encode, values) in refusals {
            for value in values {
                assert_eq!(encode(&value), None, "{name} {value}");
            }
        }
    }

    /// Dates and times count from PostgreSQL's 2000-01-01 epoch.
    #[test]
    fn temporal_elements_count_from_the_postgres_epoch() {
        assert_eq!(
            encode_date(&json!("2000-01-02")),
            Some(1_i32.to_be_bytes().to_vec())
        );
        let second = 1_000_000_i64.to_be_bytes().to_vec();
        assert_eq!(encode_time(&json!("00:00:01")), Some(second.clone()));
        assert_eq!(
            encode_timestamp(&json!("2000-01-01 00:00:01")),
            Some(second.clone())
        );
        assert_eq!(
            encode_timestamptz(&json!("2000-01-01 01:00:01+01:00")),
            Some(second)
        );
    }

    /// `12.50` is base-10000 digits `12` and `5000` at weight 0 and scale 2.
    #[test]
    fn a_decimal_element_encodes_as_numeric() {
        let mut expected = Vec::new();
        for word in [2_u16, 0, 0, 2, 12, 5000] {
            expected.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(encode_decimal(&json!("12.50")), Some(expected));
        assert_eq!(
            encode_decimal(&json!("-1")).unwrap()[4..6],
            0x4000_u16.to_be_bytes()
        );
        assert_eq!(encode_decimal(&json!(12.5)), encode_decimal(&json!("12.5")));
    }

    #[test]
    fn json_elements_must_hold_json_text() {
        assert_eq!(
            encode_json(&json!("{\"a\":1}")),
            Some(b"{\"a\":1}".to_vec())
        );
        assert_eq!(encode_jsonb(&json!("[1]")), Some(b"\x01[1]".to_vec()));
    }

    /// `[[7],[null]]` is two dimensions of lengths 2 and 1, each from lower
    /// bound 1, flagged as holding a NULL, its NULL element of length -1.
    #[test]
    fn an_array_encodes_its_dimensions_nulls_and_element_lengths() {
        let words =
            |words: &[i32]| -> Vec<u8> { words.iter().flat_map(|w| w.to_be_bytes()).collect() };
        let encoded =
            EncodedArray::new(&PgArray::parse("[[7],[null]]").unwrap(), encode_int).unwrap();
        assert_eq!(encoded.header.to_vec(), words(&[2, 1]));
        let mut body = words(&[2, 1, 1, 1, 8]);
        body.extend_from_slice(&7_i64.to_be_bytes());
        body.extend(words(&[-1]));
        assert_eq!(encoded.body, body);
        let without_null = EncodedArray::new(&PgArray::parse("[1]").unwrap(), encode_int).unwrap();
        assert_eq!(without_null.header.to_vec(), words(&[1, 0]));
    }

    /// Every builtin family with a replica array form binds its own elements.
    #[test]
    fn every_builtin_family_binds_an_array_of_its_elements() {
        for (family, text) in [
            (ScalarFamily::Bool, "[true,0]"),
            (ScalarFamily::Int, "[1,null,2]"),
            (ScalarFamily::Float, "[1.5,2]"),
            (ScalarFamily::Decimal, "[\"12.50\",3]"),
            (ScalarFamily::String, "[\"a\"]"),
            (ScalarFamily::Date, "[\"2024-03-04\"]"),
            (ScalarFamily::Time, "[\"05:06:07\"]"),
            (ScalarFamily::Timestamp, "[\"2024-03-04 05:06:07\"]"),
            (ScalarFamily::TimestampTz, "[\"2024-03-04 05:06:07+00:00\"]"),
            (ScalarFamily::Json, "[\"{}\"]"),
            (ScalarFamily::Jsonb, "[\"[]\"]"),
            (ScalarFamily::Interval, "[\"00:15:00\",null]"),
        ] {
            let array = PgArray::parse(text).unwrap();
            assert!(bind_builtin_array(family, &array).is_some(), "{family:?}");
        }
    }

    #[test]
    fn an_array_binds_only_when_every_element_encodes() {
        let ints = PgArray::parse("[1,null,2]").unwrap();
        assert!(bind_builtin_array(ScalarFamily::Date, &ints).is_none());
        for family in [ScalarFamily::Bytes, ScalarFamily::Uuid] {
            assert!(bind_builtin_array(family, &ints).is_none(), "{family:?}");
        }
    }

    /// An interval element keeps its microseconds, days and months apart, in
    /// PostgreSQL's binary order.
    #[test]
    fn an_interval_element_encodes_its_three_fields() {
        let mut expected = 14_706_789_000_i64.to_be_bytes().to_vec();
        expected.extend_from_slice(&(-3_i32).to_be_bytes());
        expected.extend_from_slice(&14_i32.to_be_bytes());
        assert_eq!(
            encode_interval(&json!("1 year 2 mons -3 days +04:05:06.789")),
            Some(expected)
        );
    }

    #[test]
    fn a_vector_reads_little_endian_float32_and_checks_its_dimensions() {
        let blob: Vec<u8> = [1.0_f32, -2.5]
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();
        let vector = PgVector::from_float32_blob(&blob).unwrap();
        assert_eq!(vector.dimensions(), 2);
        let mut expected = alloc::vec![0, 2, 0, 0];
        expected.extend_from_slice(&1.0_f32.to_be_bytes());
        expected.extend_from_slice(&(-2.5_f32).to_be_bytes());
        assert_eq!(vector.bytes, expected);
        assert_eq!(PgVector::from_float32_blob(&blob[..7]), None);
        assert!(bind_vector(&blob, Some(2)).is_some());
        assert!(bind_vector(&blob, None).is_some());
        assert!(bind_vector(&blob, Some(3)).is_none());
        assert!(bind_vector(&blob[..7], None).is_none());
    }

    /// pgvector names its dimension count in a `u16`.
    #[test]
    fn a_vector_longer_than_a_u16_count_is_refused() {
        let longest = alloc::vec![0_u8; 4 * usize::from(u16::MAX)];
        assert_eq!(
            PgVector::from_float32_blob(&longest).map(|v| v.dimensions()),
            Some(u16::MAX)
        );
        let too_long = alloc::vec![0_u8; longest.len() + 4];
        assert_eq!(PgVector::from_float32_blob(&too_long), None);
    }
}
