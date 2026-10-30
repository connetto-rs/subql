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
use core::marker::PhantomData;

// diesel's `Output` is written through `std::io::Write`, which its PostgreSQL backend links.
extern crate std;
use std::io::Write;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::pg::data_types::PgNumeric;
use diesel::pg::Pg;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::{
    Array, BigInt, Bool, Date, Double, HasSqlType, Json as JsonSql, Jsonb, Numeric, Text, Time,
    Timestamp, Timestamptz,
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
    let elements = array
        .elements
        .iter()
        .map(|element| match element {
            Json::Null => Some(None),
            value => encode(value).map(Some),
        })
        .collect::<Option<Vec<_>>>()?;
    Some(bind_as::<Array<ST>, EncodedArray<ST>>(EncodedArray {
        dimensions: array.dimensions.clone(),
        elements,
        _element: PhantomData,
    }))
}

/// An array whose elements are already in their binary send format.
struct EncodedArray<ST> {
    dimensions: Vec<i32>,
    elements: Vec<Option<Vec<u8>>>,
    _element: PhantomData<fn() -> ST>,
}

impl<ST> core::fmt::Debug for EncodedArray<ST> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EncodedArray")
            .field("dimensions", &self.dimensions)
            .field("elements", &self.elements.len())
            .finish()
    }
}

impl<ST> ToSql<Array<ST>, Pg> for EncodedArray<ST>
where
    Pg: HasSqlType<ST>,
{
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let element_oid = Pg::metadata(out.metadata_lookup()).oid()?;
        out.write_all(&i32::try_from(self.dimensions.len())?.to_be_bytes())?;
        out.write_all(&i32::from(self.elements.iter().any(Option::is_none)).to_be_bytes())?;
        out.write_all(&element_oid.to_be_bytes())?;
        for &length in &self.dimensions {
            out.write_all(&length.to_be_bytes())?;
            out.write_all(&1_i32.to_be_bytes())?;
        }
        for element in &self.elements {
            match element {
                Some(bytes) => {
                    out.write_all(&i32::try_from(bytes.len())?.to_be_bytes())?;
                    out.write_all(bytes)?;
                }
                None => out.write_all(&(-1_i32).to_be_bytes())?,
            }
        }
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
        // A replica holds no array of BLOB-stored elements, and subql reads no interval text.
        ScalarFamily::Bytes | ScalarFamily::Uuid | ScalarFamily::Interval => None,
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
    let (sign, weight, scale, digits) = match PgNumeric::from(decimal) {
        PgNumeric::Positive {
            weight,
            scale,
            digits,
        } => (0_u16, weight, scale, digits),
        PgNumeric::Negative {
            weight,
            scale,
            digits,
        } => (0x4000, weight, scale, digits),
        PgNumeric::NaN => return None,
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
#[derive(Debug, Clone, PartialEq)]
pub struct PgVector(Vec<f32>);

impl PgVector {
    /// Read sqlite-vec's float32 BLOB, the elements as little-endian `f32`,
    /// or `None` when its length is not a whole number of elements.
    #[must_use]
    pub fn from_float32_blob(blob: &[u8]) -> Option<Self> {
        let (elements, rest) = blob.as_chunks::<4>();
        rest.is_empty().then(|| {
            Self(
                elements
                    .iter()
                    .map(|&bytes| f32::from_le_bytes(bytes))
                    .collect(),
            )
        })
    }

    /// The number of elements.
    #[must_use]
    pub const fn dimensions(&self) -> usize {
        self.0.len()
    }
}

impl ToSql<VectorType, Pg> for PgVector {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(&u16::try_from(self.0.len())?.to_be_bytes())?;
        out.write_all(&0_u16.to_be_bytes())?;
        for &element in &self.0 {
            out.write_all(&element.to_be_bytes())?;
        }
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
    if dimensions
        .is_some_and(|expected| usize::try_from(expected).ok() != Some(vector.dimensions()))
    {
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
        assert_eq!(encode_bool(&json!(2)), None);
        assert_eq!(encode_int(&json!("1")), None);
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
        assert_eq!(encode_date(&json!("yesterday")), None);
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
        assert_eq!(encode_decimal(&json!(true)), None);
    }

    #[test]
    fn json_elements_must_hold_json_text() {
        assert_eq!(
            encode_json(&json!("{\"a\":1}")),
            Some(b"{\"a\":1}".to_vec())
        );
        assert_eq!(encode_jsonb(&json!("[1]")), Some(b"\x01[1]".to_vec()));
        assert_eq!(encode_json(&json!("{")), None);
        assert_eq!(encode_jsonb(&json!(1)), None);
    }

    #[test]
    fn an_array_binds_only_when_every_element_encodes() {
        let ints = PgArray::parse("[1,null,2]").unwrap();
        assert!(bind_builtin_array(ScalarFamily::Int, &ints).is_some());
        assert!(bind_builtin_array(ScalarFamily::Date, &ints).is_none());
        for family in [
            ScalarFamily::Bytes,
            ScalarFamily::Uuid,
            ScalarFamily::Interval,
        ] {
            assert!(bind_builtin_array(family, &ints).is_none(), "{family:?}");
        }
    }

    #[test]
    fn a_vector_reads_little_endian_float32_and_checks_its_dimensions() {
        let blob: Vec<u8> = [1.0_f32, -2.5]
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();
        assert_eq!(
            PgVector::from_float32_blob(&blob),
            Some(PgVector(alloc::vec![1.0, -2.5]))
        );
        assert_eq!(PgVector::from_float32_blob(&blob[..7]), None);
        assert!(bind_vector(&blob, Some(2)).is_some());
        assert!(bind_vector(&blob, None).is_some());
        assert!(bind_vector(&blob, Some(3)).is_none());
    }
}
