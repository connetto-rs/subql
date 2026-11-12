//! One table's shape as a backend declares it, and the facts a drift
//! judgment compares.
//!
//! A [`TableShape`] is one side of the judgment: the catalog's table, or
//! the live table as the database has it now. It carries the columns in
//! order, each with its declared type, nullability and default presence,
//! the primary key columns in key order, the `CHECK` constraint
//! expressions in declaration order, and the replica identity where the
//! backend has one. A catalog side built from a parser catalog has no
//! replica identity, since a `CREATE TABLE` never declares one.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sql_traits::structs::ParserDB;
use sql_traits::traits::CheckConstraintLike;
use sql_traits::utils::normalize_sqlparser_type;
use sqlparser::ast::{
    ArrayElemTypeDef, BinaryLength, CharLengthUnits, CharacterLength, ColumnOptionDef, DataType,
    EnumMember, ExactNumberInfo, Ident, IntervalFields, ObjectName, SqlOption,
};

/// A catalog that exposes the raw sqlparser type a column declares.
///
/// The judgment compares declared bounds, and the normalization the
/// trait's `data_type` spelling keeps no memory of, so the raw type has
/// to be read as a sqlparser [`DataType`]. A `DatabaseLike` whose
/// `DataType` is one answers this with a single coercion.
pub trait SqlparserDatabase: DatabaseLike {
    /// The raw declared type of `column`.
    fn column_raw_type<'db>(column: &'db Self::Column, database: &'db Self) -> &'db DataType;
}

impl SqlparserDatabase for ParserDB {
    fn column_raw_type<'db>(
        column: &'db <Self as DatabaseLike>::Column,
        database: &'db Self,
    ) -> &'db DataType {
        ColumnLike::data_type(column, database)
    }
}

/// The replica identity a backend applies to a table's updates and
/// deletes, where the backend has one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
#[non_exhaustive]
pub enum ReplicaIdentity {
    /// The backend states no identity for the table.
    #[default]
    NotSpecified,
    /// The backend's default identity, key columns only.
    Default,
    /// A user index other than the primary key.
    Index,
    /// Every column of the key.
    All,
    /// Every column of the row.
    Full,
}

/// Declared type parameters omitted by family normalization.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TypeDetail {
    Exact(u64),
    Scale(i64),
    Max,
    Unsigned,
    Nullable,
    NotNullElements,
    CharacterUnits(CharLengthUnits),
    ArraySize(Option<u64>),
    TimeZone(String),
    IntervalFields(IntervalFields),
    Enum(Vec<EnumMember>, Option<u8>),
    Set(Vec<String>),
    Fields(Vec<FieldShape>),
    Elements(Vec<TypeShape>),
    Name(ObjectName),
    ColumnOptions(Vec<ColumnOptionDef>),
    StructOptions(Vec<SqlOption>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FieldShape {
    name: Option<Ident>,
    data_type: TypeShape,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TypeShape {
    normalized: String,
    details: Vec<TypeDetail>,
}

impl TypeShape {
    fn new(data_type: &DataType) -> Self {
        let mut details = Vec::new();
        append_type_details(data_type, &mut details);
        Self {
            normalized: normalize_sqlparser_type(data_type).into_owned(),
            details,
        }
    }
}

/// One column of a [`TableShape`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnShape {
    name: String,
    data_type: TypeShape,
    nullable: bool,
    has_default: bool,
}

impl ColumnShape {
    /// The column's name, unqualified as the catalog stores it.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared type under its normalized spelling.
    #[must_use]
    pub fn type_normalized(&self) -> &str {
        &self.data_type.normalized
    }

    /// Whether the column accepts `NULL`.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Whether the column declares a default.
    #[must_use]
    pub const fn has_default(&self) -> bool {
        self.has_default
    }

    /// Whether the declared type, bounds and members match `other`'s.
    #[must_use]
    pub fn type_matches(&self, other: &Self) -> bool {
        self.data_type == other.data_type
    }
}

/// One table's shape, one side of the drift judgment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableShape {
    columns: Vec<ColumnShape>,
    /// The primary key's column names, in the order the key declares them.
    primary_key: Vec<String>,
    /// The `CHECK` constraint expressions, in declaration order.
    checks: Vec<String>,
    /// The replica identity, where the backend has one.
    replica_identity: Option<ReplicaIdentity>,
}

impl TableShape {
    /// Build the shape of `table` as `database` declares it.
    ///
    /// # Errors
    ///
    /// A [`sql_traits::errors::LookupError`] when a column, key or check
    /// lookup on the table fails.
    pub fn from_table<DB: SqlparserDatabase>(
        table: &DB::Table,
        database: &DB,
    ) -> Result<Self, sql_traits::errors::LookupError> {
        let mut columns = Vec::new();
        for column in table.columns(database)? {
            let data_type = DB::column_raw_type(column, database);
            columns.push(ColumnShape {
                name: column.column_name().to_string(),
                data_type: TypeShape::new(data_type),
                nullable: column.is_nullable(database)?,
                has_default: column.has_default(),
            });
        }
        let primary_key = table
            .primary_key_columns(database)?
            .map(|column| column.column_name().to_string())
            .collect();
        let checks = table
            .check_constraints(database)?
            .map(|check| check.expression(database).to_string())
            .collect();
        Ok(Self {
            columns,
            primary_key,
            checks,
            replica_identity: None,
        })
    }

    /// State the replica identity on a shape the live side read from a
    /// backend that has one.
    #[must_use]
    pub const fn with_replica_identity(mut self, identity: ReplicaIdentity) -> Self {
        self.replica_identity = Some(identity);
        self
    }

    /// The columns in table order.
    #[must_use]
    pub fn columns(&self) -> &[ColumnShape] {
        &self.columns
    }

    /// The primary key's column names, in key declaration order.
    #[must_use]
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// The `CHECK` constraint expressions, in declaration order.
    #[must_use]
    pub fn checks(&self) -> &[String] {
        &self.checks
    }

    /// The replica identity, where the backend has one.
    #[must_use]
    pub const fn replica_identity(&self) -> Option<ReplicaIdentity> {
        self.replica_identity
    }
}

/// Append declared parameters, retaining their positions inside arrays and compound types.
#[expect(
    clippy::too_many_lines,
    reason = "The exhaustive match accounts for every parser type's metadata."
)]
fn append_type_details(data_type: &DataType, details: &mut Vec<TypeDetail>) {
    if matches!(
        data_type,
        DataType::TinyIntUnsigned(_)
            | DataType::SmallIntUnsigned(_)
            | DataType::MediumIntUnsigned(_)
            | DataType::IntUnsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::BigIntUnsigned(_)
            | DataType::Int2Unsigned(_)
            | DataType::Int4Unsigned(_)
            | DataType::Int8Unsigned(_)
            | DataType::UTinyInt
            | DataType::USmallInt
            | DataType::UBigInt
            | DataType::UHugeInt
            | DataType::Unsigned
            | DataType::UnsignedInteger
            | DataType::DecimalUnsigned(_)
            | DataType::DecUnsigned(_)
            | DataType::FloatUnsigned(_)
            | DataType::RealUnsigned
            | DataType::DoubleUnsigned(_)
            | DataType::DoublePrecisionUnsigned
    ) {
        details.push(TypeDetail::Unsigned);
    }
    match data_type {
        DataType::Character(length)
        | DataType::Char(length)
        | DataType::CharacterVarying(length)
        | DataType::CharVarying(length)
        | DataType::Varchar(length)
        | DataType::Nvarchar(length) => match length {
            None => {}
            Some(CharacterLength::IntegerLength { length, unit }) => {
                details.push(TypeDetail::Exact(*length));
                if let Some(unit) = unit {
                    details.push(TypeDetail::CharacterUnits(*unit));
                }
            }
            Some(CharacterLength::Max) => details.push(TypeDetail::Max),
        },
        DataType::Clob(width)
        | DataType::CharacterLargeObject(width)
        | DataType::CharLargeObject(width)
        | DataType::Binary(width)
        | DataType::Blob(width)
        | DataType::Bytes(width)
        | DataType::String(width)
        | DataType::TinyInt(width)
        | DataType::SmallInt(width)
        | DataType::MediumInt(width)
        | DataType::Int(width)
        | DataType::Integer(width)
        | DataType::BigInt(width)
        | DataType::Bit(width)
        | DataType::BitVarying(width)
        | DataType::VarBit(width)
        | DataType::Int2(width)
        | DataType::Int4(width)
        | DataType::Int8(width)
        | DataType::TinyIntUnsigned(width)
        | DataType::SmallIntUnsigned(width)
        | DataType::MediumIntUnsigned(width)
        | DataType::IntUnsigned(width)
        | DataType::IntegerUnsigned(width)
        | DataType::BigIntUnsigned(width)
        | DataType::Int2Unsigned(width)
        | DataType::Int4Unsigned(width)
        | DataType::Int8Unsigned(width)
        | DataType::Time(width, _)
        | DataType::Timestamp(width, _)
        | DataType::TimestampNtz(width)
        | DataType::Datetime(width) => details.extend(width.map(TypeDetail::Exact)),
        DataType::Float(info)
        | DataType::FloatUnsigned(info)
        | DataType::Numeric(info)
        | DataType::Decimal(info)
        | DataType::DecimalUnsigned(info)
        | DataType::Dec(info)
        | DataType::DecUnsigned(info)
        | DataType::BigNumeric(info)
        | DataType::BigDecimal(info)
        | DataType::Double(info)
        | DataType::DoubleUnsigned(info) => match info {
            ExactNumberInfo::None => {}
            ExactNumberInfo::Precision(precision) => details.push(TypeDetail::Exact(*precision)),
            ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                details.push(TypeDetail::Exact(*precision));
                details.push(TypeDetail::Scale(*scale));
            }
        },
        DataType::FixedString(width) => details.push(TypeDetail::Exact(*width)),
        DataType::Varbinary(length) => match length {
            None => {}
            Some(BinaryLength::IntegerLength { length }) => {
                details.push(TypeDetail::Exact(*length));
            }
            Some(BinaryLength::Max) => details.push(TypeDetail::Max),
        },
        DataType::Datetime64(precision, zone) => {
            details.push(TypeDetail::Exact(*precision));
            if let Some(zone) = zone {
                details.push(TypeDetail::TimeZone(zone.clone()));
            }
        }
        DataType::Interval { fields, precision } => {
            details.extend(fields.map(TypeDetail::IntervalFields));
            details.extend(precision.map(TypeDetail::Exact));
        }
        DataType::Array(element) => match element {
            ArrayElemTypeDef::None => {}
            ArrayElemTypeDef::AngleBracket(element) | ArrayElemTypeDef::Parenthesis(element) => {
                append_type_details(element, details);
                details.push(TypeDetail::ArraySize(None));
            }
            ArrayElemTypeDef::ParenthesisNotNull(element) => {
                append_type_details(element, details);
                details.push(TypeDetail::ArraySize(None));
                details.push(TypeDetail::NotNullElements);
            }
            ArrayElemTypeDef::SquareBracket(element, size)
            | ArrayElemTypeDef::Qualified(element, size) => {
                append_type_details(element, details);
                details.push(TypeDetail::ArraySize(*size));
            }
        },
        DataType::Enum(members, width) => {
            details.push(TypeDetail::Enum(members.clone(), *width));
        }
        DataType::Set(members) => details.push(TypeDetail::Set(members.clone())),
        DataType::Map(key, value, _) => details.push(TypeDetail::Elements(vec![
            TypeShape::new(key),
            TypeShape::new(value),
        ])),
        DataType::Struct(fields, _) | DataType::Tuple(fields) => {
            details.push(TypeDetail::Fields(
                fields
                    .iter()
                    .map(|field| {
                        let mut data_type = TypeShape::new(&field.field_type);
                        if let Some(options) = &field.options {
                            if !options.is_empty() {
                                data_type
                                    .details
                                    .push(TypeDetail::StructOptions(options.clone()));
                            }
                        }
                        FieldShape {
                            name: field.field_name.clone(),
                            data_type,
                        }
                    })
                    .collect(),
            ));
        }
        DataType::Union(fields) => details.push(TypeDetail::Fields(
            fields
                .iter()
                .map(|field| FieldShape {
                    name: Some(field.field_name.clone()),
                    data_type: TypeShape::new(&field.field_type),
                })
                .collect(),
        )),
        DataType::Nested(columns)
        | DataType::Object(columns)
        | DataType::Table(Some(columns))
        | DataType::NamedTable { columns, .. } => {
            if let DataType::NamedTable { name, .. } = data_type {
                details.push(TypeDetail::Name(name.clone()));
            }
            details.push(TypeDetail::Fields(
                columns
                    .iter()
                    .map(|column| {
                        let mut data_type = TypeShape::new(&column.data_type);
                        if !column.options.is_empty() {
                            data_type
                                .details
                                .push(TypeDetail::ColumnOptions(column.options.clone()));
                        }
                        FieldShape {
                            name: Some(column.name.clone()),
                            data_type,
                        }
                    })
                    .collect(),
            ));
        }
        DataType::Nullable(wrapped) => {
            append_type_details(wrapped, details);
            details.push(TypeDetail::Nullable);
        }
        DataType::Collate(wrapped, _)
        | DataType::LowCardinality(wrapped)
        | DataType::Zerofill(wrapped) => append_type_details(wrapped, details),
        DataType::Table(None)
        | DataType::Uuid
        | DataType::TinyBlob
        | DataType::MediumBlob
        | DataType::LongBlob
        | DataType::UTinyInt
        | DataType::USmallInt
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Int128
        | DataType::Int256
        | DataType::HugeInt
        | DataType::UHugeInt
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::UInt128
        | DataType::UInt256
        | DataType::UBigInt
        | DataType::Signed
        | DataType::SignedInteger
        | DataType::Unsigned
        | DataType::UnsignedInteger
        | DataType::Float4
        | DataType::Float32
        | DataType::Float64
        | DataType::Real
        | DataType::RealUnsigned
        | DataType::Float8
        | DataType::DoublePrecision
        | DataType::DoublePrecisionUnsigned
        | DataType::Bool
        | DataType::Boolean
        | DataType::Date
        | DataType::Date32
        | DataType::JSON
        | DataType::JSONB
        | DataType::Regclass
        | DataType::Text
        | DataType::TinyText
        | DataType::MediumText
        | DataType::LongText
        | DataType::Bytea
        | DataType::Custom(..)
        | DataType::Unspecified
        | DataType::Trigger
        | DataType::AnyType
        | DataType::GeometricType(_)
        | DataType::TsVector
        | DataType::TsQuery => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::drift::verdict::{judge, Incompatibility, Verdict};
    use alloc::format;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::{
        BigQueryDialect, ClickHouseDialect, Dialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect,
        SQLiteDialect,
    };

    fn column(ddl: &str, name: &str) -> ColumnShape {
        let db = ParserDB::parse::<SQLiteDialect>(ddl).expect("DDL parses");
        let table = db.tables().next().expect("one table");
        TableShape::from_table(table, &db)
            .expect("shape builds")
            .columns()
            .iter()
            .find(|column| column.name() == name)
            .cloned()
            .expect("column present")
    }

    fn shape_with_type<D: Dialect + Default>(data_type: &str) -> TableShape {
        let db =
            ParserDB::parse::<D>(&format!("CREATE TABLE t (v {data_type})")).expect("DDL parses");
        let table = db.tables().next().expect("one table");
        TableShape::from_table(table, &db).expect("shape builds")
    }

    fn assert_retyped(catalog: &TableShape, live: &TableShape) {
        for (catalog, live) in [(catalog, live), (live, catalog)] {
            assert_eq!(judge(catalog, Some(catalog)), Verdict::Unchanged);
            assert!(!catalog.columns()[0].type_matches(&live.columns()[0]));
            assert_eq!(
                judge(catalog, Some(live)),
                Verdict::Incompatible {
                    reasons: vec![Incompatibility::ColumnRetyped {
                        column: "v".into(),
                        catalog: catalog.columns()[0].type_normalized().into(),
                        live: live.columns()[0].type_normalized().into(),
                    }],
                }
            );
        }
    }

    #[test]
    fn a_column_shape_keeps_type_nullability_and_default() {
        let shape = column(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT NOT NULL DEFAULT 'x');",
            "note",
        );
        assert_eq!(shape.type_normalized(), "TEXT");
        assert!(!shape.nullable());
        assert!(shape.has_default());
    }

    #[test]
    fn a_table_absent_from_the_supplied_catalog_returns_a_lookup_error() {
        let source = ParserDB::parse::<SQLiteDialect>("CREATE TABLE source (v INTEGER)")
            .expect("DDL parses");
        let other =
            ParserDB::parse::<SQLiteDialect>("CREATE TABLE other (v INTEGER)").expect("DDL parses");
        let table = source.tables().next().expect("one table");
        assert!(TableShape::from_table(table, &other).is_err());
    }

    #[test]
    fn binary_width_is_distinct_from_an_omitted_or_different_width() {
        let omitted = shape_with_type::<MySqlDialect>("BINARY");
        let narrow = shape_with_type::<MySqlDialect>("BINARY(8)");
        let wide = shape_with_type::<MySqlDialect>("BINARY(16)");
        assert_eq!(
            narrow.columns()[0].type_normalized(),
            wide.columns()[0].type_normalized()
        );
        assert_retyped(&omitted, &narrow);
        assert_retyped(&narrow, &wide);
    }

    #[test]
    fn numeric_precision_and_scale_are_independent_bounds() {
        let unbounded = shape_with_type::<PostgreSqlDialect>("NUMERIC");
        let precise = shape_with_type::<PostgreSqlDialect>("NUMERIC(10)");
        let wider = shape_with_type::<PostgreSqlDialect>("NUMERIC(12)");
        let scaled = shape_with_type::<PostgreSqlDialect>("NUMERIC(10, 2)");
        let rescaled = shape_with_type::<PostgreSqlDialect>("NUMERIC(10, 3)");
        assert_retyped(&unbounded, &precise);
        assert_retyped(&precise, &wider);
        assert_retyped(&scaled, &rescaled);
    }

    #[test]
    fn max_character_length_is_distinct_from_omitted_and_exact_lengths() {
        let max = shape_with_type::<MsSqlDialect>("VARCHAR(MAX)");
        let omitted = shape_with_type::<MsSqlDialect>("VARCHAR");
        let exact = shape_with_type::<MsSqlDialect>("VARCHAR(8000)");
        assert_retyped(&max, &omitted);
        assert_retyped(&max, &exact);
    }

    #[test]
    fn binary_lengths_distinguish_max_exact_and_omitted_bounds() {
        let max = shape_with_type::<MsSqlDialect>("VARBINARY(MAX)");
        let omitted = shape_with_type::<MsSqlDialect>("VARBINARY");
        let narrow = shape_with_type::<MsSqlDialect>("VARBINARY(8)");
        let wide = shape_with_type::<MsSqlDialect>("VARBINARY(16)");
        assert_retyped(&max, &omitted);
        assert_retyped(&max, &wide);
        assert_retyped(&omitted, &narrow);
        assert_retyped(&narrow, &wide);
    }

    #[test]
    fn low_cardinality_keeps_fixed_string_widths() {
        let plain = shape_with_type::<ClickHouseDialect>("FixedString(8)");
        let wrapped = shape_with_type::<ClickHouseDialect>("LowCardinality(FixedString(8))");
        let wider = shape_with_type::<ClickHouseDialect>("LowCardinality(FixedString(16))");
        assert!(plain.columns()[0].type_matches(&wrapped.columns()[0]));
        assert_eq!(judge(&plain, Some(&wrapped)), Verdict::Unchanged);
        assert_retyped(&wrapped, &wider);
        assert_retyped(&plain, &wider);
    }

    #[test]
    fn array_bounds_preserve_element_width_and_dimension_order() {
        let catalog = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)[2][3]");
        let wider_element = shape_with_type::<PostgreSqlDialect>("VARCHAR(16)[2][3]");
        let swapped_dimensions = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)[3][2]");
        let unbounded_dimensions = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)[][]");
        assert_retyped(&catalog, &wider_element);
        assert_retyped(&catalog, &swapped_dimensions);
        assert_retyped(&catalog, &unbounded_dimensions);
    }

    #[test]
    fn qualified_arrays_share_element_and_dimension_bounds() {
        let bracketed = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)[3]");
        let qualified = shape_with_type::<PostgreSqlDialect>("VARCHAR(8) ARRAY[3]");
        let wider_element = shape_with_type::<PostgreSqlDialect>("VARCHAR(16) ARRAY[3]");
        let unbounded = shape_with_type::<PostgreSqlDialect>("VARCHAR(8) ARRAY");
        assert!(bracketed.columns()[0].type_matches(&qualified.columns()[0]));
        assert_eq!(judge(&bracketed, Some(&qualified)), Verdict::Unchanged);
        assert_retyped(&qualified, &wider_element);
        assert_retyped(&qualified, &unbounded);
    }

    #[test]
    fn typed_arrays_preserve_the_element_family() {
        let integers = shape_with_type::<BigQueryDialect>("ARRAY<INT64>");
        let strings = shape_with_type::<BigQueryDialect>("ARRAY<STRING>");
        let scalar = shape_with_type::<BigQueryDialect>("INT64");
        assert_retyped(&integers, &strings);
        assert_retyped(&integers, &scalar);
    }

    #[test]
    fn a_fixed_string_array_element_width_change_is_incompatible() {
        let catalog = shape_with_type::<ClickHouseDialect>("Array(FixedString(8))");
        let live = shape_with_type::<ClickHouseDialect>("Array(FixedString(16))");
        assert_eq!(
            judge(&catalog, Some(&live)),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnRetyped {
                    column: "v".into(),
                    catalog: "FIXEDSTRING[]".into(),
                    live: "FIXEDSTRING[]".into(),
                }],
            }
        );
    }

    #[test]
    fn distinct_negative_numeric_scales_are_incompatible() {
        let tens = shape_with_type::<PostgreSqlDialect>("NUMERIC(10, -1)");
        let hundreds = shape_with_type::<PostgreSqlDialect>("NUMERIC(10, -2)");
        let units = shape_with_type::<PostgreSqlDialect>("NUMERIC(10, 0)");
        assert_retyped(&tens, &hundreds);
        assert_retyped(&tens, &units);
    }

    #[test]
    fn time_precision_is_a_declared_bound() {
        let omitted = shape_with_type::<PostgreSqlDialect>("TIME");
        let seconds = shape_with_type::<PostgreSqlDialect>("TIME(0)");
        let millis = shape_with_type::<PostgreSqlDialect>("TIME(3)");
        assert_retyped(&seconds, &millis);
        assert_retyped(&omitted, &seconds);
    }

    #[test]
    fn timestamp_precision_is_a_declared_bound() {
        let millis = shape_with_type::<PostgreSqlDialect>("TIMESTAMP(3) WITH TIME ZONE");
        let micros = shape_with_type::<PostgreSqlDialect>("TIMESTAMP(6) WITH TIME ZONE");
        let unzoned = shape_with_type::<PostgreSqlDialect>("TIMESTAMP(3) WITHOUT TIME ZONE");
        assert_retyped(&millis, &micros);
        assert_retyped(&millis, &unzoned);
    }

    #[test]
    fn datetime_precision_is_a_declared_bound() {
        let seconds = shape_with_type::<MySqlDialect>("DATETIME(0)");
        let millis = shape_with_type::<MySqlDialect>("DATETIME(3)");
        assert_retyped(&seconds, &millis);
    }

    #[test]
    fn datetime64_precision_and_zone_are_independent_declarations() {
        let catalog = shape_with_type::<ClickHouseDialect>("DateTime64(3, 'UTC')");
        let precise = shape_with_type::<ClickHouseDialect>("DateTime64(6, 'UTC')");
        let zoned = shape_with_type::<ClickHouseDialect>("DateTime64(3, 'Europe/Rome')");
        assert_retyped(&catalog, &precise);
        assert_retyped(&catalog, &zoned);
    }

    #[test]
    fn decimal_alias_keeps_precision_and_scale() {
        let catalog = shape_with_type::<PostgreSqlDialect>("DEC(10, 2)");
        let wider = shape_with_type::<PostgreSqlDialect>("DEC(12, 2)");
        let rescaled = shape_with_type::<PostgreSqlDialect>("DEC(10, 3)");
        let equivalent = shape_with_type::<PostgreSqlDialect>("DECIMAL(10, 2)");
        assert_retyped(&catalog, &wider);
        assert_retyped(&catalog, &rescaled);
        assert_eq!(judge(&catalog, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn char_varying_alias_keeps_character_length() {
        let catalog = shape_with_type::<PostgreSqlDialect>("CHAR VARYING(8)");
        let wider = shape_with_type::<PostgreSqlDialect>("CHAR VARYING(16)");
        let equivalent = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)");
        assert_retyped(&catalog, &wider);
        assert_eq!(judge(&catalog, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn bit_varying_alias_keeps_bit_length() {
        let catalog = shape_with_type::<PostgreSqlDialect>("BIT VARYING(8)");
        let wider = shape_with_type::<PostgreSqlDialect>("BIT VARYING(16)");
        let equivalent = shape_with_type::<PostgreSqlDialect>("VARBIT(8)");
        assert_retyped(&catalog, &wider);
        assert_eq!(judge(&catalog, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn double_keeps_declared_decimal_precision_and_scale() {
        let catalog = shape_with_type::<MySqlDialect>("DOUBLE(10, 2)");
        let wider = shape_with_type::<MySqlDialect>("DOUBLE(12, 2)");
        let rescaled = shape_with_type::<MySqlDialect>("DOUBLE(10, 3)");
        assert_retyped(&catalog, &wider);
        assert_retyped(&catalog, &rescaled);
    }

    #[test]
    fn unsigned_decimal_keeps_precision_and_scale() {
        let catalog = shape_with_type::<MySqlDialect>("DECIMAL(10, 2) UNSIGNED");
        let wider = shape_with_type::<MySqlDialect>("DECIMAL(12, 2) UNSIGNED");
        let rescaled = shape_with_type::<MySqlDialect>("DECIMAL(10, 3) UNSIGNED");
        let equivalent = shape_with_type::<MySqlDialect>("DEC(10, 2) UNSIGNED");
        assert_retyped(&catalog, &wider);
        assert_retyped(&catalog, &rescaled);
        assert_eq!(judge(&catalog, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn omitted_array_dimensions_keep_their_positions() {
        let outer_bound = shape_with_type::<PostgreSqlDialect>("INTEGER[][3]");
        let inner_bound = shape_with_type::<PostgreSqlDialect>("INTEGER[3][]");
        let equivalent = shape_with_type::<PostgreSqlDialect>("INT[][3]");
        assert_retyped(&outer_bound, &inner_bound);
        assert_eq!(judge(&outer_bound, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn integer_signedness_is_part_of_the_declared_type() {
        let signed = shape_with_type::<MySqlDialect>("INT");
        let unsigned = shape_with_type::<MySqlDialect>("INT UNSIGNED");
        let equivalent = shape_with_type::<MySqlDialect>("INTEGER");
        assert_retyped(&signed, &unsigned);
        assert_eq!(judge(&signed, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn decimal_signedness_is_independent_of_bounds() {
        let signed = shape_with_type::<MySqlDialect>("DECIMAL");
        let unsigned = shape_with_type::<MySqlDialect>("DECIMAL UNSIGNED");
        assert_retyped(&signed, &unsigned);
    }

    #[test]
    fn enum_member_labels_are_part_of_the_declared_type() {
        let catalog = shape_with_type::<MySqlDialect>("ENUM('pending', 'ready')");
        let renamed = shape_with_type::<MySqlDialect>("ENUM('pending', 'complete')");
        let added = shape_with_type::<MySqlDialect>("ENUM('pending', 'ready', 'complete')");
        assert_retyped(&catalog, &renamed);
        assert_retyped(&catalog, &added);
    }

    #[test]
    fn enum_member_order_is_part_of_the_declared_type() {
        let catalog = shape_with_type::<MySqlDialect>("ENUM('pending', 'ready')");
        let reordered = shape_with_type::<MySqlDialect>("ENUM('ready', 'pending')");
        assert_retyped(&catalog, &reordered);
    }

    #[test]
    fn struct_member_names_are_part_of_the_declared_type() {
        let catalog = shape_with_type::<BigQueryDialect>("STRUCT<code INT64>");
        let renamed = shape_with_type::<BigQueryDialect>("STRUCT<identifier INT64>");
        assert_retyped(&catalog, &renamed);
    }

    #[test]
    fn struct_member_types_are_part_of_the_declared_type() {
        let catalog = shape_with_type::<BigQueryDialect>("STRUCT<code INT64>");
        let retyped = shape_with_type::<BigQueryDialect>("STRUCT<code STRING>");
        assert_retyped(&catalog, &retyped);
    }

    #[test]
    fn struct_member_order_is_part_of_the_declared_type() {
        let catalog = shape_with_type::<BigQueryDialect>("STRUCT<code INT64, label STRING>");
        let reordered = shape_with_type::<BigQueryDialect>("STRUCT<label STRING, code INT64>");
        assert_retyped(&catalog, &reordered);
    }

    #[test]
    fn tuple_member_bounds_keep_their_positions() {
        let catalog =
            shape_with_type::<ClickHouseDialect>("Tuple(FixedString(8), FixedString(16))");
        let reordered =
            shape_with_type::<ClickHouseDialect>("Tuple(FixedString(16), FixedString(8))");
        assert_retyped(&catalog, &reordered);
    }

    #[test]
    fn map_key_and_value_declarations_are_independent() {
        let catalog = shape_with_type::<ClickHouseDialect>("Map(UInt64, FixedString(8))");
        let rekeyed = shape_with_type::<ClickHouseDialect>("Map(String, FixedString(8))");
        let wider_value = shape_with_type::<ClickHouseDialect>("Map(UInt64, FixedString(16))");
        assert_retyped(&catalog, &rekeyed);
        assert_retyped(&catalog, &wider_value);
    }

    #[test]
    fn nested_aliases_share_their_declared_shape() {
        let catalog = shape_with_type::<BigQueryDialect>("STRUCT<amount DEC(10, 2)>");
        let alias = shape_with_type::<BigQueryDialect>("STRUCT<amount DECIMAL(10, 2)>");
        assert_eq!(judge(&catalog, Some(&alias)), Verdict::Unchanged);
    }

    #[test]
    fn array_element_nullability_is_part_of_the_declared_type() {
        let element = shape_with_type::<ClickHouseDialect>("Array(Nullable(Int32))");
        let plain = shape_with_type::<ClickHouseDialect>("Array(Int32)");
        assert_retyped(&element, &plain);
    }

    #[test]
    fn interval_fields_and_precision_are_independent() {
        let catalog = shape_with_type::<PostgreSqlDialect>("INTERVAL DAY TO SECOND(3)");
        let fields = shape_with_type::<PostgreSqlDialect>("INTERVAL HOUR TO SECOND(3)");
        let precision = shape_with_type::<PostgreSqlDialect>("INTERVAL DAY TO SECOND(6)");
        assert_retyped(&catalog, &fields);
        assert_retyped(&catalog, &precision);
    }

    #[test]
    fn enum_numeric_assignments_and_storage_width_are_preserved() {
        let catalog = shape_with_type::<ClickHouseDialect>("Enum8('ready' = 1, 'busy' = 2)");
        let reassigned = shape_with_type::<ClickHouseDialect>("Enum8('ready' = 2, 'busy' = 1)");
        let widened = shape_with_type::<ClickHouseDialect>("Enum16('ready' = 1, 'busy' = 2)");
        assert_retyped(&catalog, &reassigned);
        assert_retyped(&catalog, &widened);
    }

    #[test]
    fn set_member_order_is_part_of_the_declared_type() {
        let catalog = shape_with_type::<MySqlDialect>("SET('ready', 'busy')");
        let reordered = shape_with_type::<MySqlDialect>("SET('busy', 'ready')");
        assert_retyped(&catalog, &reordered);
    }

    #[test]
    fn nested_column_bounds_are_preserved() {
        let catalog = shape_with_type::<ClickHouseDialect>("Nested(amount Decimal(10, 2))");
        let wider = shape_with_type::<ClickHouseDialect>("Nested(amount Decimal(12, 2))");
        assert_retyped(&catalog, &wider);
    }
}
