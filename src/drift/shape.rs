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
use sqlparser::ast::{ArrayElemTypeDef, BinaryLength, CharacterLength, DataType, ExactNumberInfo};

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

/// One declared type bound, in the order the type declares them.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Bound {
    /// An exact declared bound, a `VARCHAR` length, an integer width or a
    /// numeric precision.
    Exact(u64),
    /// A `MAX`-style bound, `VARCHAR(MAX)` and the like.
    Max,
}

/// One column of a [`TableShape`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnShape {
    name: String,
    /// The declared type under its normalized spelling, aliases folded.
    type_normalized: String,
    /// The declared bounds read from the raw type, in declaration order.
    ///
    /// `normalize_sqlparser_type` folds aliases but drops the lengths a
    /// declaration carries, so the bounds are kept beside it and compared
    /// with it: `VARCHAR(10)` and `VARCHAR(20)` normalize alike and their
    /// bounds differ.
    bounds: Vec<Bound>,
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
        &self.type_normalized
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

    /// Whether the declared type and bounds match `other`'s.
    #[must_use]
    pub fn type_matches(&self, other: &Self) -> bool {
        self.type_normalized == other.type_normalized && self.bounds == other.bounds
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
                type_normalized: normalize_sqlparser_type(data_type).into_owned(),
                bounds: type_bounds(data_type),
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

/// The bounds a type declares, read from the raw sqlparser [`DataType`]
/// the normalization keeps no memory of.
///
/// Array and storage wrappers include the bounds of their inner type.
fn type_bounds(data_type: &DataType) -> Vec<Bound> {
    match data_type {
        DataType::Character(length)
        | DataType::Char(length)
        | DataType::CharacterVarying(length)
        | DataType::Varchar(length)
        | DataType::Nvarchar(length) => character_bounds(length.as_ref()),
        DataType::Clob(width)
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
        | DataType::VarBit(width)
        | DataType::Int2(width)
        | DataType::Int4(width)
        | DataType::Int8(width)
        | DataType::TinyIntUnsigned(width)
        | DataType::SmallIntUnsigned(width)
        | DataType::IntUnsigned(width)
        | DataType::IntegerUnsigned(width)
        | DataType::BigIntUnsigned(width)
        | DataType::MediumIntUnsigned(width) => optional_width(width.as_ref()),
        DataType::Float(info)
        | DataType::Numeric(info)
        | DataType::Decimal(info)
        | DataType::BigNumeric(info)
        | DataType::BigDecimal(info) => number_bounds(info),
        DataType::FixedString(width) => vec![Bound::Exact(*width)],
        DataType::Varbinary(length) => match length {
            None => Vec::new(),
            Some(BinaryLength::IntegerLength { length }) => vec![Bound::Exact(*length)],
            Some(BinaryLength::Max) => vec![Bound::Max],
        },
        DataType::Array(element) => match element {
            ArrayElemTypeDef::None => Vec::new(),
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::Parenthesis(element)
            | ArrayElemTypeDef::ParenthesisNotNull(element) => type_bounds(element),
            ArrayElemTypeDef::SquareBracket(element, size)
            | ArrayElemTypeDef::Qualified(element, size) => {
                let mut bounds = type_bounds(element);
                if let Some(size) = size {
                    bounds.push(Bound::Exact(*size));
                }
                bounds
            }
        },
        // `COLLATE` and `LowCardinality` decorate storage or comparison
        // rather than name a width, so they keep the bounds of the type
        // they wrap, like the normalization keeps its spelling.
        DataType::Collate(wrapped, _) | DataType::LowCardinality(wrapped) => type_bounds(wrapped),
        _ => Vec::new(),
    }
}

fn character_bounds(length: Option<&CharacterLength>) -> Vec<Bound> {
    match length {
        None => Vec::new(),
        Some(CharacterLength::IntegerLength { length, .. }) => vec![Bound::Exact(*length)],
        Some(CharacterLength::Max) => vec![Bound::Max],
    }
}

fn optional_width(width: Option<&u64>) -> Vec<Bound> {
    width.copied().map(Bound::Exact).into_iter().collect()
}

fn number_bounds(info: &ExactNumberInfo) -> Vec<Bound> {
    match info {
        ExactNumberInfo::None => Vec::new(),
        ExactNumberInfo::Precision(precision) => vec![Bound::Exact(*precision)],
        // A negative scale is invalid DDL the parser still accepts; map it
        // to a sentinel so two declarations of it still compare equal.
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            let scale = u64::try_from(*scale).unwrap_or(u64::MAX);
            vec![Bound::Exact(*precision), Bound::Exact(scale)]
        }
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
    fn bounds_come_from_the_raw_type_the_normalization_drops() {
        let bounded = column("CREATE TABLE t (v VARCHAR(10));", "v");
        let unbounded = column("CREATE TABLE t (v VARCHAR);", "v");
        assert_eq!(bounded.type_normalized(), unbounded.type_normalized());
        assert_ne!(
            bounded.bounds, unbounded.bounds,
            "the normalization folds both to one spelling, the bounds keep the length apart"
        );
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
}
