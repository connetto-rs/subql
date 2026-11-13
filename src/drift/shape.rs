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

use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sql_traits::traits::{
    ArrayDimension, CharacterLengthUnit, CheckConstraintLike, ColumnCollation, DataTypeLike,
    DialectLike, EnumWidth, EnumerationKind, IntervalFields, StructureKind, StructuredField,
    TypeMatch, TypeMatchLike,
};

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

/// A field of a structured type, as sql-traits answers it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FieldShape {
    name: Option<String>,
    name_is_quoted: bool,
    data_type: TypeShape,
    nullable: bool,
    options: Vec<(String, String)>,
    collation: ColumnCollation<'static>,
}

impl FieldShape {
    fn of<T: DataTypeLike>(field: &StructuredField<'_, T>, database: &T::DB) -> Self {
        Self {
            name: field.name().map(String::from),
            name_is_quoted: field.name_is_quoted(),
            data_type: TypeShape::of(field.data_type(), database),
            nullable: field.is_nullable(),
            options: field
                .options()
                .iter()
                .map(|(key, value)| (String::from(key.as_ref()), String::from(value.as_ref())))
                .collect(),
            collation: field.collation().clone().into_owned(),
        }
    }
}

/// A map type's key and value, as sql-traits answers them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MapShape {
    key: TypeShape,
    value: TypeShape,
    value_nullable: bool,
}

/// An enumerated type's members, as sql-traits answers them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EnumerationShape {
    kind: EnumerationKind,
    width: Option<EnumWidth>,
    /// Each member's label and stored value, in the order sql-traits answers them.
    members: Vec<(String, Option<i16>)>,
}

/// The declared bounds sql-traits answers for a type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TypeBounds {
    fixed_binary_length: Option<u64>,
    max_binary_length: Option<u64>,
    max_bit_length: Option<u64>,
    max_character_length: Option<u64>,
    character_length_unit: Option<CharacterLengthUnit>,
    fractional_seconds_precision: Option<u64>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<i64>,
    interval_fields: Option<IntervalFields>,
    unsigned: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TypeShape {
    normalized: String,
    bounds: TypeBounds,
    time_zone: Option<String>,
    element: Option<Box<Self>>,
    structure_kind: Option<StructureKind>,
    fields: Vec<FieldShape>,
    map: Option<Box<MapShape>>,
    enumeration: Option<EnumerationShape>,
    /// Each enforced dimension's size and element nullability, outermost first.
    array_dimensions: Option<Vec<ArrayDimension>>,
    admits_null: bool,
}

impl TypeShape {
    /// The shape of `data_type` as sql-traits answers it.
    fn of<T: DataTypeLike>(data_type: &T, database: &T::DB) -> Self {
        let element = data_type
            .array_element(database)
            .map(|element| Box::new(Self::of(element, database)));
        let enforces_dimensions = data_type.enforces_array_dimensions(database);
        // Without an enforced dimension count, an array is named by its element and one `[]`.
        let normalized = match (enforces_dimensions, &element) {
            (Some(false), Some(element)) => format!("{}[]", element.normalized),
            _ => data_type.name(database).into_owned(),
        };
        Self {
            normalized,
            bounds: TypeBounds {
                fixed_binary_length: data_type.fixed_binary_length(database),
                max_binary_length: data_type.max_binary_length(database),
                max_bit_length: data_type.max_bit_length(database),
                max_character_length: data_type.max_character_length(database),
                character_length_unit: data_type.character_length_unit(database),
                fractional_seconds_precision: data_type.fractional_seconds_precision(database),
                numeric_precision: data_type.numeric_precision(database),
                numeric_scale: data_type.numeric_scale(database),
                interval_fields: data_type.interval_fields(database),
                unsigned: data_type.is_unsigned(database),
            },
            time_zone: data_type.time_zone(database).map(String::from),
            element,
            structure_kind: data_type.structure_kind(database),
            fields: data_type
                .structured_fields(database)
                .into_iter()
                .flatten()
                .map(|field| FieldShape::of(&field, database))
                .collect(),
            map: data_type.map_entry(database).map(|entry| {
                Box::new(MapShape {
                    key: Self::of(entry.key(), database),
                    value: Self::of(entry.value(), database),
                    value_nullable: entry.value_is_nullable(),
                })
            }),
            enumeration: data_type
                .enumeration(database)
                .map(|enumeration| EnumerationShape {
                    kind: enumeration.kind(),
                    width: enumeration.width(),
                    members: enumeration
                        .into_members()
                        .map(|member| (String::from(member.label()), member.value()))
                        .collect(),
                }),
            array_dimensions: enforces_dimensions
                .filter(|enforced| *enforced)
                .and_then(|_| data_type.array_dimension_declarations(database))
                .map(Iterator::collect),
            admits_null: data_type.admits_null(database),
        }
    }
}

/// One column of a [`TableShape`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnShape {
    name: String,
    data_type: TypeShape,
    /// How the dialect reads the column as a boolean, which tells MySQL's
    /// `TINYINT(1)` from a wider `TINYINT`.
    boolean: TypeMatch,
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

    /// Whether the declared type, bounds, members and boolean reading match `other`'s.
    #[must_use]
    pub fn type_matches(&self, other: &Self) -> bool {
        self.data_type == other.data_type && self.boolean == other.boolean
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
    pub fn from_table<DB: DatabaseLike>(
        table: &DB::Table,
        database: &DB,
    ) -> Result<Self, sql_traits::errors::LookupError> {
        let mut columns = Vec::new();
        for column in table.columns(database)? {
            let boolean = database.dialect().is_bool(database, column);
            columns.push(ColumnShape {
                name: column.column_name().to_string(),
                data_type: TypeShape::of(column.data_type(database), database),
                boolean: if boolean.is_yes() {
                    TypeMatch::Yes
                } else if boolean.is_maybe() {
                    TypeMatch::Maybe
                } else {
                    TypeMatch::No
                },
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::drift::verdict::{judge, Incompatibility, Verdict};
    use alloc::format;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::{
        BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect,
        GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
        SnowflakeDialect,
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

    fn shape_from_ddl<D: Dialect + Default>(ddl: &str) -> TableShape {
        let db = ParserDB::parse::<D>(ddl).expect("DDL parses");
        let table = db.tables().next().expect("one table");
        TableShape::from_table(table, &db).expect("shape builds")
    }

    fn shape_with_type<D: Dialect + Default>(data_type: &str) -> TableShape {
        shape_from_ddl::<D>(&format!("CREATE TABLE t (v {data_type})"))
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
    fn nvarchar_keeps_max_exact_and_omitted_lengths() {
        let max = shape_with_type::<MsSqlDialect>("NVARCHAR(MAX)");
        let omitted = shape_with_type::<MsSqlDialect>("NVARCHAR");
        let narrow = shape_with_type::<MsSqlDialect>("NVARCHAR(8)");
        let wide = shape_with_type::<MsSqlDialect>("NVARCHAR(16)");
        assert_retyped(&max, &omitted);
        assert_retyped(&max, &wide);
        assert_retyped(&omitted, &narrow);
        assert_retyped(&narrow, &wide);
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
    fn enforced_array_sizes_keep_their_order() {
        let catalog = shape_with_type::<DuckDbDialect>("INTEGER[2][3]");
        let swapped = shape_with_type::<DuckDbDialect>("INTEGER[3][2]");
        let lists = shape_with_type::<DuckDbDialect>("INTEGER[][]");
        let flat = shape_with_type::<DuckDbDialect>("INTEGER[6]");
        assert_retyped(&catalog, &swapped);
        assert_retyped(&catalog, &lists);
        assert_retyped(&catalog, &flat);
    }

    #[test]
    fn postgres_arrays_compare_only_their_element() {
        let catalog = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)[2][3]");
        for unenforced in [
            "VARCHAR(8)[3][2]",
            "VARCHAR(8)[][]",
            "VARCHAR(8)[]",
            "VARCHAR(8) ARRAY",
            "CHARACTER VARYING(8)[3]",
        ] {
            let live = shape_with_type::<PostgreSqlDialect>(unenforced);
            assert_eq!(judge(&catalog, Some(&live)), Verdict::Unchanged);
        }
        assert_retyped(
            &catalog,
            &shape_with_type::<PostgreSqlDialect>("VARCHAR(16)[2][3]"),
        );
        assert_retyped(
            &catalog,
            &shape_with_type::<PostgreSqlDialect>("VARCHAR(8)"),
        );
    }

    #[test]
    fn qualified_arrays_share_element_bounds() {
        let bracketed = shape_with_type::<PostgreSqlDialect>("VARCHAR(8)[3]");
        let qualified = shape_with_type::<PostgreSqlDialect>("VARCHAR(8) ARRAY[3]");
        let wider_element = shape_with_type::<PostgreSqlDialect>("VARCHAR(16) ARRAY[3]");
        assert!(bracketed.columns()[0].type_matches(&qualified.columns()[0]));
        assert_eq!(judge(&bracketed, Some(&qualified)), Verdict::Unchanged);
        assert_retyped(&qualified, &wider_element);
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
    fn an_omitted_time_precision_is_the_engine_default() {
        let omitted = shape_with_type::<PostgreSqlDialect>("TIME");
        let micros = shape_with_type::<PostgreSqlDialect>("TIME(6)");
        assert_eq!(judge(&omitted, Some(&micros)), Verdict::Unchanged);
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
    fn national_character_types_keep_character_length() {
        for (national, plain, wider) in [
            ("NCHAR(4)", "CHAR(4)", "NCHAR(8)"),
            (
                "NATIONAL CHARACTER(4)",
                "CHARACTER(4)",
                "NATIONAL CHARACTER(8)",
            ),
            ("NATIONAL CHAR(4)", "CHAR(4)", "NATIONAL CHAR(8)"),
            ("NCHAR VARYING(8)", "VARCHAR(8)", "NCHAR VARYING(16)"),
            (
                "NATIONAL CHARACTER VARYING(8)",
                "VARCHAR(8)",
                "NATIONAL CHARACTER VARYING(16)",
            ),
            (
                "NATIONAL CHAR VARYING(8)",
                "VARCHAR(8)",
                "NATIONAL CHAR VARYING(16)",
            ),
        ] {
            let catalog = shape_with_type::<PostgreSqlDialect>(national);
            let equivalent = shape_with_type::<PostgreSqlDialect>(plain);
            let wider = shape_with_type::<PostgreSqlDialect>(wider);
            assert_eq!(judge(&catalog, Some(&equivalent)), Verdict::Unchanged);
            assert_retyped(&catalog, &wider);
        }
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
        let outer_bound = shape_with_type::<DuckDbDialect>("INTEGER[][3]");
        let inner_bound = shape_with_type::<DuckDbDialect>("INTEGER[3][]");
        let equivalent = shape_with_type::<DuckDbDialect>("INT[][3]");
        assert_retyped(&outer_bound, &inner_bound);
        assert_eq!(judge(&outer_bound, Some(&equivalent)), Verdict::Unchanged);
    }

    #[test]
    fn inner_array_dimensions_are_part_of_the_declared_type() {
        let narrow = shape_with_type::<DuckDbDialect>("INTEGER[2][3]");
        let wide = shape_with_type::<DuckDbDialect>("INTEGER[4][3]");
        assert_retyped(&narrow, &wide);
        let nullable = shape_with_type::<SnowflakeDialect>("ARRAY(ARRAY(INT))");
        let required = shape_with_type::<SnowflakeDialect>("ARRAY(ARRAY(INT NOT NULL))");
        assert_retyped(&nullable, &required);
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
    fn only_the_boolean_tinyint_display_width_is_part_of_the_declared_type() {
        let narrow = shape_with_type::<MySqlDialect>("INT(10)");
        let wide = shape_with_type::<MySqlDialect>("INT(11)");
        let boolean = shape_with_type::<MySqlDialect>("TINYINT(1)");
        let tiny = shape_with_type::<MySqlDialect>("TINYINT(4)");
        assert_eq!(judge(&narrow, Some(&wide)), Verdict::Unchanged);
        assert_retyped(&boolean, &tiny);
    }

    #[test]
    fn decimal_signedness_is_independent_of_bounds() {
        let signed = shape_with_type::<MySqlDialect>("DECIMAL");
        let unsigned = shape_with_type::<MySqlDialect>("DECIMAL UNSIGNED");
        assert_retyped(&signed, &unsigned);
    }

    #[test]
    fn zerofill_declares_an_unsigned_integer() {
        let zerofill = shape_with_type::<MySqlDialect>("INT ZEROFILL");
        let unsigned = shape_with_type::<MySqlDialect>("INT UNSIGNED");
        let signed = shape_with_type::<MySqlDialect>("INT");
        assert_eq!(judge(&zerofill, Some(&unsigned)), Verdict::Unchanged);
        assert_retyped(&zerofill, &signed);
    }

    #[test]
    fn sqlite_ignores_a_declared_unsigned() {
        let unsigned = shape_with_type::<SQLiteDialect>("INT UNSIGNED");
        let signed = shape_with_type::<SQLiteDialect>("INT");
        assert_eq!(judge(&unsigned, Some(&signed)), Verdict::Unchanged);
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

    #[test]
    fn character_length_units_are_part_of_the_declared_bound() {
        let characters = shape_with_type::<GenericDialect>("CHARACTER(8 CHARACTERS)");
        let octets = shape_with_type::<GenericDialect>("CHARACTER(8 OCTETS)");
        let omitted = shape_with_type::<GenericDialect>("CHARACTER(8)");
        assert_retyped(&characters, &octets);
        assert_retyped(&characters, &omitted);
    }

    #[test]
    fn array_element_declarations_distinguish_untyped_nullable_and_required() {
        let untyped = shape_with_type::<SnowflakeDialect>("ARRAY");
        let nullable = shape_with_type::<SnowflakeDialect>("ARRAY(VARCHAR(8))");
        let required = shape_with_type::<SnowflakeDialect>("ARRAY(VARCHAR(8) NOT NULL)");
        let wider = shape_with_type::<SnowflakeDialect>("ARRAY(VARCHAR(16) NOT NULL)");
        assert_retyped(&untyped, &nullable);
        assert_retyped(&nullable, &required);
        assert_retyped(&required, &wider);
    }

    #[test]
    fn union_member_names_types_and_order_are_preserved() {
        let catalog = shape_with_type::<DuckDbDialect>("UNION(code INTEGER, label VARCHAR)");
        let renamed = shape_with_type::<DuckDbDialect>("UNION(identifier INTEGER, label VARCHAR)");
        let retyped = shape_with_type::<DuckDbDialect>("UNION(code BIGINT, label VARCHAR)");
        let reordered = shape_with_type::<DuckDbDialect>("UNION(label VARCHAR, code INTEGER)");
        let alias = shape_with_type::<DuckDbDialect>("UNION(code INT, label VARCHAR)");
        assert_retyped(&catalog, &renamed);
        assert_retyped(&catalog, &retyped);
        assert_retyped(&catalog, &reordered);
        assert_eq!(judge(&catalog, Some(&alias)), Verdict::Unchanged);
    }

    #[test]
    fn struct_field_rounding_options_are_preserved() {
        let omitted = shape_with_type::<BigQueryDialect>("STRUCT<amount NUMERIC(10, 2)>");
        let empty = shape_with_type::<BigQueryDialect>("STRUCT<amount NUMERIC(10, 2) OPTIONS()>");
        let even = shape_with_type::<BigQueryDialect>(
            "STRUCT<amount NUMERIC(10, 2) OPTIONS(rounding_mode = 'ROUND_HALF_EVEN')>",
        );
        let away = shape_with_type::<BigQueryDialect>(
            "STRUCT<amount NUMERIC(10, 2) OPTIONS(rounding_mode = 'ROUND_HALF_AWAY_FROM_ZERO')>",
        );
        assert_retyped(&omitted, &even);
        assert_retyped(&even, &away);
        assert_eq!(judge(&omitted, Some(&empty)), Verdict::Unchanged);
    }

    #[test]
    fn object_field_nullability_is_preserved() {
        let nullable = shape_with_type::<SnowflakeDialect>("OBJECT(city VARCHAR)");
        let required = shape_with_type::<SnowflakeDialect>("OBJECT(city VARCHAR NOT NULL)");
        assert_retyped(&nullable, &required);
    }

    #[test]
    fn collated_struct_fields_keep_their_declared_bounds() {
        let catalog =
            shape_with_type::<DatabricksDialect>("STRUCT<label VARCHAR(8) COLLATE UTF8_LCASE>");
        let wider =
            shape_with_type::<DatabricksDialect>("STRUCT<label VARCHAR(16) COLLATE UTF8_LCASE>");
        assert_retyped(&catalog, &wider);
    }

    #[test]
    fn collated_array_elements_and_map_values_keep_their_declared_bounds() {
        let element = shape_with_type::<DatabricksDialect>("ARRAY<VARCHAR(8) COLLATE UTF8_LCASE>");
        let wider_element =
            shape_with_type::<DatabricksDialect>("ARRAY<VARCHAR(16) COLLATE UTF8_LCASE>");
        assert_retyped(&element, &wider_element);
        let value = shape_with_type::<DatabricksDialect>("MAP<STRING, VARCHAR(4) COLLATE UNICODE>");
        let wider_value =
            shape_with_type::<DatabricksDialect>("MAP<STRING, VARCHAR(8) COLLATE UNICODE>");
        assert_retyped(&value, &wider_value);
    }

    #[test]
    fn struct_field_collation_is_part_of_the_declared_type() {
        let lowercase =
            shape_with_type::<DatabricksDialect>("STRUCT<label VARCHAR(8) COLLATE UTF8_LCASE>");
        let binary =
            shape_with_type::<DatabricksDialect>("STRUCT<label VARCHAR(8) COLLATE UTF8_BINARY>");
        let omitted = shape_with_type::<DatabricksDialect>("STRUCT<label VARCHAR(8)>");
        assert_retyped(&lowercase, &binary);
        assert_retyped(&lowercase, &omitted);
    }

    #[test]
    fn composite_type_attributes_are_part_of_the_declared_type() {
        let composite = |attributes: &str| {
            shape_from_ddl::<PostgreSqlDialect>(&format!(
                "CREATE TYPE pair AS ({attributes}); CREATE TABLE t (v pair);"
            ))
        };
        let catalog = composite("left_side INT, right_side TEXT");
        assert_retyped(&catalog, &composite("left_side BIGINT, right_side TEXT"));
        assert_retyped(&catalog, &composite("left_side INT, label TEXT"));
        assert_retyped(&catalog, &composite("right_side TEXT, left_side INT"));
        assert_retyped(
            &catalog,
            &composite("left_side INT, right_side TEXT COLLATE \"C\""),
        );
        assert_eq!(
            judge(
                &catalog,
                Some(&composite("left_side INTEGER, right_side TEXT"))
            ),
            Verdict::Unchanged
        );
    }

    #[test]
    fn map_value_nullability_is_part_of_the_declared_type() {
        let nullable = shape_with_type::<SnowflakeDialect>("MAP(VARCHAR, INT)");
        let required = shape_with_type::<SnowflakeDialect>("MAP(VARCHAR, INT NOT NULL)");
        assert_retyped(&nullable, &required);
    }

    #[test]
    fn postgres_enum_type_labels_are_part_of_the_declared_type() {
        let enumerated = |ddl: &str| {
            shape_from_ddl::<PostgreSqlDialect>(&format!("{ddl}; CREATE TABLE t (v status);"))
        };
        let catalog = enumerated("CREATE TYPE status AS ENUM ('pending', 'ready')");
        assert_retyped(
            &catalog,
            &enumerated("CREATE TYPE status AS ENUM ('pending', 'done')"),
        );
        assert_retyped(
            &catalog,
            &enumerated("CREATE TYPE status AS ENUM ('ready', 'pending')"),
        );
        assert_retyped(
            &catalog,
            &enumerated("CREATE TYPE status AS ENUM ('pending', 'ready', 'done')"),
        );
        assert_eq!(
            judge(
                &catalog,
                Some(&enumerated(
                    "CREATE TYPE status AS ENUM ('pending'); ALTER TYPE status ADD VALUE 'ready'"
                ))
            ),
            Verdict::Unchanged
        );
    }

    #[test]
    fn clickhouse_enum_values_written_or_assigned_compare_alike() {
        let assigned = shape_with_type::<ClickHouseDialect>("Enum8('ready', 'busy')");
        let written = shape_with_type::<ClickHouseDialect>("Enum8('busy' = 2, 'ready' = 1)");
        assert_eq!(judge(&assigned, Some(&written)), Verdict::Unchanged);
    }

    #[test]
    fn mysql_blob_lengths_compare_as_the_type_mysql_creates() {
        let short = shape_with_type::<MySqlDialect>("BLOB(100)");
        let longer_tiny = shape_with_type::<MySqlDialect>("BLOB(200)");
        let regular = shape_with_type::<MySqlDialect>("BLOB(300)");
        assert_eq!(judge(&short, Some(&longer_tiny)), Verdict::Unchanged);
        assert_retyped(&short, &regular);
    }

    #[test]
    fn mysql_float_precision_compares_as_single_or_double() {
        let narrow = shape_with_type::<MySqlDialect>("FLOAT(10)");
        let single = shape_with_type::<MySqlDialect>("FLOAT(20)");
        let double = shape_with_type::<MySqlDialect>("FLOAT(30)");
        assert_eq!(judge(&narrow, Some(&single)), Verdict::Unchanged);
        assert_retyped(&narrow, &double);
    }

    #[test]
    fn a_bare_bit_holds_one_bit() {
        let bare = shape_with_type::<PostgreSqlDialect>("BIT");
        let one = shape_with_type::<PostgreSqlDialect>("BIT(1)");
        let eight = shape_with_type::<PostgreSqlDialect>("BIT(8)");
        assert_eq!(judge(&bare, Some(&one)), Verdict::Unchanged);
        assert_retyped(&bare, &eight);
    }

    #[test]
    fn a_bare_sql_server_varbinary_holds_one_byte() {
        let bare = shape_with_type::<MsSqlDialect>("VARBINARY");
        let one = shape_with_type::<MsSqlDialect>("VARBINARY(1)");
        assert_eq!(judge(&bare, Some(&one)), Verdict::Unchanged);
    }
}
