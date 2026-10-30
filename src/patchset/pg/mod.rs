//! Postgres adapter for [`sqlite_diff_rs`] patchset application.
//!
//! Dispatches columns to native diesel binds based on the target
//! column's Postgres type, resolved from the subql catalog.
//!
//! # Dispatched types
//!
//! * `BOOLEAN` gets a native `Bool` bind when the wire carries an SQLite
//!   `Value::Integer`. Any other wire shape on a `BOOLEAN` column is
//!   rejected with a [`diesel::result::Error::QueryBuilderError`] before
//!   the query executes.
//! * `UUID` gets a native `Uuid` bind when the wire carries either a
//!   16-byte `Value::Blob` (compact binary UUID) or a `Value::Text` that
//!   parses through [`uuid::Uuid::parse_str`] (hyphenated form). Both
//!   flavors are accepted transparently, allowing clients that prefer
//!   BLOB storage and clients that prefer TEXT storage to coexist against
//!   the same server without changing the adapter. Any other wire shape
//!   on a `UUID` column is rejected.
//! * `NUMERIC(p,s)` / `DECIMAL(p,s)` gets a native `Numeric` bind when the
//!   wire carries a `Value::Integer` of minor units, which is how a
//!   pg2sqlite replica stores the column: `1250` binds as `12.50` at scale
//!   2. A `NUMERIC` without a declared scale binds from a `Value::Text`
//!   holding the verbatim decimal digits, parsed through
//!   [`bigdecimal::BigDecimal`]. The target column is classified through
//!   the catalog's [`crate::backend::ScalarKind`], since Postgres has no
//!   implicit assignment cast from text to `numeric`. Any other wire shape
//!   on a decimal column is rejected.
//! * `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, and `TIME` get native temporal
//!   binds when the wire carries a `Value::Text` holding the replica's
//!   temporal text (`YYYY-MM-DD HH:MM:SS.ffffff+00:00` for a
//!   `TIMESTAMPTZ`) or Postgres's own text output, parsed through
//!   `sql_scalar_text`. `TIMESTAMPTZ` normalizes to a UTC instant.
//!   Each column is classified through the catalog's [`crate::backend::ScalarKind`],
//!   since Postgres has no implicit assignment cast from text to these
//!   types. Any other wire shape on such a column is rejected.
//! * `JSON` and `JSONB` get native binds when the wire carries a
//!   `Value::Text` holding JSON text, parsed through `serde_json`. A
//!   `JSONB` column normalizes key order and whitespace on store, so a
//!   round trip preserves the value, not the exact input bytes. Each is
//!   classified through the catalog's [`crate::backend::ScalarKind`]. Any other wire
//!   shape on such a column is rejected.
//! * An array column gets a native array bind when the wire carries a
//!   `Value::Text` holding the replica's JSON array text, of any number of
//!   dimensions. It binds as the widest type of its element family, such as
//!   `int8[]` for any integer element, which PostgreSQL's assignment cast
//!   narrows to the column's own type. An array of an enum or domain needs
//!   [`CustomTypePgAdapter`].
//! * A pgvector `vector(n)` column gets a native `vector` bind when the wire
//!   carries sqlite-vec's float32 `Value::Blob` of `n` elements.
//!
//! Every other column falls through to [`sqlite_diff_rs::DefaultBinder`]
//! which handles the trivial SQLite-to-diesel type mappings (`Integer ->
//! BigInt`, `Real -> Double`, `Text -> Text`, `Blob -> Binary`, `Null ->
//! literal NULL`).
//!
//! A Postgres `ENUM`, `DOMAIN`, or other user-defined type has no
//! built-in arm here, since a native bind needs a diesel `SqlType` and a
//! [`ToSql`](diesel::serialize::ToSql) the caller owns. Wrap [`PgAdapter`] in a
//! [`CustomTypePgAdapter`] and register a [`PgCustomBinder`] per such
//! type to bind those columns, and arrays of them, natively, still with no
//! SQL cast.

use alloc::borrow::Cow;
use alloc::boxed::Box;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::pg::Pg;
use diesel::result::Error as DieselError;
use diesel::sql_types::{
    Bool, Date, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid as UuidSqlType,
};
use sql_scalar_text::{parse_date, parse_time, parse_timestamp, parse_timestamp_tz};
use sql_traits::prelude::{
    ColumnLike, DataTypeLike, DatabaseLike, DialectLike, TypeMatchLike, VectorKind,
};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use crate::backend::{ScalarFamily, ScalarKindOf};
use crate::patchset::columns::{
    bind_error, minor_units, shape_of, unknown_column_error, ColumnIndex,
};

pub(crate) mod array;
pub(crate) mod custom_type;

pub use array::{bind_array_as, PgArray, PgVector, VectorType};
pub use custom_type::{bind_as, CustomTypePgAdapter, PgCustomBinder};

/// Adapter that resolves column names and native diesel binders for a
/// Postgres target from a subql catalog.
///
/// See the module docs for the full dispatch table.
#[derive(Debug)]
pub struct PgAdapter<'db, DB: DatabaseLike> {
    catalog: &'db DB,
    columns: ColumnIndex<'db, DB>,
}

impl<'db, DB: DatabaseLike> PgAdapter<'db, DB> {
    /// Index the catalog once and build the adapter over the index. The
    /// catalog handle is kept only for per-column type classification,
    /// which never walks the schema.
    ///
    /// # Errors
    /// [`CatalogError`](crate::CatalogError) when the catalog fails to
    /// yield a table's columns.
    pub fn new(catalog: &'db DB) -> Result<Self, crate::CatalogError> {
        Ok(Self {
            catalog,
            columns: ColumnIndex::new(catalog)?,
        })
    }

    fn column_at(&self, table_name: &str, index: usize) -> Option<&'db DB::Column> {
        self.columns.column_at(table_name, index)
    }

    /// Classify the target column through the catalog's [`crate::backend::ScalarKind`],
    /// the dispatch key for families Postgres will not assignment-cast
    /// from a text bind. Returns `None` for an unknown table or column,
    /// or a declared type that maps to no supported scalar.
    fn scalar_kind_at(
        &self,
        table_name: &str,
        column_index: usize,
    ) -> Option<ScalarKindOf<crate::backend::Postgres>> {
        let column = self.column_at(table_name, column_index)?;
        crate::catalog_helpers::classify_scalar_kind::<crate::backend::Postgres>(
            &column.data_type(self.catalog).name(self.catalog),
        )
    }

    /// The declared SQL type name of the target column, verbatim from the
    /// catalog. For a Postgres user-defined type such as an `ENUM` or a
    /// `DOMAIN`, this is the type's own name (for example `mood` or
    /// `sku`). [`CustomTypePgAdapter`] uses it to route a column to a
    /// caller-registered native bind. Returns `None` for an unknown table
    /// or column.
    #[must_use]
    pub fn column_type_name(&self, table_name: &str, column_index: usize) -> Option<Cow<'_, str>> {
        self.column_at(table_name, column_index)
            .map(|col| col.data_type(self.catalog).name(self.catalog))
    }

    /// The declared type name of the target array column's element, such as
    /// `mood` for a `mood[]` column, or `None` for a column that is not an
    /// array.
    #[must_use]
    pub fn element_type_name(&self, table_name: &str, column_index: usize) -> Option<Cow<'_, str>> {
        let data_type = self
            .column_at(table_name, column_index)?
            .data_type(self.catalog);
        Some(data_type.array_element(self.catalog)?.name(self.catalog))
    }

    /// The bind of an array or pgvector column, or `None` for a column that is
    /// neither.
    fn collection_bind<'a, S, B>(
        &self,
        col: &DB::Column,
        value: &'a Value<S, B>,
    ) -> Option<Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError>>
    where
        S: AsRef<str> + Sync,
        B: AsRef<[u8]> + Sync,
    {
        let data_type = col.data_type(self.catalog);
        let col_name = col.column_name();
        if let Some(element) = data_type.array_element(self.catalog) {
            let family = element.scalar_family(self.catalog);
            return Some(match value {
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                Value::Text(text) => PgArray::parse(text.as_ref())
                    .and_then(|array| array::bind_builtin_array(family?, &array))
                    .ok_or_else(|| {
                        bind_error(
                            col_name,
                            "a JSON array of its element type",
                            "TEXT that is not",
                        )
                    }),
                other => Err(bind_error(
                    col_name,
                    "JSON array TEXT or NULL",
                    shape_of(other),
                )),
            });
        }
        let vector = data_type.vector(self.catalog)?;
        Some(match value {
            Value::Null => Ok(Box::new(DefaultBinder::from(value))),
            Value::Blob(blob) if vector.kind() == VectorKind::Vector => {
                array::bind_vector(blob.as_ref(), vector.dimensions()).ok_or_else(|| {
                    bind_error(
                        col_name,
                        "a float32 BLOB of its dimensions",
                        "a BLOB that is not",
                    )
                })
            }
            other => Err(bind_error(
                col_name,
                "a float32 BLOB or NULL",
                shape_of(other),
            )),
        })
    }
}

impl<DB, S, B> Adapter<Pg, S, B> for PgAdapter<'_, DB>
where
    DB: DatabaseLike,
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    fn column_name(&self, table_name: &str, column_index: usize) -> &str {
        self.column_at(table_name, column_index)
            .map_or("", ColumnLike::column_name)
    }

    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError> {
        let Some(col) = self.column_at(table_name, column_index) else {
            return Err(unknown_column_error(table_name, column_index));
        };
        let dialect = self.catalog.dialect();
        let col_name = col.column_name();
        if let Some(binder) = self.collection_bind(col, value) {
            return binder;
        }

        // BOOLEAN: strict Integer -> Bool. Null is legitimate NULL.
        // Other wire shapes are refused with a rollback-inducing error
        // so the mismatch surfaces at the adapter, not two frames deep
        // inside PG's error text.
        if dialect.is_bool(self.catalog, col).is_yes() {
            return match value {
                Value::Integer(i) => Ok(bind_as::<Bool, bool>(*i != 0)),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => Err(bind_error(col_name, "INTEGER or NULL", shape_of(other))),
            };
        }

        // UUID: accept both 16-byte Blob and hyphenated Text. Null is
        // legitimate NULL. Other wire shapes are refused.
        if dialect.is_uuid(self.catalog, col).is_yes() {
            return match value {
                Value::Blob(b) => uuid::Uuid::from_slice(b.as_ref())
                    .map(|u| -> Box<dyn Binder<Pg> + Send + 'a> {
                        bind_as::<UuidSqlType, uuid::Uuid>(u)
                    })
                    .map_err(|_| {
                        bind_error(
                            col_name,
                            "16-byte BLOB or hyphenated TEXT",
                            &alloc::format!("BLOB of length {}", b.as_ref().len()),
                        )
                    }),
                Value::Text(s) => uuid::Uuid::parse_str(s.as_ref())
                    .map(|u| -> Box<dyn Binder<Pg> + Send + 'a> {
                        bind_as::<UuidSqlType, uuid::Uuid>(u)
                    })
                    .map_err(|_| {
                        bind_error(
                            col_name,
                            "16-byte BLOB or hyphenated TEXT",
                            "unparseable TEXT",
                        )
                    }),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => Err(bind_error(
                    col_name,
                    "16-byte BLOB or hyphenated TEXT",
                    shape_of(other),
                )),
            };
        }

        // Rich scalars Postgres will not assignment-cast from a text
        // bind: decimal and the temporals. Each parses the verbatim wire
        // text into its diesel type and binds it natively. Bool and UUID
        // are handled above, and everything else falls to DefaultBinder.
        match self
            .scalar_kind_at(table_name, column_index)
            .and_then(|kind| kind.family())
        {
            Some(ScalarFamily::Decimal) => decimal_bind(
                col_name,
                crate::catalog_helpers::minor_unit_scale(self.catalog, col.data_type(self.catalog)),
                value,
            ),
            Some(ScalarFamily::Timestamp) => {
                text_scalar_bind(col_name, value, "timestamp TEXT or NULL", |s| {
                    Some(bind_as::<Timestamp, NaiveDateTime>(parse_timestamp(s)?))
                })
            }
            Some(ScalarFamily::TimestampTz) => {
                text_scalar_bind(col_name, value, "timestamptz TEXT or NULL", |s| {
                    Some(bind_as::<Timestamptz, DateTime<Utc>>(parse_timestamp_tz(
                        s,
                    )?))
                })
            }
            Some(ScalarFamily::Date) => {
                text_scalar_bind(col_name, value, "date TEXT or NULL", |s| {
                    Some(bind_as::<Date, NaiveDate>(parse_date(s)?))
                })
            }
            Some(ScalarFamily::Time) => {
                text_scalar_bind(col_name, value, "time TEXT or NULL", |s| {
                    Some(bind_as::<Time, NaiveTime>(parse_time(s)?))
                })
            }
            Some(ScalarFamily::Json) => {
                text_scalar_bind(col_name, value, "json TEXT or NULL", |s| {
                    Some(bind_as::<Json, serde_json::Value>(
                        serde_json::from_str(s).ok()?,
                    ))
                })
            }
            Some(ScalarFamily::Jsonb) => {
                text_scalar_bind(col_name, value, "jsonb TEXT or NULL", |s| {
                    Some(bind_as::<Jsonb, serde_json::Value>(
                        serde_json::from_str(s).ok()?,
                    ))
                })
            }
            _ => Ok(Box::new(DefaultBinder::from(value))),
        }
    }
}

/// Bind a decimal column from minor units at a declared `scale`, or from its
/// verbatim decimal text without one.
fn decimal_bind<'a, S, B>(
    col_name: &str,
    scale: Option<u32>,
    value: &'a Value<S, B>,
) -> Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError>
where
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    match scale {
        Some(scale) => Ok(match minor_units(col_name, value, scale)? {
            Some(decimal) => bind_as::<Numeric, BigDecimal>(decimal),
            None => Box::new(DefaultBinder::from(value)),
        }),
        None => text_scalar_bind(col_name, value, "decimal TEXT or NULL", |s| {
            Some(bind_as::<Numeric, BigDecimal>(
                sql_scalar_text::parse_decimal(s)?,
            ))
        }),
    }
}

/// Bind a rich scalar carried as verbatim `Value::Text`. `parse` turns
/// the text into a native binder. `Value::Null` passes through as a
/// literal NULL, and any other wire shape (or a parse failure) is
/// refused with a rollback-inducing error naming the column and the
/// expected shape.
fn text_scalar_bind<'a, S, B>(
    col_name: &str,
    value: &'a Value<S, B>,
    expected: &str,
    parse: impl FnOnce(&str) -> Option<Box<dyn Binder<Pg> + Send + 'a>>,
) -> Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError>
where
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    match value {
        Value::Text(s) => {
            parse(s.as_ref()).ok_or_else(|| bind_error(col_name, expected, "unparseable TEXT"))
        }
        Value::Null => Ok(Box::new(DefaultBinder::from(value))),
        other => Err(bind_error(col_name, expected, shape_of(other))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    use diesel::pg::Pg;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{Adapter, Value};
    use sqlparser::dialect::PostgreSqlDialect;

    use super::PgAdapter;

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE things (id BIGINT PRIMARY KEY, active BOOLEAN);",
        )
        .unwrap()
    }

    /// A column or table the catalog does not know is refused at bind time
    /// with an error naming the lookup, never silently bound with a default.
    #[test]
    fn an_unknown_column_is_refused_at_bind() {
        let db = catalog();
        let adapter = PgAdapter::new(&db).expect("the catalog indexes");

        let err = Adapter::<Pg, String, Vec<u8>>::bind(&adapter, "things", 9, &Value::Integer(1))
            .map(drop)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("things") && err.contains('9'),
            "the refusal names the table and the column index, got {err:?}"
        );

        let err = Adapter::<Pg, String, Vec<u8>>::bind(&adapter, "ghosts", 0, &Value::Integer(1))
            .map(drop)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("ghosts"),
            "the refusal names the unknown table, got {err:?}"
        );
    }

    /// A `NUMERIC(p,s)` column arrives as the replica's integer count of
    /// minor units, so it refuses the decimal text a bare `NUMERIC` takes,
    /// naming the column, rather than binding a value off by `10^s`.
    #[test]
    fn a_scaled_numeric_binds_minor_units_and_refuses_text() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE ledger (id BIGINT PRIMARY KEY, price NUMERIC(10,2), bare NUMERIC);",
        )
        .unwrap();
        let adapter = PgAdapter::new(&db).expect("the catalog indexes");
        let bind = |column: usize, value: &Value<String, Vec<u8>>| {
            Adapter::<Pg, String, Vec<u8>>::bind(&adapter, "ledger", column, value)
                .map(drop)
                .map_err(|err| err.to_string())
        };

        assert_eq!(bind(1, &Value::Integer(1250)), Ok(()));
        assert_eq!(bind(1, &Value::Null), Ok(()));
        let err = bind(1, &Value::Text("12.50".to_string())).unwrap_err();
        assert!(
            err.contains("price"),
            "the refusal names the column, got {err:?}"
        );
        assert_eq!(bind(2, &Value::Text("12.50".to_string())), Ok(()));
        assert!(
            bind(2, &Value::Integer(1250)).is_err(),
            "a bare NUMERIC still takes only decimal text"
        );
        assert_eq!(
            bind(2, &Value::Text("twelve".to_string())),
            Err("column `bare` expects decimal TEXT or NULL, got unparseable TEXT".to_string()),
            "a bare NUMERIC refuses text that is not a decimal"
        );
    }
}
