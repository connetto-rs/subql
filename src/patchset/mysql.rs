//! MySQL adapter for [`sqlite_diff_rs`] patchset application.
//!
//! Dispatches MySQL `TINYINT(1)` / `BOOLEAN` columns to native `bool`
//! binds when the wire carries an SQLite `Value::Integer`, and
//! `DECIMAL(p,s)` columns to native `DECIMAL` binds from the integer count
//! of minor units a replica stores. All other columns fall through to
//! [`sqlite_diff_rs::DefaultBinder`].

use alloc::boxed::Box;

use diesel::mysql::Mysql;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::sql_types::{Bool, Numeric};
use sql_traits::prelude::{ColumnLike, DatabaseLike, DialectLike, TypeMatchLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use super::columns::{bind_error, minor_units, shape_of, unknown_column_error, ColumnIndex};

/// Adapter that resolves column names and native diesel binders for a
/// MySQL target from a subql catalog.
///
/// Dispatches MySQL `TINYINT(1)` / `BOOLEAN` columns to native `bool`
/// binds when the wire carries an SQLite `Value::Integer`, and
/// `DECIMAL(p,s)` columns to native `DECIMAL` binds from minor units. All
/// other columns fall through to [`DefaultBinder`].
#[derive(Debug)]
pub struct MysqlAdapter<'db, DB: DatabaseLike> {
    catalog: &'db DB,
    columns: ColumnIndex<'db, DB>,
}

impl<'db, DB: DatabaseLike> MysqlAdapter<'db, DB> {
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
}

impl<DB, S, B> Adapter<Mysql, S, B> for MysqlAdapter<'_, DB>
where
    DB: DatabaseLike,
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    fn column_name(&self, table_name: &str, column_index: usize) -> &str {
        self.columns
            .column_at(table_name, column_index)
            .map_or("", ColumnLike::column_name)
    }

    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> Result<Box<dyn Binder<Mysql> + Send + 'a>, diesel::result::Error> {
        // MySQL `TINYINT(1)` (aka `BOOLEAN`) columns arrive as
        // `Value::Integer` on the wire (SQLite has no native bool).
        // Native-bind those as `Bool`. Null is legitimate NULL. Any
        // other wire shape on a boolean column is refused with a
        // rollback-inducing error.
        let Some(col) = self.columns.column_at(table_name, column_index) else {
            return Err(unknown_column_error(table_name, column_index));
        };
        if self.catalog.dialect().is_bool(self.catalog, col).is_yes() {
            return match value {
                Value::Integer(i) => Ok(Box::new(BoolBinder(*i != 0))),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => Err(bind_error(
                    col.column_name(),
                    "INTEGER or NULL",
                    shape_of(other),
                )),
            };
        }
        // `DECIMAL(p,s)` arrives as the replica's integer count of minor
        // units, `value × 10^s`.
        if let Some(scale) =
            crate::catalog_helpers::minor_unit_scale(self.catalog, col.data_type(self.catalog))
        {
            return Ok(match minor_units(col.column_name(), value, scale)? {
                Some(decimal) => Box::new(DecimalBinder(decimal)),
                None => Box::new(DefaultBinder::from(value)),
            });
        }
        Ok(Box::new(DefaultBinder::from(value)))
    }
}

/// Binder that pushes a boolean value onto the AST as a native
/// [`Bool`] bind. Constructed by [`MysqlAdapter`] when a column resolved
/// to MySQL `TINYINT(1)` / `BOOLEAN` receives an SQLite `Value::Integer`
/// on the wire.
struct BoolBinder(bool);

impl Binder<Mysql> for BoolBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Mysql>) -> QueryResult<()> {
        out.push_bind_param::<Bool, bool>(&self.0)
    }
}

/// Binder that pushes a decimal onto the AST as a native [`Numeric`] bind.
/// Constructed by [`MysqlAdapter`] when a `DECIMAL(p,s)` column receives
/// the replica's integer count of minor units.
struct DecimalBinder(bigdecimal::BigDecimal);

impl Binder<Mysql> for DecimalBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Mysql>) -> QueryResult<()> {
        out.push_bind_param::<Numeric, bigdecimal::BigDecimal>(&self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    use diesel::mysql::Mysql;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{Adapter, Value};
    use sqlparser::dialect::MySqlDialect;

    use super::MysqlAdapter;

    fn catalog() -> ParserDB {
        ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE things (id BIGINT PRIMARY KEY, active TINYINT(1));",
        )
        .unwrap()
    }

    /// A column or table the catalog does not know is refused at bind time
    /// with an error naming the lookup, never silently bound with a default.
    #[test]
    fn an_unknown_column_is_refused_at_bind() {
        let db = catalog();
        let adapter = MysqlAdapter::new(&db).expect("the catalog indexes");

        let err =
            Adapter::<Mysql, String, Vec<u8>>::bind(&adapter, "things", 9, &Value::Integer(1))
                .map(drop)
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("things") && err.contains('9'),
            "the refusal names the table and the column index, got {err:?}"
        );

        let err =
            Adapter::<Mysql, String, Vec<u8>>::bind(&adapter, "ghosts", 0, &Value::Integer(1))
                .map(drop)
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("ghosts"),
            "the refusal names the unknown table, got {err:?}"
        );
    }

    /// A boolean column takes NULL. SQLite has no boolean storage class, so a
    /// nullable `TINYINT(1)` cell arrives as `Value::Null`, which is a
    /// legitimate NULL and must bind rather than be refused as a shape the
    /// column cannot take.
    #[test]
    fn null_binds_on_a_boolean_column() {
        let db = catalog();
        let adapter = MysqlAdapter::new(&db).expect("the catalog indexes");

        Adapter::<Mysql, String, Vec<u8>>::bind(&adapter, "things", 1, &Value::Null)
            .map(drop)
            .expect("NULL is a legitimate value for a boolean column");
    }

    /// Every other wire shape on a boolean column is refused, naming the
    /// column, the shapes it takes and the shape it got, so the statement is
    /// never built around a coerced value.
    #[test]
    fn a_non_integer_wire_shape_on_a_boolean_column_is_refused() {
        let db = catalog();
        let adapter = MysqlAdapter::new(&db).expect("the catalog indexes");

        for (value, shape) in [
            (Value::Text("yes".to_string()), "TEXT"),
            (Value::Real(1.0), "REAL"),
            (Value::Blob(Vec::from([1_u8])), "BLOB"),
        ] {
            let err = Adapter::<Mysql, String, Vec<u8>>::bind(&adapter, "things", 1, &value)
                .map(drop)
                .unwrap_err()
                .to_string();
            assert_eq!(
                err,
                alloc::format!("column `active` expects INTEGER or NULL, got {shape}"),
                "the refusal names the column, the expectation and the wire shape"
            );
        }
    }

    /// A `DECIMAL(p,s)` column arrives as the replica's integer count of
    /// minor units, so it binds an integer and refuses decimal text, naming
    /// the column, rather than storing a value off by `10^s`.
    #[test]
    fn a_scaled_decimal_binds_minor_units_and_refuses_text() {
        let db = ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE ledger (id BIGINT PRIMARY KEY, price DECIMAL(10,2));",
        )
        .unwrap();
        let adapter = MysqlAdapter::new(&db).expect("the catalog indexes");
        let bind = |value: &Value<String, Vec<u8>>| {
            Adapter::<Mysql, String, Vec<u8>>::bind(&adapter, "ledger", 1, value)
                .map(drop)
                .map_err(|err| err.to_string())
        };

        assert_eq!(bind(&Value::Integer(1250)), Ok(()));
        assert_eq!(bind(&Value::Null), Ok(()));
        assert_eq!(
            bind(&Value::Text("12.50".to_string())),
            Err("column `price` expects minor-units INTEGER or NULL, got TEXT".to_string())
        );
    }
}
