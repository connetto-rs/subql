use crate::{catalog_helpers, RegisterError, TableId};
use alloc::string::{String, ToString};
use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::TargetName;
use sqlparser::ast::{Ident, ObjectName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableResolutionError {
    Ambiguous {
        qualified: String,
        unqualified: String,
        qualified_id: TableId,
        unqualified_id: TableId,
    },
    Unknown {
        qualified: Option<String>,
        unqualified: String,
    },
}

/// A table reference a parsed statement wrote, each part keeping the quoting
/// it was written with.
///
/// Rendering the parts back to text and reading that text again would lose
/// the quoting, so `"Reports"` would reach `reports`, and would read a dot
/// inside a quoted part as a qualifier.
pub struct SqlTableName<'a> {
    written: &'a ObjectName,
    name: &'a Ident,
    /// The qualifier, when the reference is a qualifier and a name. A longer
    /// reference names a catalog subql does not model, and resolves by its
    /// name alone.
    schema: Option<&'a Ident>,
}

impl<'a> SqlTableName<'a> {
    /// Read the parts of `written`.
    ///
    /// # Errors
    ///
    /// [`RegisterError::UnsupportedSql`] when a part is not an identifier.
    pub fn new(written: &'a ObjectName) -> Result<Self, RegisterError> {
        let missing = || RegisterError::UnsupportedSql("Missing table name".to_string());
        let mut name = None;
        let mut schema = None;
        for part in &written.0 {
            schema = name.replace(part.as_ident().ok_or_else(missing)?);
        }
        Ok(Self {
            written,
            name: name.ok_or_else(missing)?,
            schema: if written.0.len() == 2 { schema } else { None },
        })
    }

    /// Resolve the reference under the engine's comparison for written names.
    ///
    /// # Errors
    ///
    /// [`RegisterError::AmbiguousTable`] when the qualifier and the search path
    /// reach different tables, and [`RegisterError::UnknownTable`], naming the
    /// reference as written, when neither reaches one.
    pub fn resolve<B: crate::backend::Backend, DB: DatabaseLike>(
        &self,
        database: &DB,
    ) -> Result<TableId, RegisterError> {
        let name = crate::compiler::sql_shape::written_name_part(self.name);
        let qualified = self.schema.map(|schema| {
            name.clone()
                .with_schema(&schema.value, schema.quote_style.is_some())
        });
        resolve_table_reference::<B, DB>(qualified.as_ref(), &name, database).map_err(|error| {
            match error {
                TableResolutionError::Ambiguous {
                    qualified,
                    unqualified,
                    ..
                } => RegisterError::AmbiguousTable {
                    reference: qualified.clone(),
                    qualified,
                    unqualified,
                },
                TableResolutionError::Unknown { .. } => RegisterError::UnknownTable(self.written()),
            }
        })
    }

    /// The reference as the statement wrote it, quoting included.
    pub fn written(&self) -> String {
        self.written.to_string()
    }
}

/// Resolve a written reference through its qualifier, and through the search
/// path by its name alone, refusing one the two resolve differently.
pub fn resolve_table_reference<B: crate::backend::Backend, DB: DatabaseLike>(
    qualified: Option<&TargetName<'_>>,
    unqualified: &TargetName<'_>,
    database: &DB,
) -> Result<TableId, TableResolutionError> {
    let qualified_id = qualified
        .and_then(|target| catalog_helpers::table_id_for_name::<B, DB>(database, target.clone()));
    let unqualified_id = catalog_helpers::table_id_for_name::<B, DB>(database, unqualified.clone());
    resolve_table_ids(qualified_id, unqualified_id, || {
        (qualified.map(ToString::to_string), unqualified.to_string())
    })
}

/// Resolve table-name parts carried as wire identifier values.
pub fn resolve_table_parts<B: crate::backend::Backend, DB: DatabaseLike>(
    schema: Option<&str>,
    table: &str,
    database: &DB,
) -> Result<TableId, TableResolutionError> {
    let qualified_id = schema.and_then(|schema| {
        catalog_helpers::table_id_in_schema::<B, DB>(database, Some(schema), table)
    });
    let unqualified_id = catalog_helpers::table_id_in_schema::<B, DB>(database, None, table);
    resolve_table_ids(qualified_id, unqualified_id, || {
        (
            schema.map(|schema| alloc::format!("{schema}.{table}")),
            table.to_string(),
        )
    })
}

/// Settle the two lookups, building the names a refusal reports only when
/// there is one.
fn resolve_table_ids(
    qualified_id: Option<TableId>,
    unqualified_id: Option<TableId>,
    names: impl FnOnce() -> (Option<String>, String),
) -> Result<TableId, TableResolutionError> {
    match (qualified_id, unqualified_id) {
        (Some(q), Some(u)) if q != u => {
            let (qualified, unqualified) = names();
            Err(TableResolutionError::Ambiguous {
                qualified: qualified.unwrap_or_default(),
                unqualified,
                qualified_id: q,
                unqualified_id: u,
            })
        }
        (Some(q), _) => Ok(q),
        (None, Some(u)) => Ok(u),
        (None, None) => {
            let (qualified, unqualified) = names();
            Err(TableResolutionError::Unknown {
                qualified,
                unqualified,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::GenericDialect;

    fn parse(sql: &str) -> ParserDB {
        ParserDB::parse::<GenericDialect>(sql).expect("DDL parses")
    }

    #[test]
    fn resolves_known_table() {
        let db = parse("CREATE TABLE orders (id INT);");
        let resolved = resolve_table_reference::<crate::backend::Postgres, _>(
            None,
            &TargetName::new("orders", false),
            &db,
        )
        .expect("resolution should succeed");
        assert_eq!(
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, "orders"),
            Some(resolved)
        );
    }

    /// A wire message's table name is an identifier value, so a dot inside
    /// it belongs to the name.
    ///
    /// [`resolve_table_parts`] takes what a CDC message carried, where the
    /// quoting a written name would have shown is already gone. Reading it
    /// as SQL text would make `my.table` a qualifier and a name, and the
    /// schema `my` does not exist.
    #[test]
    fn wire_parts_keep_a_dotted_name_whole() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE TABLE "my.table" (id INT);"#,
        )
        .expect("DDL parses");
        let expected =
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, r#""my.table""#)
                .expect("the quoted spelling resolves");

        assert_eq!(
            resolve_table_parts::<crate::backend::Postgres, _>(None, "my.table", &db),
            Ok(expected)
        );
        assert_eq!(
            resolve_table_parts::<crate::backend::Postgres, _>(Some("public"), "my.table", &db),
            Ok(expected),
            "and the schema the catalog stores it under reaches it too"
        );
    }

    #[test]
    fn wire_parts_match_a_stored_mixed_case_name_exactly() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE SCHEMA app;
               CREATE SCHEMA "App";
               CREATE TABLE app.docs (id INT);
               CREATE TABLE "App"."Docs" (id INT);"#,
        )
        .expect("DDL parses");
        let expected =
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, r#""App"."Docs""#)
                .expect("the mixed-case table exists");
        let folded = catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, "app.docs")
            .expect("the folded table exists");

        assert_ne!(expected, folded);
        assert_eq!(
            resolve_table_parts::<crate::backend::Postgres, _>(Some("App"), "Docs", &db),
            Ok(expected)
        );
    }

    /// pgoutput and wal2json report the schema and relation as PostgreSQL
    /// stores them, so an unquoted catalog name is reached by its folded
    /// spelling and a quoted one only by its own.
    #[test]
    fn wire_parts_match_an_unquoted_catalog_name_as_postgres_stores_it() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE SCHEMA Sales;
               CREATE TABLE Sales.Orders (id INT);
               CREATE TABLE Sales."Ledger" (id INT);"#,
        )
        .expect("DDL parses");
        let written = |name| catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, name);
        let wire = |schema, table| {
            resolve_table_parts::<crate::backend::Postgres, _>(Some(schema), table, &db).ok()
        };

        assert_eq!(wire("sales", "orders"), written("sales.orders"));
        assert_eq!(wire("sales", "Ledger"), written(r#"sales."Ledger""#));
        assert!(written("sales.orders").is_some() && written(r#"sales."Ledger""#).is_some());
        assert_eq!(wire("Sales", "Orders"), None);
        assert_eq!(wire("sales", "ledger"), None);
    }

    #[test]
    fn mysql_wire_names_follow_lower_case_table_names() {
        use crate::backend::{MySql, NamesFoldedAtLookup, NamesStoredLowercased};

        let db = ParserDB::parse::<sqlparser::dialect::MySqlDialect>("CREATE TABLE Docs (id INT);")
            .expect("DDL parses");

        assert_eq!(
            resolve_table_parts::<MySql<NamesStoredLowercased>, _>(None, "docs", &db),
            Ok(0)
        );
        assert_eq!(
            resolve_table_parts::<MySql<NamesFoldedAtLookup>, _>(None, "docs", &db),
            Ok(0)
        );
    }

    #[test]
    fn sqlite_wire_names_fold_like_the_engine() {
        let db =
            ParserDB::parse::<sqlparser::dialect::SQLiteDialect>("CREATE TABLE Docs (id INT);")
                .expect("DDL parses");

        assert_eq!(
            resolve_table_parts::<crate::backend::SQLite, _>(None, "docs", &db),
            Ok(0)
        );
    }

    #[test]
    fn unknown_contains_best_available_reference() {
        let db = parse("CREATE TABLE elsewhere (id INT);");

        let err = resolve_table_reference::<crate::backend::Postgres, _>(
            Some(&TargetName::new("orders", false).with_schema("public", false)),
            &TargetName::new("orders", false),
            &db,
        )
        .expect_err("missing table should fail");
        assert!(matches!(
            err,
            TableResolutionError::Unknown {
                qualified: Some(qualified),
                unqualified,
            } if qualified == "public.orders" && unqualified == "orders"
        ));

        let err_unqualified = resolve_table_reference::<crate::backend::Postgres, _>(
            None,
            &TargetName::new("orders", false),
            &db,
        )
        .expect_err("unqualified missing table should fail");
        assert!(matches!(
            err_unqualified,
            TableResolutionError::Unknown {
                qualified: None,
                unqualified,
            } if unqualified == "orders"
        ));
    }
}
