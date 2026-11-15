//! A table a statement names in quotes is the table of exactly that
//! spelling, even beside one whose unquoted name folds to the same letters.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

/// `reports` and `"Reports"` are two PostgreSQL tables.
const TWINS: &str = r#"CREATE TABLE reports (id INT PRIMARY KEY, title TEXT);
    CREATE TABLE "Reports" (id INT PRIMARY KEY, title TEXT);"#;

/// The same pair under a schema the search path does not reach.
const SCHEMA_TWINS: &str = r#"CREATE SCHEMA app;
    CREATE TABLE app.reports (id INT PRIMARY KEY, title TEXT);
    CREATE TABLE app."Reports" (id INT PRIMARY KEY, title TEXT);"#;

/// A quoted name holding a dot, beside the qualified table that spelling
/// would name without its quotes.
const DOTTED: &str = r#"CREATE SCHEMA my;
    CREATE TABLE my.reports (id INT PRIMARY KEY, title TEXT);
    CREATE TABLE "my.reports" (id INT PRIMARY KEY, title TEXT);"#;

/// Which of `tables`, each written as SQL, hear an insert of row 1 once
/// `subscribe` has registered on an engine over `ddl`.
fn tables_heard(
    ddl: &str,
    tables: &[&'static str],
    subscribe: impl FnOnce(&mut Engine),
) -> Vec<&'static str> {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).expect("DDL parses");
    let ids: Vec<_> = tables
        .iter()
        .map(|table| {
            catalog_helpers::table_id::<Postgres, _>(&catalog, table)
                .unwrap_or_else(|| panic!("{table} is in the catalog"))
        })
        .collect();
    let mut engine = Engine::new(catalog, PostgreSqlDialect {});
    subscribe(&mut engine);

    let mut heard = Vec::new();
    for (table, id) in tables.iter().zip(ids) {
        let event = TestEvent::insert(id, vec![Value::Int(1), Value::String("t".into())])
            .with_pk_columns([0u16]);
        if !engine.consumers(&event).unwrap().inserted().is_empty() {
            heard.push(*table);
        }
    }
    heard
}

/// The tables a subscription to `sql` hears.
fn heard_by(ddl: &str, tables: &[&'static str], sql: &str) -> Vec<&'static str> {
    tables_heard(ddl, tables, |engine| {
        engine
            .register(SubscriptionRequest::new(1u64, sql))
            .unwrap_or_else(|error| panic!("{sql} registers: {error}"));
    })
}

/// The tables a follow of row 1 of `table`, written as SQL, hears.
fn followed(ddl: &str, tables: &[&'static str], table: &str) -> Vec<&'static str> {
    tables_heard(ddl, tables, |engine| {
        engine
            .follow_row(1u64, table, vec![Value::Int(1)])
            .unwrap_or_else(|error| panic!("{table} is followed: {error}"));
    })
}

#[test]
fn a_quoted_table_is_the_one_spelled_so_beside_its_folded_twin() {
    let tables = ["reports", r#""Reports""#];

    assert_eq!(
        heard_by(TWINS, &tables, r#"SELECT * FROM "Reports""#),
        [r#""Reports""#]
    );
    assert_eq!(
        heard_by(TWINS, &tables, "SELECT * FROM reports"),
        ["reports"]
    );
    assert_eq!(
        heard_by(TWINS, &tables, "SELECT * FROM Reports"),
        ["reports"],
        "an unquoted name folds"
    );
}

#[test]
fn a_qualified_quoted_table_is_the_one_spelled_so() {
    let tables = ["app.reports", r#"app."Reports""#];

    assert_eq!(
        heard_by(SCHEMA_TWINS, &tables, r#"SELECT * FROM app."Reports""#),
        [r#"app."Reports""#]
    );
    assert_eq!(
        heard_by(SCHEMA_TWINS, &tables, "SELECT * FROM app.Reports"),
        ["app.reports"]
    );
}

#[test]
fn a_quoted_name_holding_a_dot_is_one_table() {
    let tables = ["my.reports", r#""my.reports""#];

    assert_eq!(
        heard_by(DOTTED, &tables, r#"SELECT * FROM "my.reports""#),
        [r#""my.reports""#]
    );
    assert_eq!(
        heard_by(DOTTED, &tables, "SELECT * FROM my.reports"),
        ["my.reports"]
    );
}

/// `follow_row` reads its table as SQL text and follows the row of the
/// table that text names, whatever spelling the derived subscription uses
/// to name it again.
#[test]
fn a_followed_row_belongs_to_the_table_its_name_resolves_to() {
    let tables = ["reports", r#""Reports""#];
    assert_eq!(followed(TWINS, &tables, "Reports"), ["reports"]);
    assert_eq!(followed(TWINS, &tables, r#""Reports""#), [r#""Reports""#]);

    let tables = ["app.reports", r#"app."Reports""#];
    assert_eq!(
        followed(SCHEMA_TWINS, &tables, "app.reports"),
        ["app.reports"]
    );
    assert_eq!(
        followed(SCHEMA_TWINS, &tables, r#"app."Reports""#),
        [r#"app."Reports""#]
    );

    let tables = ["my.reports", r#""my.reports""#];
    assert_eq!(
        followed(DOTTED, &tables, r#""my.reports""#),
        [r#""my.reports""#]
    );
}
