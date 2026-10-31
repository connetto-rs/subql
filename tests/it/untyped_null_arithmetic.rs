//! Arithmetic over `NULL` alone, with no operand to type it by.
//!
//! Measured on PostgreSQL 16 and 18, MySQL 8.0.46 and SQLite 3.45:
//!
//! ```text
//! pg      - NULL, NULL + NULL, - * / % alike   ERROR operator is not unique
//! pg      + NULL, NULL + n, n * NULL           NULL
//! mysql   (- NULL) IS NULL, (NULL + NULL) ...  1
//! sqlite  (- NULL) IS NULL, (NULL + NULL) ...  1
//! ```

#![allow(clippy::unwrap_used)]

use crate::common::semantics::{notifies, register, served};
use sql_traits::structs::ParserDB;
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT)";

/// The arithmetic PostgreSQL refuses, as filters that are otherwise true.
const AMBIGUOUS: [&str; 7] = [
    "(- NULL) IS NULL",
    "(- (NULL)) IS NULL",
    "(NULL + NULL) IS NULL",
    "(NULL - NULL) IS NULL",
    "(NULL * NULL) IS NULL",
    "(NULL / NULL) IS NULL",
    "((NULL) % (NULL)) IS NULL",
];

/// PostgreSQL will not run arithmetic it cannot pick an operator for, so
/// registration says so rather than answering or promising a read.
#[test]
fn postgres_refuses_arithmetic_over_null_alone() {
    for predicate in AMBIGUOUS {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(DDL).unwrap();
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, sqlparser::dialect::PostgreSqlDialect {});
        match engine.register(SubscriptionRequest::new(
            1u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        )) {
            Err(RegisterError::RefusedByEngine { reason, .. }) => assert!(
                reason.contains("operator is not unique"),
                "PostgreSQL's own account travels with the refusal: {reason}"
            ),
            other => panic!("{predicate}: expected a refusal by the engine, got {other:?}"),
        }
    }
}

/// A `NULL` beside a typed operand takes its type, and unary `+` over one
/// is answered, so PostgreSQL runs these.
#[test]
fn postgres_serves_null_beside_a_typed_operand() {
    for predicate in [
        "(+ NULL) IS NULL",
        "(NULL + n) IS NULL",
        "(n * NULL) IS NULL",
    ] {
        assert!(
            served::<Postgres>(DDL, &format!("SELECT * FROM t WHERE {predicate}")),
            "{predicate}"
        );
    }
}

/// MySQL and SQLite answer `NULL` for the same arithmetic, so neither
/// refuses it. MySQL reads a quotient whose precision is undeclared from
/// the database, so its division is a read rather than an answer.
#[test]
fn mysql_and_sqlite_answer_null() {
    for predicate in AMBIGUOUS {
        let statement = format!("SELECT * FROM t WHERE {predicate}");
        register::<MySql>(DDL, &statement);
        assert!(
            notifies::<SQLite>(DDL, "t", &statement, vec![Value::Int(1), Value::Null]),
            "SQLite: {predicate}"
        );
    }
}
