//! The backend each test dialect is judged under.

use sqlparser::dialect::{
    BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect, GenericDialect,
    MsSqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect, SnowflakeDialect,
};

use crate::backend::{Backend, MySql, Postgres, SQLite};

/// A dialect a test parses with, and the backend whose rules judge it.
///
/// A dialect subql ships no backend for keeps its declared type names, as
/// SQLite's backend does.
pub trait TestDialect: Dialect + Default {
    type Backend: Backend;
}

impl TestDialect for PostgreSqlDialect {
    type Backend = Postgres;
}

impl TestDialect for MySqlDialect {
    type Backend = MySql;
}

impl TestDialect for SQLiteDialect {
    type Backend = SQLite;
}

macro_rules! declared_names {
    ($($dialect:ty),* $(,)?) => {
        $(impl TestDialect for $dialect {
            type Backend = SQLite;
        })*
    };
}

declared_names!(
    BigQueryDialect,
    ClickHouseDialect,
    DatabricksDialect,
    DuckDbDialect,
    GenericDialect,
    MsSqlDialect,
    SnowflakeDialect,
);
