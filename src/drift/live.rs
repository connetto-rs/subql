//! The live re-read: a database connection that loads its own catalog,
//! and the readers that turn it into the guard's [`LiveShapeReader`].

use alloc::vec::Vec;

use sql_traits::errors::LookupError;
use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::TargetName;

use super::guard::{LiveShapeReader, RulesLoad};
use super::rules::TableRules;
use super::shape::TableShape;
use crate::backend::Backend;

/// A connection to a live database that loads that database's catalog.
///
/// Implemented once per diesel connection type, behind its backend's
/// feature. The load is synchronous on the connection and only reads, so a
/// failed or panicked load leaves the connection usable for the next one.
/// [`InlineLiveReader`] runs it on the task that awaits the read, and
/// [`BlockingLiveReader`] runs it on tokio's blocking pool.
///
/// A table is named as the catalog names it, by its optional schema and its
/// name. PostgreSQL selects an explicit schema exactly and resolves a name
/// without one through the session's search path, while SQLite ignores the
/// schema.
pub trait LiveCatalog {
    /// The backend whose rules name the live shape's columns.
    type Backend: Backend;
    /// The catalog a load answers.
    type Database: DatabaseLike;
    /// The error a load or a shape can surface.
    type Error: core::error::Error + From<LookupError> + Send + Sync + 'static;

    /// Load the live catalog of the `tables` named. A table the database
    /// lacks is absent from the answer.
    ///
    /// # Errors
    ///
    /// The backend's error when the read fails or its answer does not load.
    fn load_tables(&mut self, tables: &[TargetName<'_>]) -> Result<Self::Database, Self::Error>;

    /// The table `table` names in a `database` this connection loaded, or
    /// `None` when the database lacks it.
    ///
    /// # Errors
    ///
    /// The backend's error when the name does not resolve, as when it
    /// matches more than one table.
    fn find_table<'d>(
        database: &'d Self::Database,
        table: TargetName<'_>,
    ) -> Result<Option<&'d <Self::Database as DatabaseLike>::Table>, Self::Error>;

    /// The shape of `table` in a `database` this connection loaded. A
    /// backend whose live shape holds facts its catalog does not model
    /// reads them through the connection.
    ///
    /// # Errors
    ///
    /// The backend's error when a lookup on the table or a read of those
    /// facts fails.
    fn table_shape(
        &mut self,
        database: &Self::Database,
        table: &<Self::Database as DatabaseLike>::Table,
    ) -> Result<TableShape, Self::Error> {
        TableShape::from_table::<Self::Backend, _>(table, database).map_err(Into::into)
    }
}

/// The live shape of `table`, or `None` when the database lacks it.
fn read_shape<C: LiveCatalog>(
    connection: &mut C,
    table: TargetName<'_>,
) -> Result<Option<TableShape>, C::Error> {
    let database = connection.load_tables(core::slice::from_ref(&table))?;
    let shape = C::find_table(&database, table)?
        .map(|found| connection.table_shape(&database, found))
        .transpose();
    shape
}

/// The live shape and rules of `table` from one load, or `None` when the
/// database lacks it.
fn read_shape_and_rules<C: LiveCatalog>(
    connection: &mut C,
    table: TargetName<'_>,
) -> Result<Option<(TableShape, TableRules)>, C::Error> {
    let database = connection.load_tables(core::slice::from_ref(&table))?;
    let Some(found) = C::find_table(&database, table)? else {
        return Ok(None);
    };
    let rules = TableRules::of(found, &database)?;
    let shape = connection.table_shape(&database, found)?;
    Ok(Some((shape, rules)))
}

/// One load of `tables`, with the rules of each in order, `None` for a
/// table the database lacks.
fn read_rules<C: LiveCatalog>(
    connection: &mut C,
    tables: &[TargetName<'_>],
) -> Result<RulesLoad<C::Database>, C::Error> {
    let database = connection.load_tables(tables)?;
    let rules = tables
        .iter()
        .map(|table| {
            C::find_table(&database, table.clone())?
                .map(|found| TableRules::of(found, &database).map_err(C::Error::from))
                .transpose()
        })
        .collect::<Result<Vec<_>, C::Error>>()?;
    Ok((database, rules))
}

/// A [`LiveShapeReader`] that loads on the task awaiting the read.
///
/// It needs no runtime, and every read completes in its first poll.
pub struct InlineLiveReader<C> {
    connection: C,
}

impl<C> InlineLiveReader<C> {
    /// A reader over a connection to the live database.
    #[must_use]
    pub const fn new(connection: C) -> Self {
        Self { connection }
    }

    /// The connection the reader loads through.
    pub const fn connection(&mut self) -> &mut C {
        &mut self.connection
    }
}

impl<C: LiveCatalog + Send> LiveShapeReader for InlineLiveReader<C> {
    type Error = C::Error;
    type Database = C::Database;

    fn shape<'r>(
        &'r mut self,
        table: TargetName<'r>,
    ) -> impl core::future::Future<Output = Result<Option<TableShape>, Self::Error>> + Send + 'r
    {
        core::future::ready(read_shape(&mut self.connection, table))
    }

    fn shape_and_rules<'r>(
        &'r mut self,
        table: TargetName<'r>,
    ) -> impl core::future::Future<Output = Result<Option<(TableShape, TableRules)>, Self::Error>>
           + Send
           + 'r {
        core::future::ready(read_shape_and_rules(&mut self.connection, table))
    }

    fn rules(
        &mut self,
        tables: Vec<TargetName<'static>>,
    ) -> impl core::future::Future<Output = Result<RulesLoad<Self::Database>, Self::Error>> + Send + '_
    {
        core::future::ready(read_rules(&mut self.connection, &tables))
    }
}

/// The error a [`BlockingLiveReader`] read can surface.
#[cfg(feature = "pg-streaming")]
#[derive(Debug, thiserror::Error)]
pub enum BlockingReadError<E> {
    /// The load failed.
    #[error("live catalog read: {0}")]
    Read(#[source] E),
    /// The runtime shut down before the load ran.
    #[error("the runtime shut down before the live catalog read ran")]
    Cancelled,
}

/// A [`LiveShapeReader`] that loads on tokio's blocking pool.
///
/// A read dropped before it completes leaves its load running to the end,
/// and the next read waits for the connection. A panicking load resumes on
/// the task awaiting the read. A read must be polled within a tokio runtime.
#[cfg(feature = "pg-streaming")]
pub struct BlockingLiveReader<C> {
    connection: std::sync::Arc<std::sync::Mutex<C>>,
}

#[cfg(feature = "pg-streaming")]
impl<C> BlockingLiveReader<C> {
    /// A reader over a connection to the live database.
    #[must_use]
    pub fn new(connection: C) -> Self {
        Self {
            connection: std::sync::Arc::new(std::sync::Mutex::new(connection)),
        }
    }
}

#[cfg(feature = "pg-streaming")]
impl<C> BlockingLiveReader<C>
where
    C: LiveCatalog + Send + 'static,
{
    /// Run `load` on the connection on tokio's blocking pool.
    fn on_blocking_pool<T: Send + 'static>(
        &self,
        load: impl FnOnce(&mut C) -> Result<T, C::Error> + Send + 'static,
    ) -> impl core::future::Future<Output = Result<T, BlockingReadError<C::Error>>> + Send + 'static
    {
        let connection = std::sync::Arc::clone(&self.connection);
        async move {
            let load = tokio::task::spawn_blocking(move || {
                // A panicked load only read, so its poisoned lock still guards a usable connection.
                let mut connection = connection
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                load(&mut *connection)
            });
            match load.await {
                Ok(read) => read.map_err(BlockingReadError::Read),
                Err(error) => error.try_into_panic().map_or_else(
                    |_| Err(BlockingReadError::Cancelled),
                    |payload| std::panic::resume_unwind(payload),
                ),
            }
        }
    }
}

#[cfg(feature = "pg-streaming")]
impl<C> LiveShapeReader for BlockingLiveReader<C>
where
    C: LiveCatalog + Send + 'static,
    C::Database: Send + 'static,
{
    type Error = BlockingReadError<C::Error>;
    type Database = C::Database;

    fn shape<'r>(
        &'r mut self,
        table: TargetName<'r>,
    ) -> impl core::future::Future<Output = Result<Option<TableShape>, Self::Error>> + Send + 'r
    {
        let table = table.into_owned();
        self.on_blocking_pool(move |connection| read_shape(connection, table))
    }

    fn shape_and_rules<'r>(
        &'r mut self,
        table: TargetName<'r>,
    ) -> impl core::future::Future<Output = Result<Option<(TableShape, TableRules)>, Self::Error>>
           + Send
           + 'r {
        let table = table.into_owned();
        self.on_blocking_pool(move |connection| read_shape_and_rules(connection, table))
    }

    fn rules(
        &mut self,
        tables: Vec<TargetName<'static>>,
    ) -> impl core::future::Future<Output = Result<RulesLoad<Self::Database>, Self::Error>> + Send + '_
    {
        self.on_blocking_pool(move |connection| read_rules(connection, &tables))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "The fixtures must be valid.")]
mod tests {
    use super::*;
    use alloc::collections::VecDeque;
    use alloc::string::String;
    use core::future::Future;
    use core::task::{Context, Poll, Waker};
    use sql_traits::structs::{IdentifierCase, ParserDB};
    use sqlparser::dialect::PostgreSqlDialect;

    use crate::testing::block_on;

    /// What the next load of a [`FakeCatalog`] does.
    enum Next {
        Answer,
        Fail,
        #[cfg(feature = "pg-streaming")]
        Panic,
        #[cfg(feature = "pg-streaming")]
        WaitFor(std::sync::mpsc::Receiver<()>),
    }

    #[derive(Debug, thiserror::Error)]
    enum FakeError {
        #[error("lookup: {0}")]
        Lookup(#[from] sql_traits::errors::LookupError),
        #[error("the live database refused the read")]
        Refused,
    }

    /// A live database holding `ddl`, whose loads follow `script`.
    struct FakeCatalog {
        ddl: &'static str,
        script: VecDeque<Next>,
        loads: Vec<Vec<String>>,
    }

    impl FakeCatalog {
        fn new(ddl: &'static str, script: impl IntoIterator<Item = Next>) -> Self {
            Self {
                ddl,
                script: script.into_iter().collect(),
                loads: Vec::new(),
            }
        }
    }

    impl LiveCatalog for FakeCatalog {
        type Backend = crate::backend::Postgres;
        type Database = ParserDB;
        type Error = FakeError;

        fn load_tables(&mut self, tables: &[TargetName<'_>]) -> Result<ParserDB, FakeError> {
            self.loads
                .push(tables.iter().map(ToString::to_string).collect());
            match self.script.pop_front().unwrap_or(Next::Answer) {
                Next::Answer => {}
                Next::Fail => return Err(FakeError::Refused),
                #[cfg(feature = "pg-streaming")]
                Next::Panic => panic!("the live load panicked"),
                #[cfg(feature = "pg-streaming")]
                Next::WaitFor(release) => release.recv().unwrap(),
            }
            Ok(ParserDB::parse::<PostgreSqlDialect>(self.ddl).unwrap())
        }

        /// Compares names as SQLite does, folding both sides.
        fn find_table<'d>(
            database: &'d ParserDB,
            table: TargetName<'_>,
        ) -> Result<Option<&'d <ParserDB as DatabaseLike>::Table>, FakeError> {
            Ok(database.table_by_target(table, IdentifierCase::Folded)?)
        }
    }

    const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL);";

    /// An unqualified, unquoted name.
    fn named(table: &str) -> TargetName<'_> {
        TargetName::new(table, false)
    }

    fn column_names(shape: &TableShape) -> Vec<&str> {
        shape
            .columns()
            .iter()
            .map(crate::drift::ColumnShape::name)
            .collect()
    }

    #[test]
    fn an_inline_read_loads_only_the_named_table_and_shapes_it() {
        let mut reader = InlineLiveReader::new(FakeCatalog::new(DDL, []));
        let shape = block_on(reader.shape(named("orders"))).unwrap().unwrap();
        assert_eq!(column_names(&shape), ["id", "amount"]);
        assert!(!shape.columns()[1].nullable());
        assert_eq!(block_on(reader.shape(named("ledger"))).unwrap(), None);
        assert_eq!(reader.connection().loads, [["orders"], ["ledger"]]);
    }

    #[test]
    fn a_read_names_its_table_by_schema_and_name() {
        let mut reader = InlineLiveReader::new(FakeCatalog::new(
            "CREATE SCHEMA sales;
             CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL);
             CREATE TABLE sales.orders (id INT PRIMARY KEY, region TEXT);",
            [],
        ));
        let mut read = |schema| {
            block_on(reader.shape(named("orders").with_schema(schema, false)))
                .unwrap()
                .map(|shape| column_names(&shape).join(", "))
        };
        assert_eq!(read("sales").as_deref(), Some("id, region"));
        assert_eq!(read("public").as_deref(), Some("id, amount"));
        assert_eq!(read("archive"), None);
        assert_eq!(
            reader.connection().loads,
            [["sales.orders"], ["public.orders"], ["archive.orders"]]
        );
    }

    #[test]
    fn an_inline_read_completes_in_its_first_poll_and_answers_the_load_error() {
        let mut reader = InlineLiveReader::new(FakeCatalog::new(DDL, [Next::Fail]));
        let mut read = core::pin::pin!(reader.shape(named("orders")));
        assert!(
            matches!(
                read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(FakeError::Refused))
            ),
            "an inline read answers within its first poll, with no runtime"
        );
    }

    #[test]
    fn a_read_whose_name_matches_two_tables_answers_the_lookup_error() {
        let mut reader = InlineLiveReader::new(FakeCatalog::new(
            "CREATE TABLE orders (id INT PRIMARY KEY);
             CREATE TABLE \"Orders\" (id INT PRIMARY KEY);",
            [],
        ));
        let answer = block_on(reader.shape(named("orders")));
        assert!(
            matches!(
                answer,
                Err(FakeError::Lookup(LookupError::AmbiguousTableLookup { .. }))
            ),
            "{answer:?}"
        );
    }

    #[cfg(feature = "pg-streaming")]
    mod blocking {
        use super::*;
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use std::sync::mpsc::channel;

        fn runtime() -> tokio::runtime::Runtime {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .build()
                .unwrap()
        }

        #[test]
        fn a_blocking_read_answers_the_shape_absence_and_load_error() {
            let runtime = runtime();
            let mut reader = BlockingLiveReader::new(FakeCatalog::new(DDL, [Next::Fail]));
            assert!(matches!(
                runtime.block_on(reader.shape(named("orders"))),
                Err(BlockingReadError::Read(FakeError::Refused))
            ));
            let shape = runtime
                .block_on(reader.shape(named("orders").with_schema("public", false)))
                .unwrap()
                .unwrap();
            assert_eq!(column_names(&shape), ["id", "amount"]);
            assert_eq!(
                runtime
                    .block_on(reader.shape(named("orders").with_schema("sales", false)))
                    .unwrap(),
                None
            );
        }

        #[test]
        fn a_dropped_blocking_read_leaves_the_connection_for_the_next_read() {
            let runtime = runtime();
            let (release, held) = channel();
            let mut reader = BlockingLiveReader::new(FakeCatalog::new(DDL, [Next::WaitFor(held)]));
            runtime.block_on(async {
                let mut read = core::pin::pin!(reader.shape(named("orders")));
                assert!(read
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending());
            });
            release.send(()).unwrap();
            let shape = runtime.block_on(reader.shape(named("orders"))).unwrap();
            assert!(
                shape.is_some(),
                "the retried read finds the connection usable"
            );
        }

        #[test]
        fn a_panicking_load_resumes_on_the_reader_and_the_next_read_runs() {
            let runtime = runtime();
            let mut reader = BlockingLiveReader::new(FakeCatalog::new(DDL, [Next::Panic]));
            let panicked = catch_unwind(AssertUnwindSafe(|| {
                runtime.block_on(reader.shape(named("orders")))
            }));
            assert!(
                panicked.is_err(),
                "the load's panic reaches the awaiting task"
            );
            assert!(runtime
                .block_on(reader.shape(named("orders")))
                .unwrap()
                .is_some());
        }

        #[test]
        fn a_read_started_once_the_runtime_shuts_down_is_cancelled() {
            let runtime = runtime();
            let handle = runtime.handle().clone();
            runtime.shutdown_background();
            let _context = handle.enter();
            let mut reader = BlockingLiveReader::new(FakeCatalog::new(DDL, []));
            let answer = block_on(reader.shape(named("orders")));
            assert!(
                matches!(answer, Err(BlockingReadError::Cancelled)),
                "{answer:?}"
            );
        }
    }
}
