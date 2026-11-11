use rls2fga_types::ActionStatement;
use sql_traits::prelude::DatabaseLike;

use crate::visibility::shapes::SharedShapes;
use crate::TableId;

/// Answers from the row where the schema decides it, delegates the rest.
///
/// Build it from [`rls2fga::translator::Translation::relations`] and the
/// catalog those relations were planned against.
///
/// # Examples
///
/// ```
/// # extern crate alloc;
/// # use core::convert::Infallible;
/// # use core::future::Future;
/// use rls2fga_types::ConfidenceLevel;
/// # use rls2fga::translator::TranslatorBuilder;
/// use sqlparser::{ast::DataType, dialect::PostgreSqlDialect};
/// use sql_traits::structs::{Deployment, SessionSetting, SettingValue};
/// use subql::backend::{Postgres, Value};
/// use subql::testing::TestEvent;
/// use alloc::sync::Arc;
/// use rls2fga_types::ActionStatement;
/// use subql::visibility::policy::{RequestValues, RowPolicy, Subject};
/// use subql::visibility::shapes::Shapes;
/// use subql::visibility::store::Enumeration;
/// use subql::visibility::{EventRow, RowView, RowWrite, Verdict, VisibilityPolicy};
/// use subql::{catalog_helpers, ParserDB};
/// use alloc::borrow::Cow;
///
/// #[derive(Clone)]
/// struct Watcher {
///     subject: &'static str,
///     caller: &'static str,
/// }
///
/// impl Subject for Watcher {
///     fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
///         core::iter::once(Cow::Borrowed(self.subject))
///     }
///
///     fn acting_role(&self) -> Option<&str> {
///         Some("app_user")
///     }
///
///     fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
///         if parameter != "app_user_id" {
///             return false;
///         }
///         out.push(self.caller);
///         true
///     }
/// }
///
/// // The delegate answers whatever the row cannot. This one is never asked.
/// struct Backend;
/// impl VisibilityPolicy for Backend {
///     type Watcher = Watcher;
///     type Error = Infallible;
///     type Backend = Postgres;
///     fn may_see<R>(
///         &self,
///         _row: &R,
///         _watchers: &[Watcher],
///         _verdicts: &mut [Verdict],
///     ) -> impl Future<Output = Result<(), Infallible>> + Send
///     where
///         R: RowView<Backend = Postgres> + Sync + ?Sized,
///     {
///         async { Ok(()) }
///     }
///     fn may_write<R>(
///         &self,
///         _write: RowWrite<'_, R>,
///         _watcher: &Watcher,
///     ) -> impl Future<Output = Result<Verdict, Infallible>> + Send
///     where
///         R: RowView<Backend = Postgres> + Sync + ?Sized,
///     {
///         async { Ok(Verdict::Deny) }
///     }
/// }
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE ROLE app_user LOGIN;
///      CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);
///      GRANT SELECT ON docs TO app_user;
///      ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
///      CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_setting('app.user_id', true));",
/// )?;
/// let docs = catalog_helpers::table_id::<subql::backend::Postgres, _>(&db, "docs").expect("docs is in the catalog");
///
/// let deployment = Deployment::new(
///     "app_user",
///     ["app_user"],
///     [SessionSetting::value("app.user_id", SettingValue::Caller(DataType::Text))?],
/// )?;
/// let translator = TranslatorBuilder::new(deployment)
///     .with_min_confidence(ConfidenceLevel::B)
///     .build();
/// let outputs = translator.translate(&db)?.outputs_accepting_gaps();
/// let translation = outputs.translation();
/// let enumerations: Vec<Enumeration<'_>> = outputs
///     .tuple_queries()
///     .iter()
///     .filter_map(Enumeration::of)
///     .collect();
/// let naming = std::borrow::Cow::from(translation.row_naming()).into_owned();
/// let answers = translation.action_relations().to_vec();
/// let shapes = Arc::new(
///     Shapes::new::<Postgres>(db, translation.relations(), &enumerations)
///         .with_row_naming(&naming)
///         .with_action_relations(&answers),
/// );
/// let policy = RowPolicy::new(shapes, Backend);
/// assert!(policy.answers_locally(docs, ActionStatement::Select));
///
/// let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::String("alice".into())])
///     .with_pk_columns([0u16]);
/// let row = EventRow::current(&event, policy.catalog()).expect("an insert carries a post-image");
///
/// let watchers = [
///     Watcher { subject: "user:alice", caller: "alice" },
///     Watcher { subject: "user:bob", caller: "bob" },
/// ];
/// let mut verdicts = Vec::new();
/// Verdict::reset(&mut verdicts, watchers.len());
///
/// let runtime = tokio::runtime::Builder::new_current_thread().build()?;
/// runtime.block_on(policy.may_see(&row, &watchers, &mut verdicts))?;
/// assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct RowPolicy<DB, P> {
    shapes: SharedShapes<DB>,
    inner: P,
}

impl<DB: DatabaseLike, P> RowPolicy<DB, P> {
    /// Answer from `shapes`, delegating whatever it does not settle to `inner`.
    ///
    /// The index is shared rather than built here, so this and the policy behind
    /// it read one catalog and one set of descriptions. Two of them built apart
    /// could disagree, and every record would then name rows that do not exist.
    #[must_use]
    pub const fn new(shapes: SharedShapes<DB>, inner: P) -> Self {
        Self { shapes, inner }
    }

    /// The index, so a caller shares it with the policy behind this one.
    #[must_use]
    pub const fn shapes(&self) -> &SharedShapes<DB> {
        &self.shapes
    }

    /// The catalog, so the caller builds its [`EventRow`](crate::visibility::EventRow)
    /// views against the same one the recipes were indexed with.
    pub fn catalog(&self) -> &DB {
        self.shapes.catalog()
    }

    /// The delegate.
    pub const fn inner(&self) -> &P {
        &self.inner
    }

    /// Whether a question about `action` on a row of `table` can be answered
    /// without a round trip.
    ///
    /// Reports the recipe, not the row: a row whose cell fails to decode is
    /// still delegated, and so is a watcher that cannot supply a value the
    /// recipe compares against.
    #[must_use]
    pub fn answers_locally(&self, table: TableId, statement: ActionStatement) -> bool {
        self.shapes.answers_locally(table, statement)
    }
}
