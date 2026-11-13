//! Which columns of which tables a statement references.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::marker::PhantomData;
use core::ops::ControlFlow;

use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::TargetName;
use sqlparser::ast::{
    AccessExpr, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderByKind, Query, Select,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, Visit, Visitor,
};

use super::sql_shape::{written_column, written_table_name};
use crate::backend::Backend;
use crate::{catalog_helpers, ColumnId, TableId};

/// The columns of each table a statement references, which is the set
/// PostgreSQL holds a caller's column privileges against.
///
/// Every column the statement names counts, wherever it names it: the
/// projection, `WHERE`, join conditions, `GROUP BY`, `HAVING`, `ORDER BY`,
/// aggregate arguments and subqueries, each under the table it resolves to. A
/// whole-row reference (`*`, `docs.*`, a bare `docs`) names every column of
/// its table.
///
/// Taken from the statement as written, before any read rule is folded into
/// it, because PostgreSQL never asks the caller for a column only a policy
/// reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnsRead {
    /// Sorted by table, each column list sorted and free of duplicates.
    tables: Box<[(TableId, Box<[ColumnId]>)]>,
}

impl ColumnsRead {
    /// The columns of `table` the statement references, in ascending order.
    ///
    /// [`None`] means one of two things, and a caller treats both as a read of
    /// every column: the statement does not read `table`, or it makes a
    /// reference subql cannot resolve to a column (a CTE, a set operation, a
    /// derived table, a column alias list, `NATURAL` join, an unknown name), so
    /// no table it reads reports a set.
    ///
    /// An empty slice is a read of `table` that names none of its columns, as
    /// `SELECT count(*) FROM docs` does.
    #[must_use]
    pub fn get(&self, table: TableId) -> Option<&[ColumnId]> {
        self.tables
            .binary_search_by_key(&table, |(id, _)| *id)
            .ok()
            .map(|index| &*self.tables[index].1)
    }

    /// The columns `statement` references, per table, resolved against
    /// `database`.
    pub(crate) fn of<B: Backend, DB: DatabaseLike>(statement: &Statement, database: &DB) -> Self {
        let Statement::Query(query) = statement else {
            return Self::default();
        };
        let mut references = References::<B, DB> {
            charges: Charges {
                database,
                read: BTreeMap::new(),
                backend: PhantomData,
            },
            scopes: Vec::new(),
            not_columns: Vec::new(),
        };
        if query.visit(&mut references).is_break() {
            return Self::default();
        }
        Self {
            tables: references
                .charges
                .read
                .into_iter()
                .map(|(table, mut columns)| {
                    columns.sort_unstable();
                    columns.dedup();
                    (table, columns.into_boxed_slice())
                })
                .collect(),
        }
    }
}

/// One table a `FROM` clause names, under the name a column reference
/// qualifies it by.
struct Binding {
    /// The alias, else the table's own unqualified name.
    name: Ident,
    /// Whether `name` is an alias, which hides the table's own name.
    aliased: bool,
    table: TableId,
    /// How many columns the table has, which a whole-row reference reads.
    arity: ColumnId,
}

/// The tables one query level reads and the names its output carries.
struct Scope {
    bindings: Vec<Binding>,
    /// Output column names, which a bare name in `ORDER BY` or `GROUP BY` may
    /// name instead of an input column.
    outputs: Vec<Ident>,
}

impl Scope {
    /// Whether an output column of this level is called `name`.
    fn outputs_name<B: Backend>(&self, name: &Ident) -> bool {
        self.outputs
            .iter()
            .any(|output| same_column_name::<B>(output, name))
    }
}

/// Collects every column reference of a query, one [`Scope`] per query level
/// so a correlated reference resolves to the level that reads its table.
///
/// Breaks on the first reference it cannot resolve, which discards the
/// whole answer rather than reporting a partial set.
struct References<'d, B, DB> {
    charges: Charges<'d, B, DB>,
    scopes: Vec<Scope>,
    /// Identifiers that name something other than a column: an `ORDER BY`
    /// output name and a field after a dot. Compared by address only.
    not_columns: Vec<*const Expr>,
}

/// The columns charged so far, per table.
struct Charges<'d, B, DB> {
    database: &'d DB,
    read: BTreeMap<TableId, Vec<ColumnId>>,
    backend: PhantomData<B>,
}

impl<B: Backend, DB: DatabaseLike> Charges<'_, B, DB> {
    fn charge(&mut self, table: TableId, column: ColumnId) {
        self.read.entry(table).or_default().push(column);
    }

    /// Charge `column` to every table among `bindings` that has it, answering
    /// whether one did.
    fn charge_every(&mut self, bindings: &[Binding], column: &Ident) -> bool {
        let mut found = false;
        for binding in bindings {
            if let Some(id) = written_column::<B, DB>(self.database, binding.table, column) {
                self.charge(binding.table, id);
                found = true;
            }
        }
        found
    }

    /// Charge every column of `binding`, as a whole-row reference reads them all.
    fn charge_row(&mut self, binding: &Binding) {
        self.read
            .entry(binding.table)
            .or_default()
            .extend(0..binding.arity);
    }

    /// Bind the table `factor` names into `scope`.
    fn bind(&mut self, scope: &mut Scope, factor: &TableFactor) -> ControlFlow<()> {
        let TableFactor::Table {
            name, alias, args, ..
        } = factor
        else {
            return ControlFlow::Break(());
        };
        // A table function, or columns renamed by the alias, would make a
        // written name mean something other than the catalog's column.
        if args.is_some()
            || alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
        {
            return ControlFlow::Break(());
        }
        let database = self.database;
        let found = written_table_name(name)
            .and_then(|target| catalog_helpers::table_id_for_name::<B, DB>(database, target))
            .and_then(|table| {
                let arity = catalog_helpers::table_arity(database, table).ok()?;
                Some((table, ColumnId::try_from(arity).ok()?))
            });
        let own = name.0.last().and_then(ObjectNamePart::as_ident);
        let (Some((table, arity)), Some(own)) = (found, own) else {
            return ControlFlow::Break(());
        };
        let (name, aliased) = alias
            .as_ref()
            .map_or_else(|| (own.clone(), false), |alias| (alias.name.clone(), true));
        self.read.entry(table).or_default();
        scope.bindings.push(Binding {
            name,
            aliased,
            table,
            arity,
        });
        ControlFlow::Continue(())
    }

    /// The scope `select` opens, charging what its `FROM` clause and
    /// projection read without naming an expression.
    fn open(&mut self, select: &Select) -> ControlFlow<(), Scope> {
        let mut scope = Scope {
            bindings: Vec::new(),
            outputs: Vec::new(),
        };
        for from in &select.from {
            // A `USING` column comes from the join tree it is written in, never
            // from an earlier comma-separated `FROM` item.
            let tree = scope.bindings.len();
            self.bind(&mut scope, &from.relation)?;
            for join in &from.joins {
                self.bind(&mut scope, &join.relation)?;
                let constraint = match &join.join_operator {
                    JoinOperator::Join(constraint)
                    | JoinOperator::Inner(constraint)
                    | JoinOperator::Left(constraint)
                    | JoinOperator::LeftOuter(constraint)
                    | JoinOperator::Right(constraint)
                    | JoinOperator::RightOuter(constraint)
                    | JoinOperator::FullOuter(constraint)
                    | JoinOperator::CrossJoin(constraint)
                    | JoinOperator::Semi(constraint)
                    | JoinOperator::LeftSemi(constraint)
                    | JoinOperator::RightSemi(constraint)
                    | JoinOperator::Anti(constraint)
                    | JoinOperator::LeftAnti(constraint)
                    | JoinOperator::RightAnti(constraint)
                    | JoinOperator::StraightJoin(constraint)
                    | JoinOperator::AsOf { constraint, .. } => constraint,
                    JoinOperator::CrossApply
                    | JoinOperator::OuterApply
                    | JoinOperator::ArrayJoin
                    | JoinOperator::LeftArrayJoin
                    | JoinOperator::InnerArrayJoin => return ControlFlow::Break(()),
                };
                match constraint {
                    // The expression is visited with the rest of the select.
                    JoinConstraint::On(_) | JoinConstraint::None => {}
                    // Each side reads the column, so every table of this join
                    // tree bound so far that has it is charged with it.
                    JoinConstraint::Using(columns) => {
                        for column in columns {
                            let Some(name) = single_ident(column) else {
                                return ControlFlow::Break(());
                            };
                            self.charge_every(&scope.bindings[tree..], name);
                        }
                    }
                    JoinConstraint::UsingAlias(..) | JoinConstraint::Natural => {
                        return ControlFlow::Break(());
                    }
                }
            }
        }
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for binding in &scope.bindings {
                        self.charge_row(binding);
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    let Some(binding) = qualifier::<B>(&scope.bindings, name) else {
                        return ControlFlow::Break(());
                    };
                    self.charge_row(binding);
                }
                // The expression is visited with the rest of the select.
                SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(_), _) => {}
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(name) = output_name(expr) {
                        scope.outputs.push(name.clone());
                    }
                }
                SelectItem::ExprWithAlias { alias, .. } => scope.outputs.push(alias.clone()),
                SelectItem::ExprWithAliases { aliases, .. } => {
                    scope.outputs.extend(aliases.iter().cloned());
                }
            }
        }
        ControlFlow::Continue(scope)
    }
}

impl<B: Backend, DB: DatabaseLike> References<'_, B, DB> {
    /// Resolve an unqualified name: a column of the innermost level that reads
    /// one by that name, else a whole-row reference to a table bound there,
    /// else the same at each enclosing level.
    fn bare(&mut self, name: &Ident) -> ControlFlow<()> {
        for scope in self.scopes.iter().rev() {
            if self.charges.charge_every(&scope.bindings, name) {
                return ControlFlow::Continue(());
            }
            if let Some(binding) = scope
                .bindings
                .iter()
                .find(|binding| names::<B>(binding, name))
            {
                self.charges.charge_row(binding);
                return ControlFlow::Continue(());
            }
        }
        ControlFlow::Break(())
    }

    /// Resolve `table.column` against the innermost level binding `table`, or
    /// `schema.table.column` against an unaliased binding of that table.
    fn qualified(&mut self, parts: &[Ident]) -> ControlFlow<()> {
        let (table, column) = match parts {
            [qualifier, column] => {
                let Some(table) = self.scopes.iter().rev().find_map(|scope| {
                    scope
                        .bindings
                        .iter()
                        .find(|binding| names::<B>(binding, qualifier))
                        .map(|binding| binding.table)
                }) else {
                    return ControlFlow::Break(());
                };
                (table, column)
            }
            [schema, name, column] => {
                let target = TargetName::new(&name.value, name.quote_style.is_some())
                    .with_schema(&schema.value, schema.quote_style.is_some());
                let Some(table) =
                    catalog_helpers::table_id_for_name::<B, DB>(self.charges.database, target)
                else {
                    return ControlFlow::Break(());
                };
                let bound = self.scopes.iter().any(|scope| {
                    scope
                        .bindings
                        .iter()
                        .any(|binding| !binding.aliased && binding.table == table)
                });
                if !bound {
                    return ControlFlow::Break(());
                }
                (table, column)
            }
            _ => return ControlFlow::Break(()),
        };
        match written_column::<B, DB>(self.charges.database, table, column) {
            Some(id) => {
                self.charges.charge(table, id);
                ControlFlow::Continue(())
            }
            None => ControlFlow::Break(()),
        }
    }

    /// Charge the whole row of the table `name` qualifies at any level.
    fn whole_row(&mut self, name: &ObjectName) -> ControlFlow<()> {
        let binding = self
            .scopes
            .iter()
            .rev()
            .find_map(|scope| qualifier::<B>(&scope.bindings, name));
        match binding {
            Some(binding) => {
                self.charges.charge_row(binding);
                ControlFlow::Continue(())
            }
            None => ControlFlow::Break(()),
        }
    }
}

impl<B: Backend, DB: DatabaseLike> Visitor for References<'_, B, DB> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if query.with.is_some() {
            return ControlFlow::Break(());
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return ControlFlow::Break(());
        };
        let scope = self.charges.open(select)?;
        // `ORDER BY name` names the output column before an input column.
        if let Some(order_by) = &query.order_by {
            if let OrderByKind::Expressions(exprs) = &order_by.kind {
                for order in exprs {
                    if let Expr::Identifier(name) = &order.expr {
                        if scope.outputs_name::<B>(name) {
                            self.not_columns.push(core::ptr::from_ref(&order.expr));
                        }
                    }
                }
            }
        }
        // `GROUP BY name` names an input column before the output column.
        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                if let Expr::Identifier(name) = expr {
                    let input = scope.bindings.iter().any(|binding| {
                        written_column::<B, DB>(self.charges.database, binding.table, name)
                            .is_some()
                    });
                    if !input && scope.outputs_name::<B>(name) {
                        self.not_columns.push(core::ptr::from_ref(expr));
                    }
                }
            }
        }
        self.scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.not_columns.contains(&core::ptr::from_ref(expr)) {
            return ControlFlow::Continue(());
        }
        match expr {
            Expr::Identifier(name) => self.bare(name),
            Expr::CompoundIdentifier(parts) => self.qualified(parts),
            Expr::QualifiedWildcard(name, _) => self.whole_row(name),
            Expr::Function(function) => {
                // `count(docs.*)` reads the row. A bare `*` argument reads none.
                if let FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
                        // The name of a named argument is the parameter's.
                        if let FunctionArg::ExprNamed { name, .. } = arg {
                            self.not_columns.push(core::ptr::from_ref(name));
                        }
                        let (FunctionArg::Unnamed(arg)
                        | FunctionArg::Named { arg, .. }
                        | FunctionArg::ExprNamed { arg, .. }) = arg;
                        if let FunctionArgExpr::QualifiedWildcard(name) = arg {
                            self.whole_row(name)?;
                        }
                    }
                }
                ControlFlow::Continue(())
            }
            Expr::CompoundFieldAccess { access_chain, .. } => {
                for access in access_chain {
                    if let AccessExpr::Dot(field @ Expr::Identifier(_)) = access {
                        self.not_columns.push(core::ptr::from_ref(field));
                    }
                }
                ControlFlow::Continue(())
            }
            // A lambda names parameters and a full-text match names columns
            // outside any expression, neither of which this resolves.
            Expr::Lambda(_) | Expr::MatchAgainst { .. } => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

/// The binding a `name.*` names among `bindings`.
fn qualifier<'b, B: Backend>(bindings: &'b [Binding], name: &ObjectName) -> Option<&'b Binding> {
    let ident = single_ident(name)?;
    bindings.iter().find(|binding| names::<B>(binding, ident))
}

/// The one identifier `name` consists of, or [`None`] when it has more parts.
fn single_ident(name: &ObjectName) -> Option<&Ident> {
    match name.0.as_slice() {
        [part] => part.as_ident(),
        _ => None,
    }
}

/// The output name a projected expression carries without an alias, when it
/// is a column reference.
fn output_name(expr: &Expr) -> Option<&Ident> {
    match expr {
        Expr::Identifier(name) => Some(name),
        Expr::CompoundIdentifier(parts) => parts.last(),
        _ => None,
    }
}

/// Whether `written` qualifies a column by `binding`'s name.
fn names<B: Backend>(binding: &Binding, written: &Ident) -> bool {
    B::WRITTEN_TABLE_NAME_CASE.identifiers_match(
        &binding.name.value,
        binding.name.quote_style.is_some(),
        &written.value,
        written.quote_style.is_some(),
    )
}

/// Whether two written column names name one column.
fn same_column_name<B: Backend>(left: &Ident, right: &Ident) -> bool {
    B::COLUMN_NAME_CASE.identifiers_match(
        &left.value,
        left.quote_style.is_some(),
        &right.value,
        right.quote_style.is_some(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::ColumnsRead;
    use crate::backend::Postgres;
    use crate::{catalog_helpers, ColumnId};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::{
        ClickHouseDialect, Dialect, DuckDbDialect, GenericDialect, MsSqlDialect, MySqlDialect,
        PostgreSqlDialect, SnowflakeDialect,
    };

    const DDL: &str = "CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT, owner TEXT);
        CREATE TABLE members (doc_id INT, user_id TEXT);";

    const EVERY_DOC: &[ColumnId] = &[0, 1, 2, 3];
    const EVERY_MEMBER: &[ColumnId] = &[0, 1];

    /// The columns of `docs` and of `members` that `sql`, parsed by `dialect`,
    /// references.
    fn read(
        dialect: &dyn Dialect,
        sql: &str,
    ) -> (
        Option<alloc::vec::Vec<ColumnId>>,
        Option<alloc::vec::Vec<ColumnId>>,
    ) {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let docs = catalog_helpers::table_id::<Postgres, _>(&db, "docs").unwrap();
        let members = catalog_helpers::table_id::<Postgres, _>(&db, "members").unwrap();
        let statements = sqlparser::parser::Parser::parse_sql(dialect, sql).unwrap();
        let read = ColumnsRead::of::<Postgres, _>(&statements[0], &db);
        (
            read.get(docs).map(<[ColumnId]>::to_vec),
            read.get(members).map(<[ColumnId]>::to_vec),
        )
    }

    fn pg(
        sql: &str,
    ) -> (
        Option<alloc::vec::Vec<ColumnId>>,
        Option<alloc::vec::Vec<ColumnId>>,
    ) {
        read(&PostgreSqlDialect {}, sql)
    }

    #[test]
    fn a_whole_row_reference_reads_every_column_of_its_table() {
        for sql in [
            "SELECT docs.* FROM docs",
            "SELECT id FROM docs WHERE docs IS NOT NULL",
            "SELECT id FROM docs WHERE (docs.*) IS NOT NULL",
            "SELECT count(docs.*) FROM docs",
            "SELECT jsonb_build_object(a => docs.*) FROM docs",
        ] {
            assert_eq!(pg(sql), (Some(EVERY_DOC.to_vec()), None), "{sql}");
        }
        assert_eq!(
            read(&GenericDialect {}, "SELECT f(a => docs.*) FROM docs"),
            (Some(EVERY_DOC.to_vec()), None),
            "a named argument in a dialect naming it by identifier"
        );
        assert_eq!(
            read(&SnowflakeDialect {}, "SELECT (docs).* FROM docs"),
            (Some(EVERY_DOC.to_vec()), None),
            "the star expanding an expression reads what the expression reads"
        );
        assert_eq!(
            pg("SELECT * FROM docs, members"),
            (Some(EVERY_DOC.to_vec()), Some(EVERY_MEMBER.to_vec()))
        );
        assert_eq!(
            pg("SELECT d.* FROM docs d JOIN members m ON m.doc_id = d.id"),
            (Some(EVERY_DOC.to_vec()), Some(alloc::vec![0]))
        );
    }

    #[test]
    fn every_join_kind_charges_its_condition() {
        let on = "ON m.doc_id = d.id";
        for (dialect, join) in [
            (&PostgreSqlDialect {} as &dyn Dialect, "JOIN"),
            (&PostgreSqlDialect {}, "INNER JOIN"),
            (&PostgreSqlDialect {}, "LEFT JOIN"),
            (&PostgreSqlDialect {}, "LEFT OUTER JOIN"),
            (&PostgreSqlDialect {}, "RIGHT JOIN"),
            (&PostgreSqlDialect {}, "RIGHT OUTER JOIN"),
            (&PostgreSqlDialect {}, "FULL OUTER JOIN"),
            (&GenericDialect {}, "SEMI JOIN"),
            (&GenericDialect {}, "LEFT SEMI JOIN"),
            (&GenericDialect {}, "RIGHT SEMI JOIN"),
            (&GenericDialect {}, "ANTI JOIN"),
            (&GenericDialect {}, "LEFT ANTI JOIN"),
            (&GenericDialect {}, "RIGHT ANTI JOIN"),
            (&MySqlDialect {}, "STRAIGHT_JOIN"),
        ] {
            let sql = alloc::format!("SELECT d.title FROM docs d {join} members m {on}");
            assert_eq!(
                read(dialect, &sql),
                (Some(alloc::vec![0, 1]), Some(alloc::vec![0])),
                "{sql}"
            );
        }
        assert_eq!(
            pg("SELECT d.title FROM docs d CROSS JOIN members m WHERE m.doc_id = d.id"),
            (Some(alloc::vec![0, 1]), Some(alloc::vec![0]))
        );
        assert_eq!(
            read(
                &SnowflakeDialect {},
                "SELECT d.title FROM docs d ASOF JOIN members m \
                 MATCH_CONDITION (d.id >= m.doc_id) ON m.user_id = d.owner"
            ),
            (Some(alloc::vec![0, 1, 3]), Some(alloc::vec![0, 1]))
        );
    }

    #[test]
    fn a_schema_qualified_column_names_an_unaliased_table() {
        assert_eq!(
            pg("SELECT public.docs.title FROM docs"),
            (Some(alloc::vec![1]), None)
        );
        assert_eq!(pg("SELECT public.docs.title FROM docs d"), (None, None));
        assert_eq!(pg("SELECT nowhere.docs.title FROM docs"), (None, None));
    }

    #[test]
    fn a_field_after_a_dot_is_not_a_column() {
        assert_eq!(
            pg("SELECT id FROM docs WHERE (title).owner = 'x'"),
            (Some(alloc::vec![0, 1]), None)
        );
    }

    #[test]
    fn a_multi_column_alias_names_outputs_order_by_may_use() {
        assert_eq!(
            read(
                &GenericDialect {},
                "SELECT title AS (body, owner) FROM docs ORDER BY body"
            ),
            (Some(alloc::vec![1]), None)
        );
    }

    #[test]
    fn an_output_name_stands_in_for_a_column_only_where_postgresql_reads_it_so() {
        assert_eq!(
            pg("SELECT title AS body FROM docs ORDER BY body"),
            (Some(alloc::vec![1]), None),
            "ORDER BY reads the output column"
        );
        assert_eq!(
            pg("SELECT count(*) AS body FROM docs GROUP BY body"),
            (Some(alloc::vec![2]), None),
            "GROUP BY reads the input column of that name"
        );
        assert_eq!(
            pg("SELECT upper(title) AS heading FROM docs GROUP BY heading"),
            (Some(alloc::vec![1]), None),
            "GROUP BY falls back to the output column"
        );
        assert_eq!(
            pg("SELECT title AS heading FROM docs WHERE heading = 'x'"),
            (None, None),
            "WHERE never reads an output column"
        );
    }

    #[test]
    fn an_expression_in_any_clause_charges_its_columns() {
        for (dialect, sql, docs) in [
            (
                &PostgreSqlDialect {} as &dyn Dialect,
                "SELECT id FROM docs ORDER BY lower(title)",
                alloc::vec![0, 1],
            ),
            (
                &DuckDbDialect {},
                "SELECT id FROM docs ORDER BY ALL",
                alloc::vec![0],
            ),
            (
                &PostgreSqlDialect {},
                "SELECT count(*) FROM docs GROUP BY lower(title)",
                alloc::vec![1],
            ),
            (
                &GenericDialect {},
                "SELECT title, count(*) FROM docs GROUP BY ALL",
                alloc::vec![1],
            ),
            (
                &PostgreSqlDialect {},
                "SELECT current_timestamp, id FROM docs",
                alloc::vec![0],
            ),
            (
                &PostgreSqlDialect {},
                "SELECT id FROM docs WHERE (title)[1] = 'x'",
                alloc::vec![0, 1],
            ),
        ] {
            assert_eq!(read(dialect, sql), (Some(docs), None), "{sql}");
        }
    }

    #[test]
    fn a_statement_reading_no_table_reports_no_table() {
        assert_eq!(pg("INSERT INTO docs (id) VALUES (1)"), (None, None));
    }

    /// Each reports no set for any table, however much of it resolved, so a
    /// consumer checks it as `SELECT *` rather than as a partial answer.
    #[test]
    fn an_unresolvable_reference_reports_no_table() {
        for (dialect, sql) in [
            (
                &PostgreSqlDialect {} as &dyn Dialect,
                "SELECT x FROM (SELECT id AS x FROM docs) t",
            ),
            (
                &PostgreSqlDialect {},
                "WITH x AS (SELECT 1) SELECT id FROM docs",
            ),
            (
                &PostgreSqlDialect {},
                "SELECT id FROM docs UNION SELECT id FROM docs",
            ),
            (
                &PostgreSqlDialect {},
                "SELECT * FROM docs, generate_series(1, 3)",
            ),
            (
                &PostgreSqlDialect {},
                "SELECT a FROM docs AS d (a, b, c, e)",
            ),
            (&PostgreSqlDialect {}, "SELECT id FROM docs, nowhere"),
            (&PostgreSqlDialect {}, "SELECT id FROM docs, a.b.c.nowhere"),
            (
                &PostgreSqlDialect {},
                "SELECT id FROM docs NATURAL JOIN members",
            ),
            (
                &PostgreSqlDialect {},
                "SELECT id FROM docs JOIN (SELECT 1) x ON true",
            ),
            (
                &PostgreSqlDialect {},
                "SELECT id FROM docs d JOIN members m USING (m.doc_id)",
            ),
            (&PostgreSqlDialect {}, "SELECT nope FROM docs"),
            (&PostgreSqlDialect {}, "SELECT docs.nope FROM docs"),
            (&PostgreSqlDialect {}, "SELECT x.id FROM docs"),
            (&PostgreSqlDialect {}, "SELECT x.* FROM docs"),
            (&PostgreSqlDialect {}, "SELECT count(x.*) FROM docs"),
            (&PostgreSqlDialect {}, "SELECT a.b.c.d FROM docs"),
            (
                &MsSqlDialect {},
                "SELECT d.id FROM docs d CROSS APPLY members m",
            ),
            (
                &ClickHouseDialect {},
                "SELECT id FROM docs ARRAY JOIN members",
            ),
            (
                &DuckDbDialect {},
                "SELECT list_transform([1], x -> x + 1) FROM docs",
            ),
            (
                &MySqlDialect {},
                "SELECT id FROM docs WHERE MATCH (title) AGAINST ('x')",
            ),
        ] {
            assert_eq!(read(dialect, sql), (None, None), "{sql}");
        }
    }
}
