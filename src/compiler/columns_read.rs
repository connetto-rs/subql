//! Which columns of which tables a statement references.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::marker::PhantomData;
use core::ops::ControlFlow;

use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::TargetName;
use sqlparser::ast::{
    AccessExpr, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, JoinConstraint,
    JoinOperator, ObjectName, ObjectNamePart, OrderByKind, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, Visit, Visitor,
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
}

/// The tables one query level reads and the names its output carries.
struct Scope {
    bindings: Vec<Binding>,
    /// Output column names, which a bare name in `ORDER BY` or `GROUP BY` may
    /// name instead of an input column.
    outputs: Vec<Ident>,
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

    /// Charge every column of `table`, as a whole-row reference reads them all.
    fn charge_row(&mut self, table: TableId) -> ControlFlow<()> {
        let Ok(arity) = catalog_helpers::table_arity(self.database, table) else {
            return ControlFlow::Break(());
        };
        for ordinal in 0..arity {
            let Ok(column) = ColumnId::try_from(ordinal) else {
                return ControlFlow::Break(());
            };
            self.charge(table, column);
        }
        ControlFlow::Continue(())
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
        let Some(table) = written_table_name(name)
            .and_then(|target| catalog_helpers::table_id_for_name::<B, DB>(self.database, target))
        else {
            return ControlFlow::Break(());
        };
        let (name, aliased) = match alias {
            Some(alias) => (alias.name.clone(), true),
            None => match name.0.last().and_then(ObjectNamePart::as_ident) {
                Some(ident) => (ident.clone(), false),
                None => return ControlFlow::Break(()),
            },
        };
        self.read.entry(table).or_default();
        scope.bindings.push(Binding {
            name,
            aliased,
            table,
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
                            let [part] = column.0.as_slice() else {
                                return ControlFlow::Break(());
                            };
                            let Some(name) = part.as_ident() else {
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
                        self.charge_row(binding.table)?;
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    let table = qualifier::<B>(&scope.bindings, name)?;
                    self.charge_row(table)?;
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
    /// else the same at each enclosing level, else an output name of the
    /// innermost level.
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
                return self.charges.charge_row(binding.table);
            }
        }
        let output = self.scopes.last().is_some_and(|scope| {
            scope
                .outputs
                .iter()
                .any(|output| same_column_name::<B>(output, name))
        });
        if output {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
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
        let table = self
            .scopes
            .iter()
            .rev()
            .find_map(|scope| qualifier::<B>(&scope.bindings, name).continue_value());
        match table {
            Some(table) => self.charges.charge_row(table),
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
                        if scope
                            .outputs
                            .iter()
                            .any(|output| same_column_name::<B>(output, name))
                        {
                            self.not_columns.push(core::ptr::from_ref(&order.expr));
                        }
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
            Expr::Wildcard(_) => {
                if let Some(scope) = self.scopes.last() {
                    for binding in &scope.bindings {
                        self.charges.charge_row(binding.table)?;
                    }
                }
                ControlFlow::Continue(())
            }
            Expr::Function(function) => {
                // `count(docs.*)` reads the row. A bare `*` argument reads none.
                if let FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
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

/// The table a `name.*` or `name.column` names among `bindings`.
fn qualifier<B: Backend>(bindings: &[Binding], name: &ObjectName) -> ControlFlow<(), TableId> {
    let [part] = name.0.as_slice() else {
        return ControlFlow::Break(());
    };
    part.as_ident()
        .and_then(|ident| bindings.iter().find(|binding| names::<B>(binding, ident)))
        .map_or(ControlFlow::Break(()), |binding| {
            ControlFlow::Continue(binding.table)
        })
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
