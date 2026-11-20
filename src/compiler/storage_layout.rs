//! Where a statement's outputs sit in its select list and the type each is
//! stored as in the replica.

use alloc::vec::Vec;

use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::TargetName;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint,
    Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement,
    WildcardAdditionalOptions,
};

use super::columns_read::{
    binding, join_constraint, names, output_name, qualifier, same_column_name, Binding,
};
use super::sql_shape::written_column;
use crate::backend::{Backend, DeclaredType, MeanRule, SumRule};
use crate::emit::{column_storage_type, StorageLayout, StorageType};
use crate::{catalog_helpers, ColumnId, TableId};

/// One output column of the select list.
struct Output<'s> {
    storage_type: Option<StorageType>,
    /// The column it reads, when it is a column reference.
    column: Option<(TableId, ColumnId)>,
    /// The name `GROUP BY` may refer to it by.
    name: Option<&'s Ident>,
    /// The expression, for an item that is not a wildcard.
    expr: Option<&'s Expr>,
}

/// The [`StorageLayout`] of `statement` against `database`.
///
/// `delivers_aggregates` is whether the registration delivers aggregate
/// values, which is what places `GROUP BY` values and types the aggregate.
/// `None` when a select-list column cannot be resolved: a CTE, a set
/// operation, a derived table or table function, an alias column list, or
/// a wildcard over a join that merges columns.
pub fn storage_layout<B: Backend, DB: DatabaseLike>(
    statement: &Statement,
    database: &DB,
    delivers_aggregates: bool,
) -> Option<StorageLayout> {
    let Statement::Query(query) = statement else {
        return None;
    };
    if query.with.is_some() {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let (bindings, merges_columns) = bind::<B, DB>(select, database)?;
    let resolver = Resolver::<B, DB> {
        database,
        bindings: &bindings,
        backend: core::marker::PhantomData,
    };

    let mut outputs: Vec<Output<'_>> = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options) => {
                if merges_columns || !plain(options) {
                    return None;
                }
                for binding in &bindings {
                    expand::<B, DB>(&mut outputs, database, binding)?;
                }
            }
            SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(name),
                options,
            ) => {
                if !plain(options) {
                    return None;
                }
                expand::<B, DB>(&mut outputs, database, qualifier::<B>(&bindings, name)?)?;
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(_), _)
            | SelectItem::ExprWithAliases { .. } => return None,
            SelectItem::UnnamedExpr(expr) => outputs.push(resolver.output(expr, output_name(expr))),
            SelectItem::ExprWithAlias { expr, alias } => {
                outputs.push(resolver.output(expr, Some(alias)));
            }
        }
    }

    let mut groups = Vec::new();
    let mut aggregate = None;
    if delivers_aggregates {
        let mut aggregates = outputs.iter().enumerate().filter_map(|(position, output)| {
            output
                .expr
                .and_then(|expr| resolver.aggregate(expr))
                .map(|storage_type| (position, storage_type))
        });
        if let (Some((position, storage_type)), None) = (aggregates.next(), aggregates.next()) {
            outputs[position].storage_type = storage_type;
            aggregate = Some(position);
        }
        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            groups = exprs
                .iter()
                .map(|expr| resolver.group_position(expr, &outputs))
                .collect();
        }
    }

    Some(StorageLayout {
        outputs: outputs
            .into_iter()
            .map(|output| output.storage_type)
            .collect(),
        groups: groups.into_boxed_slice(),
        aggregate,
    })
}

/// The tables `select` reads, in `FROM` order, and whether a join merges
/// columns so a wildcard lists fewer than every table's.
fn bind<B: Backend, DB: DatabaseLike>(
    select: &Select,
    database: &DB,
) -> Option<(Vec<Binding>, bool)> {
    let mut bindings = Vec::new();
    let mut merges_columns = false;
    for from in &select.from {
        bindings.push(binding::<B, DB>(database, &from.relation)?);
        for join in &from.joins {
            bindings.push(binding::<B, DB>(database, &join.relation)?);
            match join_constraint(&join.join_operator)? {
                JoinConstraint::On(_) | JoinConstraint::None => {}
                JoinConstraint::Using(_)
                | JoinConstraint::UsingAlias(..)
                | JoinConstraint::Natural => merges_columns = true,
            }
        }
    }
    Some((bindings, merges_columns))
}

/// Whether a wildcard lists its tables' columns unchanged.
const fn plain(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

/// Append every column of `binding`'s table, as a wildcard lists them.
fn expand<B: Backend, DB: DatabaseLike>(
    outputs: &mut Vec<Output<'_>>,
    database: &DB,
    binding: &Binding,
) -> Option<()> {
    for column in 0..binding.arity {
        outputs.push(Output {
            storage_type: Some(column_storage_type::<B, DB>(
                database,
                binding.table,
                column,
            )?),
            column: Some((binding.table, column)),
            name: None,
            expr: None,
        });
    }
    Some(())
}

/// Resolves expressions of one select against the tables it reads.
struct Resolver<'d, B, DB> {
    database: &'d DB,
    bindings: &'d [Binding],
    backend: core::marker::PhantomData<B>,
}

impl<B: Backend, DB: DatabaseLike> Resolver<'_, B, DB> {
    fn output<'s>(&self, expr: &'s Expr, name: Option<&'s Ident>) -> Output<'s> {
        let column = self.column(expr);
        Output {
            storage_type: column.and_then(|(table, column)| {
                column_storage_type::<B, DB>(self.database, table, column)
            }),
            column,
            name,
            expr: Some(expr),
        }
    }

    /// The column `expr` references, when it is a column reference that
    /// names exactly one.
    fn column(&self, expr: &Expr) -> Option<(TableId, ColumnId)> {
        match expr {
            Expr::Nested(inner) => self.column(inner),
            Expr::Identifier(name) => {
                let mut found = self.bindings.iter().filter_map(|binding| {
                    written_column::<B, DB>(self.database, binding.table, name)
                        .map(|column| (binding.table, column))
                });
                match (found.next(), found.next()) {
                    (Some(column), None) => Some(column),
                    _ => None,
                }
            }
            Expr::CompoundIdentifier(parts) => match parts.as_slice() {
                [table, column] => {
                    let binding = self
                        .bindings
                        .iter()
                        .find(|binding| names::<B>(binding, table))?;
                    Some((
                        binding.table,
                        written_column::<B, DB>(self.database, binding.table, column)?,
                    ))
                }
                [schema, table, column] => {
                    let target = TargetName::new(&table.value, table.quote_style.is_some())
                        .with_schema(&schema.value, schema.quote_style.is_some());
                    let table = catalog_helpers::table_id_for_name::<B, DB>(self.database, target)?;
                    self.bindings
                        .iter()
                        .any(|binding| !binding.aliased && binding.table == table)
                        .then_some(())?;
                    Some((
                        table,
                        written_column::<B, DB>(self.database, table, column)?,
                    ))
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// When `expr` is an aggregate call, the type the backend answers it in,
    /// `None` inside when that type follows the values rather than the
    /// column.
    #[expect(
        clippy::option_option,
        reason = "not an aggregate, or one with no fixed type"
    )]
    fn aggregate(&self, expr: &Expr) -> Option<Option<StorageType>> {
        let Expr::Function(function) = expr else {
            return None;
        };
        if function.over.is_some() || !function.within_group.is_empty() {
            return None;
        }
        let name = function
            .name
            .0
            .last()?
            .as_ident()?
            .value
            .to_ascii_lowercase();
        let FunctionArguments::List(list) = &function.args else {
            return None;
        };
        let [FunctionArg::Unnamed(argument)] = list.args.as_slice() else {
            return None;
        };
        let column = match argument {
            FunctionArgExpr::Expr(argument) => self.column(argument),
            FunctionArgExpr::Wildcard
            | FunctionArgExpr::WildcardWithOptions(_)
            | FunctionArgExpr::QualifiedWildcard(_) => None,
        };
        let declared = || {
            let (table, column) = column?;
            catalog_helpers::column_scalar_kind::<B, DB>(self.database, table, column)?
                .declared_type()
        };
        Some(match name.as_str() {
            "count" => Some(StorageType::new(sqlite_diff_rs::WireType::Int)),
            "min" | "max" => column.and_then(|(table, column)| {
                column_storage_type::<B, DB>(self.database, table, column)
            }),
            "sum" => declared().and_then(|declared| sum_type(B::sum_rule(declared))),
            "avg" => declared().map(|declared| match declared {
                DeclaredType::Float(_) => StorageType::new(sqlite_diff_rs::WireType::Real),
                DeclaredType::Interval => StorageType::new(sqlite_diff_rs::WireType::Interval),
                _ => match B::MEAN {
                    MeanRule::Exact => StorageType::new(sqlite_diff_rs::WireType::Decimal),
                    MeanRule::Double => StorageType::new(sqlite_diff_rs::WireType::Real),
                },
            }),
            // Folded as a double, which is what a read answers over a float
            // column. Over an exact one a read answers the engine's decimal,
            // so the type follows the value.
            "var_pop" | "var_samp" | "variance" | "stddev_pop" | "stddev_samp" | "stddev" => {
                matches!(declared(), Some(DeclaredType::Float(_)))
                    .then(|| StorageType::new(sqlite_diff_rs::WireType::Real))
            }
            _ => return None,
        })
    }

    /// The position among `outputs` of the value `GROUP BY` names by
    /// `expr`.
    fn group_position(&self, expr: &Expr, outputs: &[Output<'_>]) -> Option<usize> {
        if let Expr::Value(value) = expr {
            if let sqlparser::ast::Value::Number(ordinal, _) = &value.value {
                let ordinal: usize = ordinal.parse().ok()?;
                return ordinal
                    .checked_sub(1)
                    .filter(|position| *position < outputs.len());
            }
        }
        // `GROUP BY name` names an input column before an output column.
        if let Some(column) = self.column(expr) {
            return outputs
                .iter()
                .position(|output| output.column == Some(column));
        }
        if let Expr::Identifier(name) = expr {
            if let Some(position) = outputs.iter().position(|output| {
                output
                    .name
                    .is_some_and(|output| same_column_name::<B>(output, name))
            }) {
                return Some(position);
            }
        }
        outputs.iter().position(|output| output.expr == Some(expr))
    }
}

/// The type a total accumulates in, `None` when it changes with the values
/// summed.
const fn sum_type(rule: SumRule) -> Option<StorageType> {
    Some(match rule {
        SumRule::Integer => StorageType::new(sqlite_diff_rs::WireType::Int),
        SumRule::IntegerPromotingToDouble => return None,
        SumRule::Decimal { .. } => StorageType::new(sqlite_diff_rs::WireType::Decimal),
        SumRule::Single => StorageType::single_precision(),
        SumRule::Double => StorageType::new(sqlite_diff_rs::WireType::Real),
        SumRule::Interval => StorageType::new(sqlite_diff_rs::WireType::Interval),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::WireType;
    use sqlparser::dialect::PostgreSqlDialect;

    use super::storage_layout;
    use crate::backend::Postgres;
    use crate::emit::{StorageLayout, StorageType};

    const DDL: &str = "CREATE TABLE chats (id UUID PRIMARY KEY, title TEXT);
        CREATE TABLE messages (id INT PRIMARY KEY, chat_id UUID, size REAL, sent TIMESTAMPTZ);";

    const UUID: Option<StorageType> = Some(StorageType::new(WireType::Uuid));
    const INT: Option<StorageType> = Some(StorageType::new(WireType::Int));
    const TEXT: Option<StorageType> = Some(StorageType::new(WireType::Text));
    const TIMESTAMPTZ: Option<StorageType> = Some(StorageType::new(WireType::TimestampTz));
    const SINGLE: Option<StorageType> = Some(StorageType::single_precision());

    fn layout(sql: &str, delivers_aggregates: bool) -> Option<StorageLayout> {
        let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let statement =
            crate::compiler::sql_shape::parse_single_statement(sql, &PostgreSqlDialect {}).unwrap();
        storage_layout::<Postgres, _>(&statement, &database, delivers_aggregates)
    }

    #[test]
    fn wildcards_expand_to_every_column_in_from_order() {
        let layout = layout(
            "SELECT m.*, c.title, upper(c.title) FROM messages m JOIN chats c ON c.id = m.chat_id",
            false,
        )
        .unwrap();
        assert_eq!(
            layout.outputs(),
            [INT, UUID, SINGLE, TIMESTAMPTZ, TEXT, None]
        );
        assert_eq!(layout.groups(), [None; 0]);
        assert_eq!(layout.aggregate(), None);
    }

    #[test]
    fn groups_are_placed_by_column_ordinal_and_alias() {
        let layout = layout_of(
            "SELECT chat_id AS chat, size, count(*) FROM messages GROUP BY messages.chat_id, 2",
        )
        .unwrap();
        assert_eq!(layout.groups(), [Some(0), Some(1)]);
        assert_eq!(layout.aggregate(), Some(2));
        assert_eq!(layout.outputs(), [UUID, SINGLE, INT]);

        let layout =
            layout_of("SELECT chat_id AS chat, sum(size) FROM messages GROUP BY chat").unwrap();
        assert_eq!(layout.groups(), [Some(0)]);
        assert_eq!(layout.aggregate(), Some(1));
        assert_eq!(layout.outputs(), [UUID, SINGLE]);
    }

    #[test]
    fn only_a_lone_aggregate_is_the_maintained_one() {
        let layout =
            layout_of("SELECT chat_id, count(*), max(sent) FROM messages GROUP BY chat_id")
                .unwrap();
        assert_eq!(layout.aggregate(), None);
        assert_eq!(layout.outputs(), [UUID, None, None]);
    }

    fn layout_of(sql: &str) -> Option<StorageLayout> {
        layout(sql, true)
    }

    #[test]
    fn a_group_left_out_of_the_select_list_has_no_position() {
        let layout = layout_of("SELECT max(sent) FROM messages GROUP BY chat_id").unwrap();
        assert_eq!(layout.groups(), [None]);
        assert_eq!(layout.aggregate(), Some(0));
        assert_eq!(layout.outputs(), [TIMESTAMPTZ]);
    }

    #[test]
    fn unresolvable_select_lists_have_no_layout() {
        for sql in [
            "WITH x AS (SELECT * FROM chats) SELECT * FROM x",
            "SELECT id FROM chats UNION SELECT chat_id FROM messages",
            "SELECT * FROM (SELECT id FROM chats) AS d",
            "SELECT * FROM chats JOIN messages USING (id)",
            "SELECT * FROM chats AS c (a, b)",
        ] {
            assert!(layout(sql, false).is_none(), "{sql}");
        }
    }

    const SPANS: &str =
        "CREATE TABLE spans (id INT PRIMARY KEY, took INTERVAL, ratio DOUBLE PRECISION);";

    fn layout_in<B: crate::backend::Backend>(
        dialect: &dyn sqlparser::dialect::Dialect,
        sql: &str,
    ) -> Option<StorageLayout> {
        let database =
            ParserDB::parse::<PostgreSqlDialect>(&alloc::format!("{DDL}{SPANS}")).unwrap();
        let statement = crate::compiler::sql_shape::parse_single_statement(sql, dialect).unwrap();
        storage_layout::<B, _>(&statement, &database, true)
    }

    #[test]
    fn outputs_resolve_through_every_reference_form() {
        let layout = layout("SELECT *, (title), public.chats.title FROM chats", false).unwrap();
        assert_eq!(layout.outputs(), [UUID, TEXT, TEXT, TEXT]);
        let layout = layout_of("SELECT public.chats.title FROM chats c").unwrap();
        assert_eq!(layout.outputs(), [None]);
        assert!(layout_of("INSERT INTO chats (title) VALUES ('x')").is_none());
    }

    #[test]
    fn calls_other_than_plain_aggregates_have_no_type() {
        let layout = layout_of(
            "SELECT current_timestamp, coalesce(title, 'x'), upper(title), count(*) OVER (), \
             count(*) FROM chats GROUP BY 3, 9, upper(title)",
        )
        .unwrap();
        assert_eq!(layout.outputs(), [None, None, None, None, INT]);
        assert_eq!(layout.groups(), [Some(2), None, Some(2)]);
        assert_eq!(layout.aggregate(), Some(4));
    }

    #[test]
    fn aggregates_take_the_type_their_backend_answers() {
        const INTERVAL: Option<StorageType> = Some(StorageType::new(WireType::Interval));
        const REAL: Option<StorageType> = Some(StorageType::new(WireType::Real));
        let postgres = PostgreSqlDialect {};
        let sqlite = sqlparser::dialect::SQLiteDialect {};
        for (layout, expected) in [
            (
                layout_in::<Postgres>(&postgres, "SELECT avg(took) FROM spans"),
                INTERVAL,
            ),
            (
                layout_in::<Postgres>(&postgres, "SELECT sum(ratio) FROM spans"),
                REAL,
            ),
            (
                layout_in::<crate::backend::SQLite>(&sqlite, "SELECT avg(id) FROM spans"),
                REAL,
            ),
            (
                layout_in::<crate::backend::SQLite>(&sqlite, "SELECT sum(id) FROM spans"),
                None,
            ),
        ] {
            let layout = layout.unwrap();
            assert_eq!(layout.aggregate(), Some(0));
            assert_eq!(layout.outputs(), [expected]);
        }
    }

    #[test]
    fn filtered_wildcards_have_no_layout() {
        let generic = sqlparser::dialect::GenericDialect {};
        for sql in [
            "SELECT * EXCLUDE (title) FROM chats",
            "SELECT c.* EXCLUDE (title) FROM chats c",
        ] {
            assert!(layout_in::<Postgres>(&generic, sql).is_none(), "{sql}");
        }
    }
}
