//! Complete public read rules for caller-bound registration.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ops::ControlFlow;
use hashbrown::{HashMap, HashSet};
use rls2fga::classifier::patterns::{
    ClassifiedExpr, ClassifiedPolicy, Composite, PatternClass, PolicyMode,
};
use rls2fga::parser::names::role_keyword;
use rls2fga::translator::Translator;
use rls2fga_types::{ConfidenceLevel, PolicyCommand};
use sql_traits::prelude::{DatabaseLike, ExpressionScope, PolicyLike, SemanticProofs, TableLike};
use sql_traits::structs::IdentifierCase;
use sqlparser::ast::{BinaryOperator, CastKind, Expr, InTarget, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser_canonicalize::Canonicalizer;

use crate::catalog_helpers::{contract_table_id, table_has_rls};
use crate::TableId;

/// Compose each RLS-enabled table's complete public read rule.
pub fn read_rules<DB: DatabaseLike>(
    database: &DB,
    translator: &Translator,
) -> HashMap<TableId, Expr> {
    let min_confidence = translator.min_confidence();
    let classified = translator.classify(database);

    let mut refused = HashSet::new();
    for policy in database.policies() {
        if policy.applies_to_public()
            || !matches!(
                policy.command(),
                sqlparser::ast::CreatePolicyCommand::Select
                    | sqlparser::ast::CreatePolicyCommand::All
            )
        {
            continue;
        }
        let Ok(table) = policy.table(database) else {
            return HashMap::new();
        };
        if let Some(id) = table
            .table_id(database)
            .and_then(|id| u32::try_from(id).ok())
        {
            refused.insert(id);
        }
    }

    let mut by_table: HashMap<&rls2fga_types::TableId, Vec<&ClassifiedPolicy>> = HashMap::new();
    for policy in &classified {
        let Some(target) = policy.resolved_table() else {
            continue;
        };
        if !matches!(policy.command(), PolicyCommand::Select | PolicyCommand::All) {
            continue;
        }
        by_table.entry(target).or_default().push(policy);
    }

    let mut rules: HashMap<TableId, Expr> = HashMap::new();
    for (target, reads) in by_table {
        let Some(table) = contract_table_id(database, target) else {
            continue;
        };
        if refused.contains(&table) {
            continue;
        }
        let Ok(true) = table_has_rls(database, table) else {
            continue;
        };
        let Some(scope) = usize::try_from(table)
            .ok()
            .and_then(|index| database.table_by_id(index))
        else {
            refused.insert(table);
            continue;
        };
        match public_read_rule(&reads, min_confidence, scope, database) {
            Some(rule) => {
                rules.insert(table, rule);
            }
            None => {
                refused.insert(table);
            }
        }
    }

    // Row security without a permissive read policy admits nobody.
    for table in database.tables() {
        let Some(index) = table.table_id(database) else {
            continue;
        };
        let Ok(table_id) = u32::try_from(index) else {
            continue;
        };
        if rules.contains_key(&table_id) || refused.contains(&table_id) {
            continue;
        }
        let Ok(true) = table_has_rls(database, table_id) else {
            continue;
        };
        rules.insert(table_id, Expr::Value(SqlValue::Boolean(false).into()));
    }

    rules
}

/// Compose public permissive grants and restrictive barriers, or decline the fold.
fn public_read_rule<DB: DatabaseLike>(
    reads: &[&ClassifiedPolicy],
    min_confidence: ConfidenceLevel,
    table: &DB::Table,
    database: &DB,
) -> Option<Expr> {
    let mut permissive: Vec<Expr> = Vec::new();
    let mut restrictive: Vec<Expr> = Vec::new();
    for policy in reads {
        // Missing clauses have no classified rule to fold.
        let (Some(clause), Some(classification)) = (policy.using(), policy.using_classification())
        else {
            return None;
        };
        // A person's identity cannot answer the request's database role.
        if role_keyworded(clause) {
            return None;
        }
        if !complete(classification, min_confidence) {
            return None;
        }
        let clause = written_spelling(clause, table, database);
        match policy.mode() {
            PolicyMode::Permissive => permissive.push(clause),
            PolicyMode::Restrictive => restrictive.push(clause),
        }
    }
    Some(compose(permissive, restrictive))
}

/// Require every nested classification to reach the confidence threshold.
fn complete(classification: &ClassifiedExpr, min_confidence: ConfidenceLevel) -> bool {
    if classification.confidence < min_confidence {
        return false;
    }
    match &classification.pattern {
        PatternClass::Unknown(_) => false,
        PatternClass::P5ParentInheritance(inheritance) => {
            complete(&inheritance.inner_pattern, min_confidence)
        }
        PatternClass::ExpandedFunction(expanded) => complete(&expanded.inner, min_confidence),
        PatternClass::P7AbacAnd(abac) => complete(&abac.relationship_part, min_confidence),
        PatternClass::P8Composite(Composite { parts, .. }) => {
            parts.iter().all(|part| complete(part, min_confidence))
        }
        // Other patterns contain no nested classifications.
        _ => true,
    }
}

/// Whether the clause reads the request's database role.
fn role_keyworded(clause: &Expr) -> bool {
    sqlparser::ast::visit_expressions(clause, |node| match node {
        Expr::Identifier(ident)
            if ident.quote_style.is_none() && role_keyword(&ident.value).is_some() =>
        {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// `clause` with each comparison PostgreSQL's deparse spells respelled as
/// written, only where the semantic proofs give both spellings one canonical
/// text, so a converting cast such as `(ch)::text` on a `char` column stays.
fn written_spelling<DB: DatabaseLike>(clause: &Expr, table: &DB::Table, database: &DB) -> Expr {
    match clause {
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            right,
        } => Expr::BinaryOp {
            left: Box::new(written_spelling(left, table, database)),
            op: op.clone(),
            right: Box::new(written_spelling(right, table, database)),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(written_spelling(expr, table, database)),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(written_spelling(inner, table, database))),
        comparison => match without_conversions(comparison) {
            Some(respelled) if canonically_alike(&respelled, comparison, table, database) => {
                respelled
            }
            _ => comparison.clone(),
        },
    }
}

/// `comparison` without the conversions the deparse spells, or `None` when it
/// spells none.
fn without_conversions(comparison: &Expr) -> Option<Expr> {
    let respelled = match comparison {
        Expr::BinaryOp { left, op, right } => {
            let operands = || (Box::new(bare(left)), Box::new(bare(right)));
            match op {
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    let (left, right) = operands();
                    Expr::BinaryOp {
                        left,
                        op: op.clone(),
                        right,
                    }
                }
                BinaryOperator::PGLikeMatch | BinaryOperator::PGNotLikeMatch => {
                    let (expr, pattern) = operands();
                    Expr::Like {
                        negated: matches!(op, BinaryOperator::PGNotLikeMatch),
                        any: false,
                        expr,
                        pattern,
                        escape_char: None,
                    }
                }
                BinaryOperator::PGILikeMatch | BinaryOperator::PGNotILikeMatch => {
                    let (expr, pattern) = operands();
                    Expr::ILike {
                        negated: matches!(op, BinaryOperator::PGNotILikeMatch),
                        any: false,
                        expr,
                        pattern,
                        escape_char: None,
                    }
                }
                _ => return None,
            }
        }
        Expr::AnyOp {
            left,
            compare_op: BinaryOperator::Eq,
            right,
            is_some,
        } => array_elements(right).map_or_else(
            || Expr::AnyOp {
                left: Box::new(bare(left)),
                compare_op: BinaryOperator::Eq,
                right: right.clone(),
                is_some: *is_some,
            },
            |elements| in_list(left, elements, false),
        ),
        Expr::AllOp {
            left,
            compare_op: BinaryOperator::NotEq,
            right,
        } => in_list(left, array_elements(right)?, true),
        _ => return None,
    };
    (respelled != *comparison).then_some(respelled)
}

/// `tested [NOT] IN (elements)`, each operand without its conversion.
fn in_list(tested: &Expr, elements: &[Expr], negated: bool) -> Expr {
    Expr::In {
        expr: Box::new(bare(tested)),
        target: InTarget::List(elements.iter().map(bare).collect()),
        negated,
        global: false,
    }
}

/// The elements of an `ARRAY[...]` constructor, under any cast and parentheses.
fn array_elements(expr: &Expr) -> Option<&[Expr]> {
    match expr {
        Expr::Array(array) => Some(&array.elem),
        Expr::Nested(inner)
        | Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr: inner,
            format: None,
            ..
        } => array_elements(inner),
        _ => None,
    }
}

/// `operand` without the cast and the parentheses around a constant or a column.
fn bare(operand: &Expr) -> Expr {
    let is_constant_or_column = |expr: &Expr| {
        matches!(
            expr,
            Expr::Value(_) | Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        )
    };
    match operand {
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr,
            format: None,
            ..
        } => {
            let inner = bare(expr);
            if is_constant_or_column(&inner) {
                inner
            } else {
                operand.clone()
            }
        }
        Expr::Nested(inner) if is_constant_or_column(inner) => (**inner).clone(),
        _ => operand.clone(),
    }
}

/// Whether `respelled` and `deparsed` canonicalize alike over `table`.
fn canonically_alike<DB: DatabaseLike>(
    respelled: &Expr,
    deparsed: &Expr,
    table: &DB::Table,
    database: &DB,
) -> bool {
    let canonical = |expr: &Expr| {
        let scope = ExpressionScope::Table(table);
        SemanticProofs::for_expression(expr, &scope, database, IdentifierCase::Folded)
            .ok()
            .and_then(|proofs| {
                Canonicalizer::new(&PostgreSqlDialect {})
                    .normalize_with_context(&proofs.view())
                    .ok()
            })
    };
    canonical(respelled).is_some_and(|text| canonical(deparsed).is_some_and(|other| text == other))
}

/// Intersect restrictive clauses with the permissive union.
fn compose(permissive: Vec<Expr>, restrictive: Vec<Expr>) -> Expr {
    let rule = permissive.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::Or,
        right: Box::new(right),
    });
    let mut rule = rule.unwrap_or_else(|| Expr::Value(SqlValue::Boolean(false).into()));
    for clause in restrictive {
        rule = Expr::BinaryOp {
            left: Box::new(rule),
            op: BinaryOperator::And,
            right: Box::new(clause),
        };
    }
    rule
}
