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
use sql_traits::prelude::{DatabaseLike, PolicyLike, TableLike};
use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue};

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
        match public_read_rule(&reads, min_confidence) {
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
fn public_read_rule(reads: &[&ClassifiedPolicy], min_confidence: ConfidenceLevel) -> Option<Expr> {
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
        match policy.mode() {
            PolicyMode::Permissive => permissive.push(clause.clone()),
            PolicyMode::Restrictive => restrictive.push(clause.clone()),
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
