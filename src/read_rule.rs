//! Complete public read rules for caller-bound registration.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::ops::ControlFlow;
use hashbrown::{HashMap, HashSet};
use rls2fga::classifier::function_registry::FunctionRegistry;
use rls2fga::classifier::patterns::{
    ClassifiedExpr, ClassifiedPolicy, Composite, ConstantInCallerSet, PatternClass, PolicyMode,
    RequestTest,
};
use rls2fga::classifier::policy_classifier::classify_expr;
use rls2fga::parser::names::role_keyword;
use rls2fga::translator::Translator;
use rls2fga_types::{ConfidenceLevel, PolicyCommand, RequestPredicate};
use sql_traits::prelude::{DatabaseLike, ExpressionScope, PolicyLike, SemanticProofs, TableLike};
use sql_traits::structs::IdentifierCase;
use sqlparser::ast::{
    BinaryOperator, CastKind, Expr, Function, FunctionArguments, InTarget, ObjectNamePart,
    Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser_canonicalize::Canonicalizer;

use crate::catalog_helpers::{contract_table_id, lookup_table, rls_of, table_has_rls};
use crate::visibility::policy::evaluate::request_holds;
use crate::visibility::policy::RequestValues;
use crate::{RegisterError, TableId};

/// A table's complete public read rule, beside its request-only clauses, which each registration settles from its request.
#[derive(Debug)]
pub struct ReadRule {
    /// The rule, each request-only clause in it spelled as its policy writes it.
    rule: Arc<Expr>,
    /// The request-only clauses `rule` carries, each beside what it tests.
    gates: Vec<RequestGate>,
    /// Each set parameter a gate splits, beside the separator its policy splits the setting on.
    separators: Vec<(String, String)>,
}

/// One clause of a read rule that reads only the request.
#[derive(Debug)]
struct RequestGate {
    /// The clause as the rule spells it.
    clause: Expr,
    /// What it tests of the request.
    test: RequestPredicate,
}

impl ReadRule {
    /// A rule reading no request value.
    fn fixed(rule: Expr) -> Self {
        Self {
            rule: Arc::new(rule),
            gates: Vec::new(),
            separators: Vec::new(),
        }
    }

    /// The rule with each request-only clause settled from `stated`, or `None` when `stated` omits a parameter one of them reads.
    ///
    /// # Errors
    ///
    /// [`RegisterError::RequestValueRefused`] when a value stated under a set parameter holds the separator its policy splits the setting on.
    pub fn settle(
        &self,
        stated: &[(String, Vec<String>)],
    ) -> Result<Option<Arc<Expr>>, RegisterError> {
        let sent = |parameter: &str, out: &mut RequestValues| stated_value(stated, parameter, out);
        let mut values = RequestValues::new();
        for (parameter, separator) in &self.separators {
            values.reset();
            if sent(parameter, &mut values)
                && values
                    .values()
                    .any(|value| value.contains(separator.as_str()))
            {
                return Err(RegisterError::RequestValueRefused(format!(
                    "a value stated under '{parameter}' holds '{separator}', the separator the \
                     read rule splits that setting on, so no request carries it as one value"
                )));
            }
        }
        if self.gates.is_empty() {
            return Ok(Some(Arc::clone(&self.rule)));
        }
        let mut verdicts = Vec::with_capacity(self.gates.len());
        for gate in &self.gates {
            // The acting role is left unstated, so a gate on it keeps query-only matching.
            let Some(verdict) = request_holds(&gate.test, None, &sent, &mut values) else {
                return Ok(None);
            };
            verdicts.push(verdict);
        }
        Ok(Some(Arc::new(settled(&self.rule, &self.gates, &verdicts))))
    }
}

/// Push the values `stated` gives `parameter` into `out`, answering whether `stated` names it.
fn stated_value(
    stated: &[(String, Vec<String>)],
    parameter: &str,
    out: &mut RequestValues,
) -> bool {
    let mut named = false;
    for (_, values) in stated.iter().filter(|(name, _)| name == parameter) {
        named = true;
        for value in values {
            out.push(value);
        }
    }
    named
}

/// `rule` with each of `gates` replaced by its verdict.
fn settled(rule: &Expr, gates: &[RequestGate], verdicts: &[bool]) -> Expr {
    replaced(rule, &mut |node| {
        let position = gates.iter().position(|gate| gate.clause == *node)?;
        Some(Expr::Value(SqlValue::Boolean(verdicts[position]).into()))
    })
}

/// `clause` with each subtree `replace` answers for replaced, asked top down through `AND`, `OR` and parentheses.
fn replaced(clause: &Expr, replace: &mut impl FnMut(&Expr) -> Option<Expr>) -> Expr {
    if let Some(replacement) = replace(clause) {
        return replacement;
    }
    match clause {
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            right,
        } => Expr::BinaryOp {
            left: Box::new(replaced(left, replace)),
            op: op.clone(),
            right: Box::new(replaced(right, replace)),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(replaced(inner, replace))),
        other => other.clone(),
    }
}

/// Whether `clause` is an `AND`, an `OR` or parentheses, which [`replaced`] reads through.
const fn connective(clause: &Expr) -> bool {
    matches!(
        clause,
        Expr::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            ..
        } | Expr::Nested(_)
    )
}

/// Compose each RLS-enabled table's complete public read rule, keyed by the
/// id `served` gives the table, beside the translator's registry enriched
/// with the functions `database` defines.
///
/// A table `served` gives no id is left out.
pub fn read_rules<DB: DatabaseLike>(
    database: &DB,
    translator: &Translator,
    served: impl Fn(TableId) -> Option<TableId>,
) -> (HashMap<TableId, ReadRule>, FunctionRegistry) {
    let min_confidence = translator.min_confidence();
    let (classified, registry) = translator.classify_with_effective_registry(database);
    let rules = rules_by_table(database, &classified, &registry, min_confidence)
        .into_iter()
        .filter_map(|(table, rule)| Some((served(table)?, rule)))
        .collect();
    (rules, registry)
}

/// Each RLS-enabled table's complete public read rule, keyed by its id in
/// `database`.
fn rules_by_table<DB: DatabaseLike>(
    database: &DB,
    classified: &[ClassifiedPolicy],
    registry: &FunctionRegistry,
    min_confidence: ConfidenceLevel,
) -> HashMap<TableId, ReadRule> {
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
    for policy in classified {
        let Some(target) = policy.resolved_table() else {
            continue;
        };
        if !matches!(policy.command(), PolicyCommand::Select | PolicyCommand::All) {
            continue;
        }
        by_table.entry(target).or_default().push(policy);
    }

    let mut rules: HashMap<TableId, ReadRule> = HashMap::new();
    for (target, reads) in by_table {
        let Some(table) = contract_table_id(database, target) else {
            continue;
        };
        if refused.contains(&table) {
            continue;
        }
        let Some(scope) = lookup_table(database, table)
            .ok()
            .filter(|scope| matches!(rls_of(database, scope, table), Ok(true)))
        else {
            continue;
        };
        match public_read_rule(&reads, registry, min_confidence, scope, database) {
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
        rules.insert(
            table_id,
            ReadRule::fixed(Expr::Value(SqlValue::Boolean(false).into())),
        );
    }

    rules
}

/// Compose public permissive grants and restrictive barriers, or decline the fold.
fn public_read_rule<DB: DatabaseLike>(
    reads: &[&ClassifiedPolicy],
    registry: &FunctionRegistry,
    min_confidence: ConfidenceLevel,
    table: &DB::Table,
    database: &DB,
) -> Option<ReadRule> {
    let mut permissive: Vec<Expr> = Vec::new();
    let mut restrictive: Vec<Expr> = Vec::new();
    let mut gates = Gates::default();
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
        let classify = |part: &Expr| {
            classify_expr(
                part,
                database,
                registry,
                policy.table_name(),
                policy.command(),
            )
        };
        let clause = gates.gated(&clause, &classify, min_confidence);
        match policy.mode() {
            PolicyMode::Permissive => permissive.push(clause),
            PolicyMode::Restrictive => restrictive.push(clause),
        }
    }
    Some(ReadRule {
        rule: Arc::new(compose(permissive, restrictive)),
        gates: gates.gates,
        separators: gates.separators,
    })
}

/// The request-only clauses found across one table's policies.
#[derive(Default)]
struct Gates {
    gates: Vec<RequestGate>,
    separators: Vec<(String, String)>,
}

impl Gates {
    /// `clause` with its constant tests settled and its request-only clauses, each the largest reading only the request, recorded.
    fn gated(
        &mut self,
        clause: &Expr,
        classify: &impl Fn(&Expr) -> ClassifiedExpr,
        min_confidence: ConfidenceLevel,
    ) -> Expr {
        replaced(clause, &mut |node| {
            let classified = classify(node);
            if classified.confidence < min_confidence {
                return None;
            }
            match classified.pattern.request_test()? {
                RequestTest::Constant(holds) => Some(Expr::Value(SqlValue::Boolean(holds).into())),
                RequestTest::Predicate(test) => {
                    self.split_sets(&classified.pattern);
                    if !self.gates.iter().any(|gate| gate.clause == *node) {
                        self.gates.push(RequestGate {
                            clause: node.clone(),
                            test,
                        });
                    }
                    Some(node.clone())
                }
            }
        })
    }

    /// Record the separator each caller set `pattern` splits is split on.
    fn split_sets(&mut self, pattern: &PatternClass) {
        match pattern {
            PatternClass::P16ConstantInCallerSet(ConstantInCallerSet {
                separator: Some(separator),
                source,
                ..
            }) => {
                let split = (source.request_parameter().into(), separator.clone());
                if !self.separators.contains(&split) {
                    self.separators.push(split);
                }
            }
            PatternClass::P8Composite(Composite { parts, .. }) => {
                for part in parts {
                    self.split_sets(&part.pattern);
                }
            }
            _ => {}
        }
    }
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

/// Whether the clause reads the request's database role, which sqlparser parses as an argument-less call.
fn role_keyworded(clause: &Expr) -> bool {
    sqlparser::ast::visit_expressions(clause, |node| match node {
        Expr::Function(Function {
            name,
            args: FunctionArguments::None,
            ..
        }) if matches!(
            name.0.as_slice(),
            [ObjectNamePart::Identifier(ident)]
                if ident.quote_style.is_none() && role_keyword(&ident.value).is_some()
        ) =>
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
    replaced(clause, &mut |comparison| {
        if connective(comparison) {
            return None;
        }
        Some(match without_conversions(comparison) {
            Some(respelled) if canonically_alike(&respelled, comparison, table, database) => {
                respelled
            }
            _ => comparison.clone(),
        })
    })
}

/// `comparison` without the conversions the deparse spells, or `None` when it
/// spells none.
fn without_conversions(comparison: &Expr) -> Option<Expr> {
    let respelled = match comparison {
        Expr::BinaryOp {
            left,
            op:
                op @ (BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq),
            right,
        } => Expr::BinaryOp {
            left: Box::new(bare(left)),
            op: op.clone(),
            right: Box::new(bare(right)),
        },
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
            |elements| in_list(left, elements),
        ),
        _ => return None,
    };
    (respelled != *comparison).then_some(respelled)
}

/// `tested IN (elements)`, each operand without its conversion.
fn in_list(tested: &Expr, elements: &[Expr]) -> Expr {
    Expr::In {
        expr: Box::new(bare(tested)),
        target: InTarget::List(elements.iter().map(bare).collect()),
        negated: false,
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

/// `operand` without the casts and the parentheses around a constant or a column.
fn bare(operand: &Expr) -> Expr {
    let mut inner = operand;
    while let Expr::Nested(expr)
    | Expr::Cast {
        kind: CastKind::Cast | CastKind::DoubleColon,
        expr,
        format: None,
        ..
    } = inner
    {
        inner = expr;
    }
    match inner {
        Expr::Value(_) | Expr::Identifier(_) | Expr::CompoundIdentifier(_) => inner.clone(),
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
