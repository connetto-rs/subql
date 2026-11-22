//! Asking whether a membership term can be served, and by which table's rows.
//!
//! One filter has two executors: the SQL it was written as, which the snapshot
//! runs, and the records compiled here, which answer per changed row. A filter
//! they answer differently is worse than one that is refused, so this refuses in
//! `rls2fga`'s own wording rather than serving half of it.
//!
//! Switched by the `membership-term` feature, because it is the only half of the
//! term that names an `rls2fga` type. The bounded form itself is recognised in
//! every build, both wasm targets included.

use alloc::format;
use alloc::string::ToString;
use alloc::vec::Vec;

use rls2fga::term::{TermChain, TermShapes};
use rls2fga::types::RelationShapes;
use rls2fga::types::{Guard, RecordDerivation, SubjectKey, ValueSource};
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::Expr;

use crate::access_rules::{AccessRules, Translation};
use crate::term::{CompiledTerm, TermCaller, TermMovement, TermPlan};
use crate::{catalog_helpers, RegisterError, TableId};

/// Settle whether `term` on `table` can be served for a subscriber reading as
/// `acting_role`, and how its subscriber set moves.
///
/// The role settles the read rules of the membership table the term reads
/// through. Without one, any row-level security on that table refuses the term.
/// `guarded_table` names `table` as SQL text, which `rls2fga` reads back from
/// `rules`, while columns resolve in the served `database`.
///
/// # Errors
///
/// [`RegisterError::MembershipTermRefused`] carrying `rls2fga`'s reason when the
/// relationship cannot be compiled, or subql's own reason when it can be but the
/// shape describing it reads something a changed row cannot answer.
pub fn plan_term<B: crate::backend::Backend, DB: DatabaseLike>(
    term: &CompiledTerm,
    table: TableId,
    guarded_table: &str,
    database: &DB,
    rules: &AccessRules<'_, DB>,
    translation: &Translation,
    acting_role: Option<&str>,
) -> Result<TermPlan, RegisterError> {
    let shapes = rules
        .describe_membership_term(
            &term.expr,
            &translation.registry,
            guarded_table,
            translation.translator.min_confidence(),
            acting_role,
        )
        .map_err(|refusal| RegisterError::MembershipTermRefused(refusal.reason))?;
    let caller = match &shapes.caller {
        rls2fga::term::TermCaller::Identity => TermCaller::Identity,
        rls2fga::term::TermCaller::Subjects { .. } => TermCaller::Subjects,
    };

    if term.compares_the_caller() {
        return caller_plan::<B, DB>(term, table, database, rules, &shapes, caller);
    }

    let movement = member_columns::<B, DB>(term, table, &shapes, database, rules)?;

    // Each value the term compares has to be the value the membership row keys
    // its object on at the same position, or the lookup stores under one key
    // and reads under another. The two coincide by construction for the chain
    // `rls2fga` reports, and this says so rather than trusting it, since a
    // mismatch admits nobody in silence.
    for (compared_column, member_key) in term.columns.iter().zip(&movement.member_keys) {
        let compared =
            catalog_helpers::column_scalar_kind::<B, DB>(database, table, *compared_column);
        let keyed = catalog_helpers::column_scalar_kind::<B, DB>(
            database,
            movement.member_table,
            *member_key,
        );
        if compared != keyed {
            return Err(RegisterError::MembershipTermRefused(format!(
                "a column this filter compares and the column the relationship is keyed on hold \
                 different kinds of value ({compared:?} against {keyed:?}), so a row's value \
                 could never be found in the set the relationship admits"
            )));
        }
    }

    Ok(TermPlan {
        slot: term.slot,
        columns: term.columns.clone(),
        caller,
        moved_by: Some(movement),
    })
}

/// Settle a caller comparison: the compiled rule has to be the one link naming
/// the caller from the compared column of the subscribed row, with nothing
/// beside it, or the SQL and the term would answer differently.
///
/// Verified against the shapes rather than trusted, in the same spirit as the
/// kind check above: each divergence admits the wrong rows in silence.
fn caller_plan<B: crate::backend::Backend, DB: DatabaseLike>(
    term: &CompiledTerm,
    table: TableId,
    database: &DB,
    rules: &AccessRules<'_, DB>,
    shapes: &TermShapes,
    caller: TermCaller,
) -> Result<TermPlan, RegisterError> {
    let TermChain::Direct { relation } = &shapes.chain else {
        return Err(RegisterError::MembershipTermRefused(
            "this comparison reaches the subscriber through more than the compared row itself, \
             so the subscriber the request states is not the one value it admits"
                .to_string(),
        ));
    };
    let entry = shapes
        .relations
        .iter()
        .find(|entry| &entry.relation == relation)
        .ok_or_else(|| {
            RegisterError::MembershipTermRefused(format!(
                "nothing states the records for '{relation}', so the comparison admits nobody"
            ))
        })?;

    let refuse = |what: &str| {
        RegisterError::MembershipTermRefused(format!(
            "the records for '{relation}' {what}, so the subscriber the request states is not \
             the one value this comparison admits",
            relation = entry.relation,
        ))
    };
    let [shape] = entry.shapes.as_slice() else {
        return Err(refuse("are stated by more than one query, or by none"));
    };
    let RecordDerivation::FromRow {
        table: row_table,
        template,
        guards,
        ..
    } = &shape.derivation
    else {
        return Err(refuse("need more than one row"));
    };
    // A conditional record grants nobody on its own: the service completes the
    // comparison with what the request supplies, and the term performs no such
    // completion.
    if template.context.is_some() {
        return Err(refuse(
            "carry a condition the compared row alone does not answer",
        ));
    }
    // The one guard the term itself enforces: a NULL compared cell keys nothing
    // and admits nobody, which is exactly `NotNull` on the compared column. Any
    // other guard admits records only for rows the term never re-checks.
    let enforced_by_the_lookup = |guard: &Guard| {
        matches!(guard, Guard::NotNull(column)
            if catalog_helpers::contract_column_id::<B, _>(database, table, column.as_str())
                == Some(term.columns[0]))
    };
    if !guards.iter().all(enforced_by_the_lookup) {
        return Err(refuse(
            "are guarded on more than the compared value being present",
        ));
    }
    if rules.served_table(row_table) != Some(table) {
        return Err(refuse("read a table other than the subscribed one"));
    }
    if subject_column::<B, _>(&template.subject_key, database, table) != Some(term.columns[0]) {
        return Err(refuse(
            "name the caller from something other than the compared column",
        ));
    }

    Ok(TermPlan {
        slot: term.slot,
        columns: term.columns.clone(),
        caller,
        moved_by: None,
    })
}

/// The table and the columns of the shape that names a caller.
///
/// One walk for both chains. `Direct` names the caller from a row of the
/// subscribed table, `Through` from a row of the membership table, and in both
/// cases the shape to read is the one filling the relation whose records name a
/// caller.
///
/// The membership keys come back aligned with the term's own compared columns:
/// for the `EXISTS` spelling the written pairs say which membership column
/// each compared column meets, and the shape's key set is verified against
/// them rather than trusted for its order, since `rls2fga` orders a composite
/// key by the object it names rather than by the filter's text.
fn member_columns<B: crate::backend::Backend, DB: DatabaseLike>(
    term: &CompiledTerm,
    table: TableId,
    shapes: &TermShapes,
    database: &DB,
    rules: &AccessRules<'_, DB>,
) -> Result<TermMovement, RegisterError> {
    let relation =
        match &shapes.chain {
            TermChain::Direct { relation } => relation,
            TermChain::Through { member, .. } => member,
            // `TermChain` is `#[non_exhaustive]`: a longer chain is refused rather
            // than read as one of these two, since a middle link nothing tracks
            // leaves the set moving on rows subql never looks at.
            _ => return Err(RegisterError::MembershipTermRefused(
                "this relationship reaches the subscriber by a longer chain than SubQL tracks. \
                 SubQL follows the filtered row to a related row and that row to the \
                 subscriber, and no further."
                    .to_string(),
            )),
        };

    let entry = shapes
        .relations
        .iter()
        .find(|entry| &entry.relation == relation)
        .ok_or_else(|| {
            RegisterError::MembershipTermRefused(format!(
                "nothing states the records for '{relation}', so no changed row moves who this \
                 filter admits"
            ))
        })?;

    let movement = read_from_one_row::<B, _>(entry, database, rules)?;
    align_with_the_filter::<B, DB>(term, table, movement, database)
}

/// Reorder the shape's membership keys into the filter's own pair order, or
/// refuse when the two disagree about which columns key the relationship.
fn align_with_the_filter<B: crate::backend::Backend, DB: DatabaseLike>(
    term: &CompiledTerm,
    table: TableId,
    mut movement: TermMovement,
    database: &DB,
) -> Result<TermMovement, RegisterError> {
    let refuse = |what: &str| {
        RegisterError::MembershipTermRefused(format!(
            "the relationship `rls2fga` compiled {what}, so a changed membership row could not \
             be matched against what this filter compares"
        ))
    };
    if movement.member_keys.len() != term.columns.len() {
        return Err(refuse(
            "is keyed on a different number of columns than this filter compares",
        ));
    }
    let Expr::Exists { subquery, .. } = &term.expr else {
        // The `IN` spelling compares one column, and a one-wide key needs no
        // reordering.
        return Ok(movement);
    };
    let parts =
        crate::compiler::sql_shape::membership_exists_parts::<B, DB>(subquery, table, database)
            .map_err(|_| refuse("was compiled from an EXISTS that lost its recognized shape"))?;
    if parts.member_table != movement.member_table {
        return Err(refuse(
            "reads a different table than the filter's own membership subquery",
        ));
    }
    let written: Vec<crate::ColumnId> = parts.pairs.iter().map(|pair| pair.inner).collect();
    // Set equality, not mere containment: `written` naming one membership
    // column twice while the shape keys on two would slip through a
    // containment check and store rows under a key no event ever builds.
    let matches_pairwise = written
        .iter()
        .all(|inner| movement.member_keys.contains(inner))
        && movement.member_keys.iter().all(|key| written.contains(key))
        && written
            .iter()
            .enumerate()
            .all(|(at, inner)| !written[..at].contains(inner));
    if !matches_pairwise {
        return Err(refuse(
            "is keyed on different membership columns than the filter's own pair equalities",
        ));
    }
    movement.member_keys = written;
    Ok(movement)
}

/// Resolve the one shape of `entry` that a changed row settles.
fn read_from_one_row<B: crate::backend::Backend, DB: DatabaseLike>(
    entry: &RelationShapes,
    database: &DB,
    rules: &AccessRules<'_, DB>,
) -> Result<TermMovement, RegisterError> {
    let refuse = |what: &str| {
        RegisterError::MembershipTermRefused(format!(
            "the records for '{relation}' {what}, so SubQL cannot tell from a changed row who \
             this filter admits",
            relation = entry.relation,
        ))
    };

    let [shape] = entry.shapes.as_slice() else {
        return Err(refuse("are stated by more than one query, or by none"));
    };
    let RecordDerivation::FromRow {
        table, template, ..
    } = &shape.derivation
    else {
        return Err(refuse("need more than one row"));
    };

    let table_id = rules
        .served_table(table)
        .ok_or_else(|| refuse("read a table the catalog does not know"))?;
    let member_keys = template
        .object_key
        .parts()
        .iter()
        .map(|part| column_of::<B, _>(part, database, table_id))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| refuse("name their object from something other than columns"))?;
    let subject = subject_column::<B, _>(&template.subject_key, database, table_id)
        .ok_or_else(|| refuse("name their subject from something other than one column"))?;

    Ok(TermMovement {
        member_table: table_id,
        member_keys,
        member_subject: subject,
    })
}

/// The column `key` names its subject from, when it names it from exactly one
/// scalar column of `table`.
///
/// A composite key names one object through several columns, and the subject a
/// term admits or moves is the value of a single column.
fn subject_column<B: crate::backend::Backend, DB: DatabaseLike>(
    key: &SubjectKey,
    database: &DB,
    table: TableId,
) -> Option<u16> {
    let mut parts = key.parts();
    match (parts.next(), parts.next()) {
        (Some(part), None) => column_of::<B, _>(part, database, table),
        _ => None,
    }
}

/// The column `source` reads, when it reads exactly one scalar column of
/// `table`.
///
/// A list column yields one record per element and a JSON path reads inside a
/// value, and neither is a column whose value a row's cell can be looked up by.
/// `ValueSource` is `#[non_exhaustive]`, so a shape a later rls2fga adds falls to
/// the wildcard and refuses rather than being read as a column.
fn column_of<B: crate::backend::Backend, DB: DatabaseLike>(
    source: &ValueSource,
    database: &DB,
    table: TableId,
) -> Option<u16> {
    match source {
        ValueSource::Column(name) => {
            catalog_helpers::contract_column_id::<B, _>(database, table, name.as_str())
        }
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::format;
    use alloc::vec;

    use rls2fga::term::TermShapes;
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::{ConfidenceLevel, RecordDerivation, SubjectKey, ValueSource};
    use sql_traits::structs::ParserDB;
    use sqlparser::ast::{Expr, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    use super::{caller_plan, member_columns};
    use crate::access_rules::{AccessRules, Translation};
    use crate::backend::Postgres;
    use crate::term::{CompiledTerm, TermCaller};
    use crate::testing::deployment;
    use crate::{catalog_helpers, RegisterError, TableId};

    const DDL: &str = "CREATE TABLE project_members(project_id INTEGER, user_id TEXT, \
                       PRIMARY KEY(project_id, user_id));
         CREATE TABLE notes(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER);";

    const CALLER: &str = "owner = current_setting('app.user_id', true)";

    const MEMBERSHIP: &str = "project_id IN (SELECT project_id FROM project_members \
                              WHERE user_id = current_setting('app.user_id', true))";

    fn notes(database: &ParserDB) -> TableId {
        catalog_helpers::table_id::<Postgres, _>(database, "notes").unwrap()
    }

    /// The term filtering `notes` on `filter`, comparing `column`.
    fn term(database: &ParserDB, filter: &str, column: &str) -> CompiledTerm {
        let sql = format!("SELECT * FROM notes WHERE {filter}");
        let Statement::Query(query) = Parser::parse_sql(&PostgreSqlDialect {}, &sql)
            .unwrap()
            .remove(0)
        else {
            unreachable!("a SELECT parses as a query");
        };
        let SetExpr::Select(select) = *query.body else {
            unreachable!("a plain SELECT has a select body");
        };
        let expr: Expr = select.selection.unwrap();
        CompiledTerm {
            slot: 0,
            columns: vec![catalog_helpers::contract_column_id::<Postgres, _>(
                database,
                notes(database),
                column,
            )
            .unwrap()],
            expr,
        }
    }

    fn shapes(database: &ParserDB, term: &CompiledTerm) -> TermShapes {
        let rules = AccessRules::Served(database);
        let translation = Translation::new(
            TranslatorBuilder::new(deployment::callers())
                .with_min_confidence(ConfidenceLevel::B)
                .build(),
            &rules,
        );
        rules
            .describe_membership_term(
                &term.expr,
                &translation.registry,
                "notes",
                translation.translator.min_confidence(),
                None,
            )
            .unwrap()
    }

    /// `shapes` with the one subject read from `first` joined to `rest`, so the
    /// first part stays the column the term compares or is keyed on.
    fn with_composite_subject(mut shapes: TermShapes, first: &str, rest: &str) -> TermShapes {
        let mut joined = 0;
        for shape in shapes
            .relations
            .iter_mut()
            .flat_map(|entry| &mut entry.shapes)
        {
            let RecordDerivation::FromRow { template, .. } = &mut shape.derivation else {
                continue;
            };
            let reads_first = {
                let mut parts = template.subject_key.parts();
                matches!(
                    (parts.next(), parts.next()),
                    (Some(ValueSource::Column(read)), None) if read.as_str() == first
                )
            };
            if reads_first {
                template.subject_key = SubjectKey::composite_sources(
                    ValueSource::column(first),
                    vec![ValueSource::column(rest)],
                );
                joined += 1;
            }
        }
        assert_eq!(joined, 1, "one shape names its subject from {first}");
        shapes
    }

    #[test]
    fn a_caller_comparison_refuses_a_composite_subject() {
        let database = deployment::db(DDL);
        let table = notes(&database);
        let rules = AccessRules::Served(&database);
        let term = term(&database, CALLER, "owner");
        let plan = |shapes: &TermShapes| {
            caller_plan::<Postgres, _>(
                &term,
                table,
                &database,
                &rules,
                shapes,
                TermCaller::Identity,
            )
        };
        let single = shapes(&database, &term);
        assert!(plan(&single).is_ok());
        let composite = with_composite_subject(single, "owner", "project_id");
        assert!(matches!(
            plan(&composite),
            Err(RegisterError::MembershipTermRefused(_))
        ));
    }

    #[test]
    fn a_membership_term_refuses_a_composite_member_subject() {
        let database = deployment::db(DDL);
        let table = notes(&database);
        let rules = AccessRules::Served(&database);
        let term = term(&database, MEMBERSHIP, "project_id");
        let single = shapes(&database, &term);
        assert!(member_columns::<Postgres, _>(&term, table, &single, &database, &rules).is_ok());
        let composite = with_composite_subject(single, "user_id", "project_id");
        assert!(matches!(
            member_columns::<Postgres, _>(&term, table, &composite, &database, &rules),
            Err(RegisterError::MembershipTermRefused(_))
        ));
    }
}
