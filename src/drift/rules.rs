//! One table's access rules as one comparable value.
//!
//! A table's rules are its owner and row-security flags, its policies,
//! every function those policies reach through their clauses and the
//! bodies they call, and its table and column grants. [`TableRules`] holds
//! them as sorted canonical facts, so two reads of the same live database
//! compare equal and a change to any of them compares unequal. A part the
//! database cannot answer is itself a fact, naming the error, so one
//! unreadable clause or body never hides the rest.
//!
//! The rules hold nothing of another table, so a load of the one table
//! answers the same rules as a load of every table. A table a reached
//! function reads carries its own rules.

use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use sql_traits::errors::LookupError;
use sql_traits::prelude::{
    ColumnGrantLike, ColumnLike, DatabaseLike, FunctionBody, FunctionLike, GrantLike, PolicyLike,
    TableLike,
};
use sql_traits::structs::PolicyExpression;

/// The canonical rules of one table, compared between two live reads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct TableRules {
    /// One line per fact, sorted and without duplicates.
    facts: Vec<String>,
}

impl TableRules {
    /// The rules of `table` in `database`.
    ///
    /// # Errors
    ///
    /// [`LookupError`] when `database` does not hold `table`.
    pub fn of<DB: DatabaseLike>(table: &DB::Table, database: &DB) -> Result<Self, LookupError> {
        let mut facts = Vec::new();
        facts.push(table_fact(table, database)?);
        let mut reached = BTreeSet::new();
        for policy in table.policies(database)? {
            facts.push(policy_fact(policy, database));
            match policy.using_functions(database) {
                Ok(functions) => reached.extend(functions),
                Err(error) => {
                    facts.push(format!("policy {:?} using calls: {error}", policy.name()));
                }
            }
            match policy.check_functions(database) {
                Ok(functions) => reached.extend(functions),
                Err(error) => {
                    facts.push(format!("policy {:?} check calls: {error}", policy.name()));
                }
            }
        }
        let mut unvisited: Vec<&DB::Function> = reached.iter().copied().collect();
        while let Some(function) = unvisited.pop() {
            match function.referenced_functions(database) {
                Ok(called) => {
                    for callee in called {
                        if reached.insert(callee) {
                            unvisited.push(callee);
                        }
                    }
                }
                Err(error) => facts.push(format!(
                    "function {} calls: {error}",
                    function.target_name()
                )),
            }
        }
        for function in reached {
            facts.push(function_fact(function, database));
        }
        for grant in table.grants(database)? {
            facts.push(format!("table grant {}", grant_fact(grant, database)));
        }
        for grant in database
            .column_grants()
            .filter(|grant| grant.table(database) == Some(table))
        {
            let mut columns: Vec<&str> = grant
                .columns(table, database)?
                .map(ColumnLike::column_name)
                .collect();
            columns.sort_unstable();
            facts.push(format!(
                "column grant ({}) {}",
                columns.join(", "),
                grant_fact(grant, database)
            ));
        }
        facts.sort_unstable();
        facts.dedup();
        Ok(Self { facts })
    }

    /// The canonical facts, in their sorted order.
    pub fn facts(&self) -> impl Iterator<Item = &str> {
        self.facts.iter().map(String::as_str)
    }
}

/// `table`'s owner and row-security flags.
fn table_fact<T: TableLike>(table: &T, database: &T::DB) -> Result<String, LookupError> {
    Ok(format!(
        "table owner {:?} row security {} forced {}",
        table.owner(database)?,
        table.has_row_level_security(database)?,
        table.has_forced_row_level_security(database)?,
    ))
}

/// Everything `policy` declares.
fn policy_fact<P: PolicyLike>(policy: &P, database: &P::DB) -> String {
    let mut roles: Vec<String> = policy.roles(database).map(ToString::to_string).collect();
    roles.sort_unstable();
    format!(
        "policy {:?} {} {} to ({}) using {} check {}",
        policy.name(),
        policy.command(),
        policy.policy_type(),
        roles.join(", "),
        clause(policy.using_clause(database)),
        clause(policy.check_clause(database)),
    )
}

/// A policy clause, stored or as written, whichever the database holds.
fn clause(read: Result<Option<PolicyExpression<'_>>, LookupError>) -> String {
    match read {
        Ok(Some(clause)) => format!("({})", clause.expression()),
        Ok(None) => String::from("none"),
        Err(error) => format!("unread: {error}"),
    }
}

/// Everything `function` declares that decides what a call of it answers
/// and as whom it runs.
fn function_fact<F: FunctionLike>(function: &F, database: &F::DB) -> String {
    let modes: Vec<String> = function
        .argument_modes()
        .map(|mode| mode.to_string())
        .collect();
    let arguments: Vec<_> = function.argument_type_names(database).collect();
    let settings: Vec<String> = function
        .configuration_parameters()
        .iter()
        .map(ToString::to_string)
        .collect();
    let owner = match function.owner(database) {
        Ok(owner) => format!("{owner:?}"),
        Err(error) => format!("unread: {error}"),
    };
    format!(
        "function {} ({}) modes ({}) returns {:?} set of rows {} language {:?} {} {} {} \
         leakproof {} settings ({}) owner {owner} body {}",
        function.target_name(),
        arguments.join(", "),
        modes.join(", "),
        function.return_type_name(database),
        function.returns_set(),
        function.stored_language(),
        function.security_mode(),
        function.volatility(),
        function.null_input_behavior(),
        function.is_leakproof(),
        settings.join(", "),
        body(function.written_body()),
    )
}

/// A function body as the database hands it over.
fn body(written: Option<FunctionBody<'_>>) -> String {
    match written {
        None => String::from("none"),
        Some(FunctionBody::Text(text)) => format!("text {text:?}"),
        Some(FunctionBody::BoundDefinition(text)) => format!("bound {text:?}"),
        Some(FunctionBody::Expression(expression)) => format!("expression ({expression})"),
        Some(FunctionBody::Return(expression)) => format!("return ({expression})"),
        Some(FunctionBody::Select(select)) => format!("select ({select})"),
        Some(FunctionBody::Statements(statements)) => {
            format!("statements ({})", joined(statements))
        }
        Some(FunctionBody::Atomic(statements)) => format!("atomic ({})", joined(statements)),
    }
}

/// `statements` printed one after another.
fn joined(statements: &[sqlparser::ast::Statement]) -> String {
    statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// The privileges, grantees, grant option and grantor of `grant`, each
/// list sorted.
fn grant_fact<G: GrantLike>(grant: &G, database: &G::DB) -> String {
    let mut privileges: Vec<String> = grant
        .privileges(database)
        .map(ToString::to_string)
        .collect();
    privileges.sort_unstable();
    let mut grantees: Vec<String> = grant.grantees(database).map(ToString::to_string).collect();
    grantees.sort_unstable();
    format!(
        "({}) all {} to ({}) grant option {} by {:?}",
        privileges.join(", "),
        grant.is_all_privileges(),
        grantees.join(", "),
        grant.with_grant_option(),
        grant.granted_by(database),
    )
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "The fixtures must be valid.")]
mod tests {
    use super::*;
    use sql_traits::structs::{IdentifierCase, ParserDB, TargetName};
    use sqlparser::dialect::PostgreSqlDialect;

    /// Two tables under row security, one policy reaching a helper through
    /// another, and an unrelated function.
    const BASE: &str = "
        CREATE ROLE app_user;
        CREATE TABLE members (doc_id INT, user_id TEXT);
        CREATE TABLE docs (id INT PRIMARY KEY, title TEXT);
        CREATE TABLE notes (id INT PRIMARY KEY);
        CREATE FUNCTION is_member(d INT) RETURNS BOOLEAN LANGUAGE sql
            AS $$ SELECT EXISTS (SELECT 1 FROM members WHERE doc_id = d) $$;
        CREATE FUNCTION can_read(d INT) RETURNS BOOLEAN LANGUAGE sql
            AS $$ SELECT is_member(d) $$;
        CREATE FUNCTION unrelated() RETURNS INT LANGUAGE sql AS $$ SELECT 1 $$;
        ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
        ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
        CREATE POLICY docs_read ON docs FOR SELECT TO app_user USING (can_read(id));
        CREATE POLICY notes_read ON notes FOR SELECT TO app_user USING (true);
        GRANT SELECT ON docs TO app_user;
    ";

    /// The rules of `table` in the database `ddl` declares.
    fn rules(ddl: &str, table: &str) -> TableRules {
        let database = ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap();
        let found = database
            .table_by_target(TargetName::new(table, false), IdentifierCase::AsWritten)
            .unwrap()
            .unwrap();
        TableRules::of(found, &database).unwrap()
    }

    /// `BASE` with `from` replaced by `to`, asserting the text held `from`.
    fn changed(from: &str, to: &str) -> String {
        assert!(BASE.contains(from), "the fixture lacks {from}");
        BASE.replace(from, to)
    }

    #[test]
    fn two_reads_of_one_database_compare_equal() {
        assert_eq!(rules(BASE, "docs"), rules(BASE, "docs"));
        assert!(rules(BASE, "docs").facts().count() > 0);
    }

    #[test]
    fn a_changed_policy_clause_changes_the_rules() {
        let changed = changed("USING (can_read(id))", "USING (can_read(id) AND id > 0)");
        assert_ne!(rules(BASE, "docs"), rules(&changed, "docs"));
    }

    #[test]
    fn a_changed_policy_role_changes_the_rules() {
        let changed = changed(
            "FOR SELECT TO app_user USING (can_read(id))",
            "FOR SELECT TO PUBLIC USING (can_read(id))",
        );
        assert_ne!(rules(BASE, "docs"), rules(&changed, "docs"));
    }

    #[test]
    fn a_helper_reached_through_another_helper_changes_the_rules() {
        let changed = changed(
            "WHERE doc_id = d) $$",
            "WHERE doc_id = d AND user_id IS NOT NULL) $$",
        );
        assert_ne!(rules(BASE, "docs"), rules(&changed, "docs"));
    }

    #[test]
    fn a_function_no_policy_reaches_leaves_the_rules() {
        let changed = changed("AS $$ SELECT 1 $$", "AS $$ SELECT 2 $$");
        assert_eq!(rules(BASE, "docs"), rules(&changed, "docs"));
    }

    #[test]
    fn a_table_grant_changes_only_its_tables_rules() {
        let changed = format!("{BASE} GRANT SELECT ON notes TO app_user;");
        assert_ne!(rules(BASE, "notes"), rules(&changed, "notes"));
        assert_eq!(rules(BASE, "docs"), rules(&changed, "docs"));
    }

    #[test]
    fn a_column_grant_changes_the_rules() {
        let changed = format!("{BASE} GRANT UPDATE (title) ON docs TO app_user;");
        assert_ne!(rules(BASE, "docs"), rules(&changed, "docs"));
    }

    #[test]
    fn disabling_row_security_changes_the_rules() {
        let changed = changed("ALTER TABLE docs ENABLE ROW LEVEL SECURITY;", "");
        assert_ne!(rules(BASE, "docs"), rules(&changed, "docs"));
    }
}
