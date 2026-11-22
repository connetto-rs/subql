//! The access rules an engine judges reads by.
//!
//! An engine judges by its served catalog until a consumer hands it another
//! database to follow, typically the live database the drift guard read.
//! That database names its tables by its own ids, so each table is matched to
//! the served catalog's by its written schema and name, and every answer is
//! given under the served catalog's id.

use alloc::sync::Arc;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;

use crate::{catalog_helpers, CatalogError, TableId};

#[cfg(feature = "membership-term")]
use rls2fga::classifier::function_registry::FunctionRegistry;
#[cfg(feature = "membership-term")]
use rls2fga::term::{TermRefusal, TermShapes};
#[cfg(feature = "membership-term")]
use rls2fga::translator::Translator;
#[cfg(feature = "membership-term")]
use rls2fga_types::ConfidenceLevel;
#[cfg(feature = "membership-term")]
use sqlparser::ast::Expr;

/// Access rules read from a database other than the served catalog.
///
/// Object safe, so an engine holds rules of any database type without a type
/// parameter of its own.
pub trait FollowedRules: Send + Sync {
    /// Whether `table` has row-level security under these rules.
    fn table_has_rls(&self, table: TableId) -> Result<bool, CatalogError>;

    /// Each served table's complete public read rule, and the registry the
    /// rules' functions enrich.
    #[cfg(feature = "membership-term")]
    fn read_rules(
        &self,
        translator: &Translator,
    ) -> (HashMap<TableId, Arc<Expr>>, FunctionRegistry);

    /// [`rls2fga::term::describe_membership_term`] over these rules.
    #[cfg(feature = "membership-term")]
    fn describe_membership_term(
        &self,
        expr: &Expr,
        registry: &FunctionRegistry,
        guarded_table: &str,
        min_confidence: ConfidenceLevel,
        acting_role: Option<&str>,
    ) -> Result<TermShapes, TermRefusal>;

    /// The served id of the table a visibility contract names.
    #[cfg(feature = "membership-term")]
    fn served_table(&self, table: &rls2fga_types::TableId) -> Option<TableId>;
}

/// The rules of `database`, its tables matched to the served catalog's.
pub struct Followed<R> {
    database: Arc<R>,
    /// Each matched served table's id in `database`.
    rules_ids: HashMap<TableId, TableId>,
    /// The inverse of `rules_ids`.
    #[cfg(feature = "membership-term")]
    served_ids: HashMap<TableId, TableId>,
}

impl<R: DatabaseLike> Followed<R> {
    /// Match every table of `served` to the table of `database` its written
    /// name resolves to under `B`'s comparison.
    ///
    /// A served table `database` lacks, or names ambiguously, stays unmatched.
    pub fn new<B: crate::backend::Backend, DB: DatabaseLike>(
        served: &DB,
        database: Arc<R>,
    ) -> Self {
        let rules_ids: HashMap<TableId, TableId> = served
            .tables()
            .filter_map(|table| {
                let served_id = u32::try_from(served.table_id(table)?).ok()?;
                let found = database
                    .resolve_target_table(
                        catalog_helpers::written_target(table),
                        B::WRITTEN_TABLE_NAME_CASE,
                    )
                    .ok()??;
                let rules_id = u32::try_from(database.table_id(found)?).ok()?;
                Some((served_id, rules_id))
            })
            .collect();
        #[cfg(feature = "membership-term")]
        let served_ids = rules_ids
            .iter()
            .map(|(&served_id, &rules_id)| (rules_id, served_id))
            .collect();
        Self {
            database,
            rules_ids,
            #[cfg(feature = "membership-term")]
            served_ids,
        }
    }
}

impl<R: DatabaseLike> FollowedRules for Followed<R> {
    fn table_has_rls(&self, table: TableId) -> Result<bool, CatalogError> {
        let rules_table = self
            .rules_ids
            .get(&table)
            .and_then(|&rules_id| self.database.table_by_id(usize::try_from(rules_id).ok()?))
            .ok_or(CatalogError::UnknownTable(table))?;
        catalog_helpers::rls_of(&*self.database, rules_table, table)
    }

    #[cfg(feature = "membership-term")]
    fn read_rules(
        &self,
        translator: &Translator,
    ) -> (HashMap<TableId, Arc<Expr>>, FunctionRegistry) {
        crate::read_rule::read_rules(&*self.database, translator, |table| {
            self.served_ids.get(&table).copied()
        })
    }

    #[cfg(feature = "membership-term")]
    fn describe_membership_term(
        &self,
        expr: &Expr,
        registry: &FunctionRegistry,
        guarded_table: &str,
        min_confidence: ConfidenceLevel,
        acting_role: Option<&str>,
    ) -> Result<TermShapes, TermRefusal> {
        rls2fga::term::describe_membership_term(
            expr,
            &*self.database,
            registry,
            guarded_table,
            min_confidence,
            acting_role,
        )
    }

    #[cfg(feature = "membership-term")]
    fn served_table(&self, table: &rls2fga_types::TableId) -> Option<TableId> {
        let rules_id = catalog_helpers::contract_table_id(&*self.database, table)?;
        self.served_ids.get(&rules_id).copied()
    }
}

/// The rules an engine currently judges by.
pub enum AccessRules<'a, DB> {
    /// The served catalog's own.
    Served(&'a DB),
    /// Rules a consumer handed the engine to follow.
    Followed(&'a dyn FollowedRules),
}

impl<DB: DatabaseLike> AccessRules<'_, DB> {
    /// Whether `table` has row-level security.
    ///
    /// # Errors
    ///
    /// [`CatalogError::UnknownTable`] for a table the rules do not hold,
    /// [`CatalogError::Lookup`] when the rules cannot answer.
    pub fn table_has_rls(&self, table: TableId) -> Result<bool, CatalogError> {
        match *self {
            Self::Served(database) => catalog_helpers::table_has_rls(database, table),
            Self::Followed(rules) => rules.table_has_rls(table),
        }
    }

    /// Each table's complete public read rule under the served catalog's ids,
    /// and the registry the rules' functions enrich.
    #[cfg(feature = "membership-term")]
    pub fn read_rules(
        &self,
        translator: &Translator,
    ) -> (HashMap<TableId, Arc<Expr>>, FunctionRegistry) {
        match *self {
            Self::Served(database) => crate::read_rule::read_rules(database, translator, Some),
            Self::Followed(rules) => rules.read_rules(translator),
        }
    }

    /// [`rls2fga::term::describe_membership_term`] over these rules.
    #[cfg(feature = "membership-term")]
    pub fn describe_membership_term(
        &self,
        expr: &Expr,
        registry: &FunctionRegistry,
        guarded_table: &str,
        min_confidence: ConfidenceLevel,
        acting_role: Option<&str>,
    ) -> Result<TermShapes, TermRefusal> {
        match *self {
            Self::Served(database) => rls2fga::term::describe_membership_term(
                expr,
                database,
                registry,
                guarded_table,
                min_confidence,
                acting_role,
            ),
            Self::Followed(rules) => rules.describe_membership_term(
                expr,
                registry,
                guarded_table,
                min_confidence,
                acting_role,
            ),
        }
    }

    /// The served id of the table a visibility contract names.
    #[cfg(feature = "membership-term")]
    pub fn served_table(&self, table: &rls2fga_types::TableId) -> Option<TableId> {
        match *self {
            Self::Served(database) => catalog_helpers::contract_table_id(database, table),
            Self::Followed(rules) => rules.served_table(table),
        }
    }
}

/// The translator a membership term and a read rule are judged by, with
/// what it derives from the rules the engine follows.
#[cfg(feature = "membership-term")]
pub struct Translation {
    pub translator: Translator,
    /// Each table's complete public read rule, under the served catalog's ids.
    pub read_rules: HashMap<TableId, Arc<Expr>>,
    /// The translator's registry enriched with the functions the rules define.
    pub registry: FunctionRegistry,
}

#[cfg(feature = "membership-term")]
impl Translation {
    /// Derive `translator`'s read rules and registry from `rules`.
    pub fn new<DB: DatabaseLike>(translator: Translator, rules: &AccessRules<'_, DB>) -> Self {
        let (read_rules, registry) = rules.read_rules(&translator);
        Self {
            translator,
            read_rules,
            registry,
        }
    }
}
