//! Registration scaffold shared by the Docker-backed re-execution suites.

use sql_traits::structs::ParserDB;
use subql::backend::CdcEvent;
use subql::compiler::SqlLiteralParse;
use subql::reexec::{AutoResolvingEngine, ResolverMode};
use subql::{DefaultIds, Registered, SubscriptionId, SubscriptionRequest, Tier};

/// Register `sql` for `consumer` and assert it landed on the scalar
/// re-execution tier.
///
/// Mode-independent: `register` is synchronous in both modes and every
/// connector in these suites uses `AuthContext = ()`, so `M` is passive.
pub fn register_captured<E, M>(
    engine: &mut AutoResolvingEngine<E, DefaultIds, ParserDB, M>,
    consumer: u64,
    sql: &str,
) -> SubscriptionId
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse + core::fmt::Debug,
    M: ResolverMode<E::Backend, AuthContext = ()>,
{
    match engine
        .register(SubscriptionRequest::new(consumer, sql), ())
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected the scalar re-execution tier for {sql}, got {other:?}"),
    }
}

/// Require each scalar read in `cases` to answer, through `scalar`, the value
/// `page` reads for the same cell, which the row path decodes from the type
/// the database reports for the column.
pub fn assert_scalars_match_rows<B, Error>(
    cases: &[(&str, subql::backend::ScalarFamily)],
    mut scalar: impl FnMut(
        &subql::reexec::ReadQuery<'_, B>,
        subql::backend::ScalarFamily,
    ) -> Result<subql::backend::Value<B>, Error>,
    mut page: impl FnMut(
        &subql::reexec::ReadQuery<'_, B>,
    ) -> Result<Vec<Vec<subql::backend::Value<B>>>, Error>,
) where
    B: subql::backend::Backend,
    subql::backend::Value<B>: PartialEq + core::fmt::Debug,
    Error: core::fmt::Debug,
{
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|&(sql, family)| {
            let query = subql::reexec::ReadQuery::without_binds(sql);
            let rows = page(&query).unwrap_or_else(|error| panic!("{sql} as a page: {error:?}"));
            assert_eq!(rows.len(), 1, "{sql} reads one row");
            assert_ne!(
                rows[0][0],
                subql::backend::Value::Null,
                "{sql} reads a cell"
            );
            match scalar(&query, family) {
                Ok(value) if value == rows[0][0] => None,
                answer => Some(format!(
                    "{sql} as {family:?}: scalar {answer:?}, row {:?}",
                    rows[0][0]
                )),
            }
        })
        .collect();
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}
