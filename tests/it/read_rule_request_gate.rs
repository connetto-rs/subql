//! A read rule carrying a clause that reads only the request, settled from the
//! values the registration states.

use rls2fga::translator::TranslatorBuilder;
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::{deployment, TestEvent};
use subql::{
    catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, TableId,
};

const CAPPED: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (
       owner = current_setting('app.user_id', true)
       OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
     CREATE POLICY items_cap_read ON items AS RESTRICTIVE FOR SELECT USING (
       'items:read' = ANY(string_to_array(current_setting('app.bot_list', true), ','))
       OR '*' = ANY(string_to_array(current_setting('app.bot_list', true), ',')));";

const MODED: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (
       owner = current_setting('app.user_id', true)
       OR (owner = 'shared' AND current_setting('app.mode', true) = 'main'));";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine_with(ddl: &str) -> (Engine, TableId) {
    let db = deployment::db(ddl);
    let items = catalog_helpers::table_id::<Postgres, _>(&db, "items").expect("items exists");
    let engine = SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(
        TranslatorBuilder::new(deployment::with_settings([
            deployment::set("app.subjects"),
            deployment::set("app.bot_list"),
            deployment::scalar("app.mode"),
        ]))
        .with_min_confidence(ConfidenceLevel::B)
        .build(),
    );
    (engine, items)
}

fn alice(consumer: u64) -> SubscriptionRequest<DefaultIds, Postgres> {
    SubscriptionRequest::new(consumer, "SELECT * FROM items")
        .subscriber(Value::String("alice".into()))
        .subjects([Value::String("team:a".into())])
}

/// Register `request` and answer whether its read rule folded.
fn folded(engine: &mut Engine, request: SubscriptionRequest<DefaultIds, Postgres>) -> bool {
    engine
        .register(request)
        .expect("registers")
        .served()
        .expect("served in process")
        .read_rule_folded
}

/// The consumers an insert of an `items` row owned by `owner` reaches.
fn reached(engine: &mut Engine, items: TableId, id: i64, owner: &str) -> Vec<u64> {
    engine
        .consumers(&TestEvent::insert(
            items,
            vec![Value::Int(id), Value::String(owner.into())],
        ))
        .expect("dispatch runs")
        .inserted()
        .to_vec()
}

/// PostgreSQL reads an unset or empty `app.bot_list` as no element, which the
/// cap's `= ANY` admits nothing over.
#[test]
fn a_stated_bot_list_settles_the_cap_as_postgres_reads_it() {
    let cases: [(&[&str], &[u64]); 4] = [
        (&["*"], &[1]),
        (&["items:read"], &[1]),
        (&["notes:read"], &[]),
        (&[], &[]),
    ];
    for (bot_list, admitted) in cases {
        let (mut engine, items) = engine_with(CAPPED);
        assert!(
            folded(
                &mut engine,
                alice(1).request_value("app_bot_list", bot_list.iter().copied())
            ),
            "a stated bot list {bot_list:?} settles the cap"
        );
        assert_eq!(
            reached(&mut engine, items, 1, "alice"),
            admitted,
            "{bot_list:?}"
        );
        assert_eq!(
            reached(&mut engine, items, 2, "team:a"),
            admitted,
            "{bot_list:?}"
        );
        assert!(
            reached(&mut engine, items, 3, "carol").is_empty(),
            "{bot_list:?}"
        );
    }
}

#[test]
fn callers_the_cap_settles_apart_keep_apart_predicates() {
    let (mut engine, items) = engine_with(CAPPED);
    assert!(folded(
        &mut engine,
        alice(1).request_value("app_bot_list", ["*"])
    ));
    assert!(folded(
        &mut engine,
        SubscriptionRequest::new(2, "SELECT * FROM items")
            .subscriber(Value::String("bob".into()))
            .request_value("app_bot_list", ["notes:read"])
    ));

    assert_eq!(reached(&mut engine, items, 1, "alice"), [1]);
    assert!(
        reached(&mut engine, items, 2, "bob").is_empty(),
        "bob's bot list holds no read on items"
    );
}

#[test]
fn an_unstated_bot_list_keeps_query_only_matching() {
    let (mut engine, _) = engine_with(CAPPED);
    assert!(
        !folded(&mut engine, alice(1)),
        "the request says nothing the cap reads, so the rule stays out"
    );
}

#[test]
fn a_stated_value_holding_the_separator_is_refused() {
    let (mut engine, _) = engine_with(CAPPED);
    let refused = engine
        .register(alice(1).request_value("app_bot_list", ["items:read,notes:read"]))
        .expect_err("PostgreSQL would read that value as two elements");
    assert!(
        matches!(refused, RegisterError::RequestValueRefused(_)),
        "{refused:?}"
    );
}

#[test]
fn a_request_scalar_inside_a_permissive_arm_settles_too() {
    for (mode, admitted) in [("main", &[1u64][..]), ("draft", &[])] {
        let (mut engine, items) = engine_with(MODED);
        assert!(
            folded(&mut engine, alice(1).request_value("app_mode", [mode])),
            "a stated mode {mode} settles the arm"
        );
        assert_eq!(reached(&mut engine, items, 1, "alice"), [1], "{mode}");
        assert_eq!(reached(&mut engine, items, 2, "shared"), admitted, "{mode}");
    }
}
