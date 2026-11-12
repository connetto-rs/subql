//! The columns of each table a registered statement references, checked
//! against what PostgreSQL holds a reader with column grants to.

use rls2fga::translator::TranslatorBuilder;
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::{deployment, TestEvent};
use subql::{
    catalog_helpers, ColumnId, ColumnsRead, DefaultIds, SubscriptionEngine, SubscriptionRequest,
    TableId,
};

/// `docs (id, title, body, owner)` and `members (doc_id, user_id)`.
const TABLES: &str =
    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT, owner TEXT);
     CREATE TABLE members (doc_id INTEGER, user_id TEXT, PRIMARY KEY (doc_id, user_id));";

/// [`TABLES`] with a read rule on `docs` that reads `owner`.
const POLICY: &str = "ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
     CREATE POLICY docs_p ON docs FOR ALL TO PUBLIC USING (
       owner = current_setting('app.user_id', true));";

const ID: ColumnId = 0;
const TITLE: ColumnId = 1;
const BODY: ColumnId = 2;
const OWNER: ColumnId = 3;
const DOC_ID: ColumnId = 0;
const USER_ID: ColumnId = 1;

/// One statement and the columns it reads of `docs` and of `members`.
struct Case {
    sql: &'static str,
    docs: Option<&'static [ColumnId]>,
    members: Option<&'static [ColumnId]>,
}

/// A membership term, which registers only for a subscriber stating the
/// `docs.id` values it is a member of.
const MEMBERSHIP: &str = "SELECT id FROM docs WHERE EXISTS (SELECT 1 FROM members m \
     WHERE m.doc_id = docs.id AND m.user_id = current_setting('app.user_id', true))";

const CASES: [Case; 10] = [
    Case {
        sql: "SELECT id, title FROM docs",
        docs: Some(&[ID, TITLE]),
        members: None,
    },
    Case {
        sql: "SELECT id, title FROM docs WHERE body = 'secret'",
        docs: Some(&[ID, TITLE, BODY]),
        members: None,
    },
    Case {
        sql: "SELECT id FROM docs ORDER BY body",
        docs: Some(&[ID, BODY]),
        members: None,
    },
    Case {
        sql: "SELECT title AS body FROM docs ORDER BY body",
        docs: Some(&[TITLE]),
        members: None,
    },
    Case {
        sql: "SELECT 1 FROM docs",
        docs: Some(&[]),
        members: None,
    },
    Case {
        sql: "SELECT count(*) FROM docs",
        docs: Some(&[]),
        members: None,
    },
    Case {
        sql: "SELECT * FROM docs",
        docs: Some(&[ID, TITLE, BODY, OWNER]),
        members: None,
    },
    Case {
        sql: MEMBERSHIP,
        docs: Some(&[ID]),
        members: Some(&[DOC_ID, USER_ID]),
    },
    Case {
        sql: "SELECT d.id, m.user_id FROM docs d JOIN members m ON m.doc_id = d.id",
        docs: Some(&[ID]),
        members: Some(&[DOC_ID, USER_ID]),
    },
    // A set operation is a shape subql does not enumerate, so it reports no
    // set rather than the projection alone.
    Case {
        sql: "SELECT id, title FROM docs UNION SELECT id, title FROM docs WHERE body = 'secret'",
        docs: None,
        members: None,
    },
];

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn table(db: &ParserDB, name: &str) -> TableId {
    catalog_helpers::table_id::<Postgres, _>(db, name).expect("the table is in the catalog")
}

fn engine(db: ParserDB) -> Engine {
    SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(
        TranslatorBuilder::new(deployment::with_settings([deployment::set("app.subjects")]))
            .with_min_confidence(ConfidenceLevel::B)
            .build(),
    )
}

/// What registering `case` for alice on `engine` reports it reads.
fn registered(engine: &mut Engine, consumer: u64, case: &Case) -> ColumnsRead {
    let request = SubscriptionRequest::new(consumer, case.sql);
    let request = if case.sql == MEMBERSHIP {
        request
            .subscriber(Value::String("alice".into()))
            .term_values(
                vec!["id"],
                vec![(Value::String("alice".into()), vec![Value::Int(1)])],
            )
    } else {
        request
    };
    engine
        .register(request)
        .expect("the statement registers")
        .columns_read
}

#[test]
fn every_referenced_column_is_reported_under_its_own_table() {
    let db = deployment::db(TABLES);
    let (docs, members) = (table(&db, "docs"), table(&db, "members"));
    let mut engine = engine(db);
    for (consumer, case) in (1..).zip(&CASES) {
        let read = registered(&mut engine, consumer, case);
        assert_eq!(read.get(docs), case.docs, "docs columns of {}", case.sql);
        assert_eq!(
            read.get(members),
            case.members,
            "members columns of {}",
            case.sql
        );
    }
}

#[test]
fn a_using_column_is_charged_only_within_its_join_tree() {
    let db = deployment::db(&format!(
        "{TABLES}\nCREATE TABLE tags (doc_id INTEGER, label TEXT);"
    ));
    let (members, tags) = (table(&db, "members"), table(&db, "tags"));
    let read = engine(db)
        .register(SubscriptionRequest::new(
            1,
            "SELECT count(*) FROM tags, members a JOIN members b USING (doc_id)",
        ))
        .expect("the statement registers")
        .columns_read;
    assert_eq!(
        read.get(tags),
        Some(&[][..]),
        "tags is a separate FROM item"
    );
    assert_eq!(read.get(members), Some(&[DOC_ID][..]));
}

#[test]
fn a_column_only_the_read_rule_reads_is_not_reported() {
    let db = deployment::db(&format!("{TABLES}\n{POLICY}"));
    let docs = table(&db, "docs");
    let mut engine = engine(db);
    let registration = engine
        .register(
            SubscriptionRequest::new(1, "SELECT id, title FROM docs")
                .subscriber(Value::String("alice".into())),
        )
        .expect("the statement registers");
    assert!(
        registration
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the rule reading owner is in the registered predicate"
    );
    assert_eq!(registration.columns_read.get(docs), Some(&[ID, TITLE][..]));
}

/// Every case runs on PostgreSQL as a reader granted `SELECT (id, title)` on
/// `docs` and `SELECT (doc_id, user_id)` on `members`, under the read rule, and
/// PostgreSQL serves exactly the statements whose reported columns lie within
/// those grants.
#[test]
#[ignore = "requires docker"]
fn postgres_serves_exactly_the_reads_within_the_grants() {
    use diesel::connection::SimpleConnection;

    let db = crate::common::pg::pg_database();
    // Roles are cluster-wide, so the reader is named after this database.
    let role = format!("{}_reader", db.name());
    let mut pg = db.connect();
    pg.batch_execute(&format!(
        "{TABLES}
         {POLICY}
         CREATE ROLE {role};
         GRANT SELECT (id, title) ON docs TO {role};
         GRANT SELECT (doc_id, user_id) ON members TO {role};"
    ))
    .expect("create the tables, the policy and the reader");

    let catalog = deployment::db(TABLES);
    let (docs, members) = (table(&catalog, "docs"), table(&catalog, "members"));
    let mut engine = engine(catalog);
    let within = |read: Option<&[ColumnId]>, granted: &[ColumnId]| {
        read.is_some_and(|read| read.iter().all(|column| granted.contains(column)))
    };
    let mut answers = Vec::new();
    for (consumer, case) in (1..).zip(&CASES) {
        let read = registered(&mut engine, consumer, case);
        let reads_members = case.sql.contains("members");
        let granted = within(read.get(docs), &[ID, TITLE])
            && (!reads_members || within(read.get(members), &[DOC_ID, USER_ID]));
        let served = crate::common::pg::postgres_allows(&mut pg, &role, case.sql);
        answers.push((case.sql, granted, served));
    }
    pg.batch_execute(&format!("DROP TABLE docs, members; DROP ROLE {role}"))
        .expect("drop the tables and the reader");

    for (sql, granted, served) in answers {
        assert_eq!(
            granted, served,
            "{sql}: reported within the grants vs served"
        );
    }
}
