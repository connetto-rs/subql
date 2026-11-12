//! The delegated half, against a real OpenFGA server.
//!
//! Everything else about row visibility is proven against a counting stand-in,
//! which is why the delegate went unwritten while every other criterion passed.
//! This is the criterion that was missing: a question the changed row does not
//! settle is answered by the authorization service, and the answer is right.
//!
//! The fixture is a tuple-to-userset, `can_select: member from teams`, which no
//! single row can ever decide. So the local half provably cannot answer it and
//! every verdict here came back over the wire.
//!
//! Run with:
//!   cargo +1.95 test --release --test it visibility_openfga_e2e:: \
//!       --features "visibility-openfga testing" -- --ignored --nocapture

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openfga_client::client::{
    AuthorizationModel as InstalledModel, CheckRequest, CheckRequestTupleKey,
    ConsistencyPreference, CreateStoreRequest, ListStoresRequest, OpenFgaServiceClient,
    ReadRequest, ReadRequestTupleKey, TupleKey, WriteRequest, WriteRequestWrites,
};
use openfga_client::tonic::transport::Channel;
use rls2fga::generator::json_model::{AuthorizationModel, Userset};
use rls2fga::generator::well_known::WellKnownTypes;
use rls2fga::translator::{Outputs, TranslatorBuilder};
use rls2fga::types::ActionStatement;
use rls2fga::types::ConfidenceLevel;
use rls2fga::types::{
    ConditionName, Record, RecordContextValue, RecordDerivation, RecordDescription, RelationName,
};
use sql_traits::structs::{Deployment, SessionSetting, SettingValue};
use sqlparser::ast::DataType;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::actor::Actor;
use subql::testing::deployment;
use subql::testing::relation_pool::{relation_fed_by, RelationPool};
use subql::testing::TestEvent;
use subql::visibility::openfga::{OpenFgaPolicy, ReconcileError, Reconciled};
use subql::visibility::policy::{RequestValues, RowPolicy, Subject};
use subql::visibility::shapes::Shapes;
use subql::visibility::store::{Enumeration, Region, Replay, Replayer, Requery, StoreDiff};
use subql::visibility::{EventRow, RowWrite, Verdict, VisibilityPolicy};
use subql::{catalog_helpers, ParserDB, TableId};

/// Everything a policy over one schema needs, from one translation.
///
/// Every test here repeats this, and one of them outgrew the line limit doing
/// so. Sharing it also means each test reads one translation, which is what says
/// the recipes, the naming, the answers and the model cannot disagree.
struct Wiring {
    db: ParserDB,
    outputs: Outputs,
    model: AuthorizationModel,
    /// Two membership sources restated onto one relation, in the shapes and the model alike.
    pool: Option<RelationPool>,
}

/// The request-scoped values are declared for every schema here, because
/// without them rls2fga refuses a held-keys arm rather than modelling it, and a
/// schema that has no such arm is unaffected by the declaration.
fn wiring(sql: &str) -> Wiring {
    let db = deployment::db(sql);
    let outputs = TranslatorBuilder::new(deployment::with_settings([
        deployment::set("app.subjects"),
        deployment::set("app.bot_list"),
    ]))
    .with_min_confidence(ConfidenceLevel::B)
    .build()
    .translate(&db)
    .expect("the visibility schema translates")
    .outputs_accepting_gaps();
    let model = outputs.json_model();
    Wiring {
        db,
        outputs,
        model,
        pool: None,
    }
}

impl Wiring {
    /// The relation rows of `table` feed, by the name the translation gave it.
    fn fed_by(&self, table: &str) -> RelationName {
        relation_fed_by(self.outputs.translation().relations(), table)
    }

    /// The relation `from`'s rows feed restated as the one `onto`'s rows feed, so the two
    /// producers share a region on a real server, which rls2fga never emits on its own.
    fn pooled(mut self, from: &str, onto: &str) -> Self {
        let pool = RelationPool::between(self.outputs.translation().relations(), from, onto);
        pool_model(&mut self.model, &pool);
        self.pool = Some(pool);
        self
    }

    /// The index every reader shares.
    fn shapes(self) -> Arc<Shapes<ParserDB>> {
        let Self {
            db, outputs, pool, ..
        } = self;
        let translation = outputs.translation();
        // A skipped query carries no description, so `filter_map` drops exactly
        // the entries that enumerate nothing.
        let queries: Vec<(Cow<'_, RecordDescription>, Cow<'_, str>, Option<&str>)> = outputs
            .tuple_queries()
            .iter()
            .filter_map(|query| {
                query.description.as_ref().map(|description| {
                    let condition = query.condition.as_ref().map(ConditionName::as_str);
                    pool.as_ref().map_or_else(
                        || {
                            (
                                Cow::Borrowed(description),
                                Cow::Borrowed(query.sql.as_str()),
                                condition,
                            )
                        },
                        |pool| {
                            (
                                Cow::Owned(pool.description(description)),
                                Cow::Owned(pool.sql(&query.sql)),
                                condition,
                            )
                        },
                    )
                })
            })
            .collect();
        let enumerations: Vec<Enumeration<'_>> = queries
            .iter()
            .map(|(description, sql, condition)| Enumeration {
                description,
                sql,
                condition: *condition,
            })
            .collect();
        let relations = pool.as_ref().map_or_else(
            || Cow::Borrowed(translation.relations()),
            |pool| Cow::Owned(pool.relations(translation.relations())),
        );
        Arc::new(
            Shapes::new::<Postgres>(db, &relations, &enumerations)
                .with_row_naming(translation.row_naming())
                .with_action_relations(translation.action_relations())
                .with_required_parameters(translation.notes()),
        )
    }
}

/// `model` with `pool`'s restated relation dropped, its user types added to the one it pooled
/// into, and every rewrite that read it reading that one instead.
fn pool_model(model: &mut AuthorizationModel, pool: &RelationPool) {
    for definition in &mut model.type_definitions {
        if let Some(relations) = definition.relations.as_mut() {
            relations.remove(pool.from());
            for userset in relations.values_mut() {
                restate(userset, pool);
            }
        }
        if let Some(metadata) = definition.metadata.as_mut() {
            if let Some(dropped) = metadata.relations.remove(pool.from()) {
                metadata
                    .relations
                    .get_mut(pool.onto())
                    .expect("both sources feed one type")
                    .directly_related_user_types
                    .extend(dropped.directly_related_user_types);
            }
        }
    }
}

fn restate(userset: &mut Userset, pool: &RelationPool) {
    let rename = |relation: &mut RelationName| {
        if relation == pool.from() {
            *relation = pool.onto().clone();
        }
    };
    match userset {
        Userset::This { .. } => {}
        Userset::ComputedUserset { computed_userset } => rename(&mut computed_userset.relation),
        Userset::TupleToUserset { tuple_to_userset } => {
            rename(&mut tuple_to_userset.tupleset.relation);
            rename(&mut tuple_to_userset.computed_userset.relation);
        }
        Userset::Union { union } => union
            .child
            .iter_mut()
            .for_each(|child| restate(child, pool)),
        Userset::Intersection { intersection } => intersection
            .child
            .iter_mut()
            .for_each(|child| restate(child, pool)),
        Userset::Difference { difference } => {
            restate(&mut difference.base, pool);
            restate(&mut difference.subtract, pool);
        }
    }
}

/// Write the model rls2fga emitted and return the id the server stored it under.
///
/// rls2fga's own writer, rather than a conversion spelled here: the model and the
/// request are different Rust types over one wire format, and the crate that
/// emits the model is where knowing that belongs.
async fn write_model(
    client: &mut OpenFgaServiceClient<Channel>,
    store_id: &str,
    model: &rls2fga::generator::json_model::AuthorizationModel,
) -> String {
    rls2fga::client::write_authorization_model(client, store_id, model)
        .await
        .expect("write the model")
}

async fn create_store_with_model(
    client: &mut OpenFgaServiceClient<Channel>,
    name: &str,
    model: &rls2fga::generator::json_model::AuthorizationModel,
) -> (String, String) {
    let store = client
        .create_store(CreateStoreRequest {
            name: name.to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(client, &store, model).await;
    (store, model_id)
}

/// The model the server stored under `model_id`, in the client's own type.
fn installed_model(model: &AuthorizationModel, model_id: &str) -> InstalledModel {
    let wire = serde_json::to_value(model).expect("the generated model serializes");
    let mut installed =
        serde_json::from_value::<InstalledModel>(wire).expect("the client reads the wire form");
    model_id.clone_into(&mut installed.id);
    installed
}

/// `can_select: member from teams`, which one row never decides: whether a
/// watcher is a member of the team the row names is not in the row.
const SCHEMA: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          PRIMARY KEY(team_id, user_id));
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_setting('app.user_id', true)));
";

const UPDATE_TWO_SIDED: &str = "
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON docs FOR SELECT USING (owner_id = current_setting('app.user_id', true));
CREATE POLICY pu ON docs FOR UPDATE
  USING (owner_id = current_setting('app.user_id', true)) WITH CHECK (editor_id = current_setting('app.user_id', true));
";

const WRITE_LIMIT: usize = 100;

fn update_row(owner: &str, editor: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(4),
        Value::String(owner.into()),
        Value::String(editor.into()),
    ]
}

type ReplacementPolicy = OpenFgaPolicy<ParserDB, Channel, Actor, Postgres>;
type ReplacementRow<'a> = EventRow<'a, TestEvent<Postgres>, ParserDB>;

async fn replacement_verdict(
    policy: &ReplacementPolicy,
    old: &ReplacementRow<'_>,
    new: &ReplacementRow<'_>,
    watcher: &Actor,
) -> Verdict {
    const fn assert_send<T: Send>(_: &T) {}

    let answer = policy.may_write(
        RowWrite::Update {
            old,
            new,
            assigned: &[],
            read: &[],
        },
        watcher,
    );
    assert_send(&answer);
    answer.await.expect("the service answered both judges")
}

async fn check_without_context(
    client: &mut OpenFgaServiceClient<Channel>,
    store: &str,
    model_id: &str,
    tuple_key: CheckRequestTupleKey,
) -> bool {
    client
        .check(CheckRequest {
            store_id: store.to_owned(),
            tuple_key: Some(tuple_key),
            contextual_tuples: None,
            authorization_model_id: model_id.to_owned(),
            trace: false,
            context: None,
            // Discriminant extraction: the generated field is a bare `i32`.
            consistency: ConsistencyPreference::HigherConsistency as i32,
        })
        .await
        .expect("ask without contextual tuples")
        .into_inner()
        .allowed
}

struct ReplacementFixture {
    client: OpenFgaServiceClient<Channel>,
    policy: ReplacementPolicy,
    shapes: Arc<Shapes<ParserDB>>,
    store: String,
    model_id: String,
    docs: TableId,
}

async fn replacement_fixture() -> ReplacementFixture {
    let mut client = openfga().await;
    let wired = wiring(UPDATE_TWO_SIDED);
    let docs = catalog_helpers::table_id::<Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let constants = constants(&wired.outputs);
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-replacement-write", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();
    let policy = OpenFgaPolicy::<_, _, Actor, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries both update judges");
    let stored =
        TestEvent::<Postgres>::insert(docs, update_row("alice", "bob")).with_pk_columns([0u16]);
    let (stored_diff, _requeries) = shapes
        .diff(&stored)
        .expect("the stored row states its facts");
    policy
        .apply(&stored_diff)
        .await
        .expect("write the stored row facts");
    policy
        .write_records(&constants)
        .await
        .expect("write the acting constant");

    ReplacementFixture {
        client,
        policy,
        shapes,
        store,
        model_id,
        docs,
    }
}

async fn assert_independent_replacement_judges(fixture: &ReplacementFixture) {
    let split = TestEvent::<Postgres>::update(
        fixture.docs,
        update_row("alice", "bob"),
        update_row("alice", "bob"),
    )
    .with_pk_columns([0u16]);
    let old = EventRow::previous(&split, fixture.shapes.catalog()).expect("pre-image");
    let new = EventRow::current(&split, fixture.shapes.catalog()).expect("post-image");
    let alice = Actor::app_user("user:alice");
    let bob = Actor::app_user("user:bob");

    assert_eq!(
        replacement_verdict(&fixture.policy, &old, &new, &bob).await,
        Verdict::Deny
    );
    assert_eq!(
        replacement_verdict(&fixture.policy, &old, &new, &alice).await,
        Verdict::Deny
    );
    assert_eq!(
        fixture
            .policy
            .may_write(
                RowWrite::UpdateUsing {
                    old: &old,
                    read: &[]
                },
                &alice
            )
            .await
            .expect("ask the existing-row relation"),
        Verdict::Allow
    );
    assert_eq!(
        fixture
            .policy
            .may_write(
                RowWrite::UpdateUsing {
                    old: &old,
                    read: &[]
                },
                &bob
            )
            .await
            .expect("ask the existing-row relation"),
        Verdict::Deny
    );
}

struct Aliases(&'static [&'static str]);

impl Subject for Aliases {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        self.0.iter().copied().map(Cow::Borrowed)
    }

    fn acting_role(&self) -> Option<&str> {
        Some(deployment::ROLE)
    }
}

async fn write_subject_fanout_facts(
    client: &mut OpenFgaServiceClient<Channel>,
    store: &str,
    model_id: &str,
    member: &RelationName,
    docs: &[Record],
) {
    let mut tuples = tuples_of(docs);
    tuples.push(TupleKey {
        user: "user:alice-secondary".to_owned(),
        relation: member.to_string(),
        object: "teams:1".to_owned(),
        condition: None,
    });
    tuples.push(TupleKey {
        user: "user:bob-secondary".to_owned(),
        relation: member.to_string(),
        object: "teams:2".to_owned(),
        condition: None,
    });
    client
        .write(WriteRequest {
            store_id: store.to_owned(),
            writes: Some(WriteRequestWrites {
                tuple_keys: tuples,
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: model_id.to_owned(),
        })
        .await
        .expect("write the subject grants");
}

/// A client on this run's shared OpenFGA whose first call has already come
/// back.
///
/// The shared-server acquisition is blocking and this test is driven by a
/// runtime, so it runs on the blocking pool rather than parking the runtime.
async fn openfga() -> OpenFgaServiceClient<Channel> {
    let port = tokio::task::spawn_blocking(crate::common::openfga_port)
        .await
        .expect("acquire the shared openfga");
    serving_client(port).await
}

/// A client whose first call has already come back, or a panic carrying the
/// last refusal rather than an unexplained reset inside a test body.
async fn serving_client(port: u16) -> OpenFgaServiceClient<Channel> {
    let endpoint = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = "never attempted".to_owned();
    while Instant::now() < deadline {
        match OpenFgaServiceClient::connect(endpoint.clone()).await {
            Ok(mut client) => match client.list_stores(ListStoresRequest::default()).await {
                Ok(_) => return client,
                Err(status) => last = status.to_string(),
            },
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("openfga on {endpoint} never answered a call: {last}");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_question_the_row_does_not_settle_is_answered_by_the_service() {
    let mut client = openfga().await;

    // What rls2fga makes of the schema: the model, the descriptions subql reads,
    // and which relations answer which statement. All from one translation, so
    // they cannot disagree.
    let wired = wiring(SCHEMA);
    let docs = catalog_helpers::table_id::<subql::backend::Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let member = wired.fed_by("team_members");
    let constants = constants(&wired.outputs);

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;

    let model_id = write_model(&mut client, &store, &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();

    // The facts the loader would have written: doc 4 belongs to team 1, and
    // alice is a member of it. bob is a member of nothing.
    let facts = tuples_of(&row_facts(
        &shapes,
        docs,
        vec![Value::Int(4), Value::Int(1)],
    ));
    let mut tuples = vec![TupleKey {
        user: "user:alice".to_owned(),
        relation: member.to_string(),
        object: "teams:1".to_owned(),
        condition: None,
    }];
    tuples.extend(facts);
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: tuples,
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: model_id.clone(),
        })
        .await
        .expect("write the tuples");

    let backend = OpenFgaPolicy::<_, _, Actor, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");
    backend
        .write_records(&constants)
        .await
        .expect("write the acting constant");
    let policy = RowPolicy::new(Arc::clone(&shapes), backend);

    assert!(
        !policy.answers_locally(docs, ActionStatement::Select),
        "a tuple-to-userset is never decidable from one row, so every answer \
         below had to come from the service"
    );

    let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(1)])
        .with_pk_columns([0u16]);
    let row = EventRow::current(&event, policy.catalog()).expect("an insert carries a post-image");

    let watchers = [Actor::app_user("user:alice"), Actor::app_user("user:bob")];
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    policy
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");

    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "alice is a member of the team the row names and bob is not"
    );
}

/// A replacement requires both row versions and the resulting row's facts.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_replacement_requires_both_versions_and_resulting_context() {
    let mut fixture = replacement_fixture().await;
    let replacement = TestEvent::<Postgres>::update(
        fixture.docs,
        update_row("alice", "bob"),
        update_row("alice", "alice"),
    )
    .with_pk_columns([0u16]);
    let old = EventRow::previous(&replacement, fixture.shapes.catalog()).expect("pre-image");
    let new = EventRow::current(&replacement, fixture.shapes.catalog()).expect("post-image");
    let alice = Actor::app_user("user:alice");

    assert_eq!(
        replacement_verdict(&fixture.policy, &old, &new, &alice).await,
        Verdict::Allow
    );
    let without_context = check_without_context(
        &mut fixture.client,
        &fixture.store,
        &fixture.model_id,
        CheckRequestTupleKey {
            user: alice.name,
            relation: "can_update_check".to_owned(),
            object: "docs:4".to_owned(),
        },
    )
    .await;
    assert!(
        !without_context,
        "alice is not the stored editor, so the resulting facts are required"
    );

    assert_independent_replacement_judges(&fixture).await;
}

/// Any subject name may grant its watcher without changing another watcher.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_second_subject_name_grants_only_its_watcher() {
    let mut client = openfga().await;
    let wired = wiring(SCHEMA);
    let docs = catalog_helpers::table_id::<Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let member = wired.fed_by("team_members");
    let constants = constants(&wired.outputs);
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-subject-fanout", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();
    write_subject_fanout_facts(
        &mut client,
        &store,
        &model_id,
        &member,
        &row_facts(&shapes, docs, vec![Value::Int(4), Value::Int(1)]),
    )
    .await;
    let policy = OpenFgaPolicy::<_, _, Aliases, Postgres>::new(
        Arc::clone(&shapes),
        client,
        store,
        &installed,
    )
    .expect("the index carries the read relation");
    policy
        .write_records(&constants)
        .await
        .expect("write the acting constant");
    let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(1)])
        .with_pk_columns([0u16]);
    let row = EventRow::current(&event, shapes.catalog()).expect("post-image");
    let watchers = [
        Aliases(&["user:alice-primary", "user:alice-secondary"]),
        Aliases(&["user:bob-primary", "user:bob-secondary"]),
    ];
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());

    policy
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered every subject");

    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "Alice's second name grants only Alice and Bob's decoy names another team"
    );
}

/// Direct writes are idempotent and preserve the chunk beyond the service limit.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn direct_writes_preserve_the_final_chunk() {
    let mut client = openfga().await;
    let wired = wiring(SCHEMA);
    let member = wired.fed_by("team_members");
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-direct-write-limit", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();
    let policy = OpenFgaPolicy::<_, _, String, Postgres>::new(
        shapes,
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries the model");
    let mut expected: Vec<String> = (0..=WRITE_LIMIT)
        .map(|nth| format!("user:{nth:03}"))
        .collect();
    let records: Vec<Record> = expected
        .iter()
        .map(|subject| plain_membership(&member, "teams:3", subject))
        .collect();

    policy
        .write_records(&records)
        .await
        .expect("write every chunk");
    policy
        .write_records(&records)
        .await
        .expect("retry every chunk");

    let mut stored = stored_members(&mut client, &store, &member).await;
    expected.sort();
    stored.sort();
    assert_eq!(stored, expected);
    assert!(stored.iter().any(|subject| subject == "user:100"));
}

/// A batch larger than the configured cap is split, and every watcher still gets
/// its own answer in its own place.
///
/// The audience is deliberately larger than OpenFGA's own default limit of 50
/// questions per call, so the server itself refuses an unsplit batch. That is
/// what makes this a test of the splitting rather than of the answers: without
/// the chunking the call comes back rejected, not merely slower.
///
/// The cap is a setting because the server's limit is not discoverable through
/// any of its calls, so the two ends are configured to agree.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_batch_over_the_cap_is_split_and_stays_positional() {
    let mut client = openfga().await;

    let wired = wiring(SCHEMA);
    let docs = catalog_helpers::table_id::<subql::backend::Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let member = wired.fed_by("team_members");
    let constants = constants(&wired.outputs);

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility-cap".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();

    // Doc 4 belongs to team 1, and every third watcher is a member of it.
    let audience: Vec<Actor> = (0..120)
        .map(|i| Actor::app_user(format!("user:u{i}")))
        .collect();
    let mut tuples = tuples_of(&row_facts(
        &shapes,
        docs,
        vec![Value::Int(4), Value::Int(1)],
    ));
    tuples.extend(audience.iter().step_by(3).map(|actor| TupleKey {
        user: actor.name.clone(),
        relation: member.to_string(),
        object: "teams:1".to_owned(),
        condition: None,
    }));
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: tuples,
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: model_id.clone(),
        })
        .await
        .expect("write the tuples");

    let backend = OpenFgaPolicy::<_, _, Actor, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need")
    .max_checks_per_batch(20);
    backend
        .write_records(&constants)
        .await
        .expect("write the acting constant");
    let policy = RowPolicy::new(Arc::clone(&shapes), backend);

    let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(1)])
        .with_pk_columns([0u16]);
    let row = EventRow::current(&event, policy.catalog()).expect("an insert carries a post-image");

    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, audience.len());
    policy
        .may_see(&row, &audience, &mut verdicts)
        .await
        .expect("the service answered every chunk");

    let expected: Vec<Verdict> = (0..audience.len())
        .map(|i| {
            if i % 3 == 0 {
                Verdict::Allow
            } else {
                Verdict::Deny
            }
        })
        .collect();
    assert_eq!(
        verdicts, expected,
        "120 questions at a cap of 20 is six calls, none of them over the \
         server's own limit, and each answer landed on the watcher it was \
         asked about"
    );
}

/// Requirement 6, closed against the service: the difference one changed row
/// reports is written through the client, and the next question answers from it.
///
/// This is the ordering the design put in subql rather than in the caller. Only
/// subql sees both the change stream and the questions, so only subql can write
/// before it answers. A store written by one component and read by another has
/// no ordering, and a question landing in the gap answers from facts that have
/// already moved.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_row_changing_hands_moves_the_store_and_the_next_answer() {
    // Ownership, which one row does decide, so the local half would answer it.
    // The point here is the store rather than the answering, so the questions go
    // to the service directly.
    const OWNED: &str = "
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, owner_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_setting('app.user_id', true));
";

    let mut client = openfga().await;

    let wired = wiring(OWNED);
    let docs = catalog_helpers::table_id::<subql::backend::Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility-store".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let constants = constants(&wired.outputs);
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, Actor, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");
    backend
        .write_records(&constants)
        .await
        .expect("write the acting constant");

    // Row 4 is created owned by alice.
    let created =
        TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::String("alice".into())])
            .with_pk_columns([0u16]);
    let (created_diff, _requeries) = shapes.diff(&created).expect("an insert is all additions");
    backend
        .apply(&created_diff)
        .await
        .expect("write the additions");

    let watchers = [Actor::app_user("user:alice"), Actor::app_user("user:bob")];
    let row = EventRow::current(&created, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "the store says alice owns it"
    );

    // It changes hands. The difference is one removal and one addition, and
    // writing the addition without the removal would leave alice holding it.
    let moved = TestEvent::<Postgres>::update(
        docs,
        vec![Value::Int(4), Value::String("alice".into())],
        vec![Value::Int(4), Value::String("bob".into())],
    )
    .with_pk_columns([0u16]);
    let (diff, _requeries) = shapes.diff(&moved).expect("both images are complete");
    assert_eq!(diff.added.len(), 1, "one fact stated");
    assert_eq!(diff.removed.len(), 1, "one fact withdrawn");
    backend.apply(&diff).await.expect("write the difference");

    let row = EventRow::current(&moved, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Deny, Verdict::Allow],
        "and now the store says bob does, so alice lost it rather than both holding it"
    );
}

/// The two-table removal, closed against the service store: a membership only a
/// replay reaches is withdrawn, the replay returns the slice's remaining
/// truth, and reconciling it takes the stale fact out. This is the gap where a
/// caller following the old contract, replay and write back, was told nothing
/// was stale and kept granting for ever.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario walks grant, withdrawal, empty reconcile and refreshed reconcile \
              in order, and each step's report is asserted where it happens"
)]
async fn a_withdrawn_grant_only_a_replay_reaches_is_reconciled_out() {
    // Nothing keys (team, user) uniquely, so several rows can carry one
    // grant and the latest deadline wins, which no single row image can say:
    // the shape hands over a bound query instead of settling, and the
    // reconciliation is the only remover.
    const EXPIRING: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_setting('app.user_id', true)
            AND team_members.expires_at > now()));
";

    let mut client = openfga().await;

    let wired = wiring(EXPIRING);
    let members =
        catalog_helpers::table_id::<subql::backend::Postgres, _>(&wired.db, "team_members")
            .expect("members is in the catalog");
    let member = wired.fed_by("team_members");

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-reconcile-store".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");

    let withdrawn = TestEvent::<Postgres>::delete(
        members,
        vec![
            Value::Int(3),
            Value::String("alice".into()),
            Value::String("2027-01-01T00:00:00Z".into()),
        ],
    );
    let (diff, requeries) = shapes
        .diff(&withdrawn)
        .expect("the previous image is whole");
    let [Requery::Keyed(requery)] = requeries.as_slice() else {
        panic!("one keyed replay, one table the change arrived on: {diff:?}");
    };
    let condition = requery
        .query
        .condition()
        .expect("the membership is conditional");
    let expiry = "2027-01-01T00:00:00+00:00";
    let stale = membership(&member, "user:alice", condition, expiry);
    backend
        .write_records(std::slice::from_ref(&stale))
        .await
        .expect("seed the membership");

    assert_eq!(
        stored_members(&mut client, &store, &member).await,
        ["user:alice".to_owned()]
    );
    assert!(
        diff.removed.is_empty(),
        "the row alone cannot say what the slice still holds"
    );

    let emptied = backend
        .reconcile_records(requery, &[])
        .await
        .expect("reconcile the emptied slice");
    assert!(
        emptied.added.is_empty(),
        "an empty replay writes nothing: {emptied:?}"
    );
    let [withdrawn_fact] = emptied.removed.as_slice() else {
        panic!("one stale fact deleted, got {emptied:?}");
    };
    assert_eq!(withdrawn_fact.subject, "user:alice");
    assert_eq!(withdrawn_fact.object, "teams:3");
    assert_eq!(withdrawn_fact.relation, member.to_string());
    assert_eq!(
        withdrawn_fact.context, stale.context,
        "the withdrawal carries the context the store held the tuple under"
    );
    let retry = StoreDiff {
        added: Vec::new(),
        removed: vec![stale],
    };
    backend
        .apply(&retry)
        .await
        .expect("retry the applied deletion");

    assert_eq!(
        stored_members(&mut client, &store, &member).await,
        Vec::<String>::new()
    );

    let refreshed = backend
        .reconcile_records(
            requery,
            &[membership(
                &member,
                "user:bob",
                condition,
                "2027-01-01T00:00:00+00:00",
            )],
        )
        .await
        .expect("reconcile the replayed truth");
    assert_eq!(
        refreshed.added,
        vec![membership(
            &member,
            "user:bob",
            condition,
            "2027-01-01T00:00:00+00:00",
        )],
        "the report names the fresh fact it wrote"
    );
    assert!(refreshed.removed.is_empty(), "nothing left to withdraw");

    assert_eq!(
        stored_members(&mut client, &store, &member).await,
        ["user:bob".to_owned()]
    );
}

/// Two membership sources, one settled by the row and one only a replay
/// reaches. Pooled onto one relation, their regions overlap and they form one
/// group.
const SHARED_REGION: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE public.team_guests(team_id INTEGER REFERENCES teams(id), user_id TEXT,
    expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_setting('app.user_id', true))
  OR EXISTS (SELECT 1 FROM team_guests
          WHERE team_guests.team_id = docs.team_id AND team_guests.user_id = current_setting('app.user_id', true)
            AND team_guests.expires_at > now()));
";

/// A membership whose residual compares one row against an aggregate over the
/// whole table. No key narrows it, so the shape carries the unnarrowed query
/// and one changed share can move a grant on any paper at all.
const WHOLE_SHAPE: &str = "
CREATE TABLE public.papers(id INTEGER PRIMARY KEY, owner TEXT);
CREATE TABLE public.paper_shares(paper_id INTEGER REFERENCES papers(id), viewer TEXT,
    weight NUMERIC, PRIMARY KEY(paper_id, viewer));
ALTER TABLE papers ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON papers FOR SELECT USING (
  EXISTS (SELECT 1 FROM paper_shares s
          WHERE s.paper_id = papers.id AND s.viewer = current_setting('app.user_id', true)
            AND s.weight > (SELECT avg(weight) FROM paper_shares)));
";

/// The caller's half of a group reconcile: run one member's query and hand
/// back its rows.
///
/// Canned rather than run against a database, because what is under test is
/// the reconciliation of the union, not the SQL. Each member is recognised by
/// the table its query reads, which is what distinguishes the two producers.
struct CannedReplay {
    rows: Vec<(&'static str, Vec<Record>)>,
}

impl Replayer for CannedReplay {
    type Error = core::convert::Infallible;

    fn replay(
        &self,
        member: &Replay,
    ) -> impl Future<Output = Result<Vec<Record>, Self::Error>> + Send {
        let matched = self
            .rows
            .iter()
            .find(|(table, _)| member.sql().contains(table))
            .unwrap_or_else(|| panic!("no canned rows for {}", member.sql()));
        core::future::ready(Ok(matched.1.clone()))
    }
}

fn plain_membership(member: &RelationName, object: &str, subject: &str) -> Record {
    Record {
        object: object.to_owned(),
        relation: member.clone(),
        subject: subject.to_owned(),
        context: None,
    }
}

/// Both producers' facts survive one reconcile of the region they share, which
/// the old code refused to attempt at all rather than delete one with the
/// other.
///
/// Deliberately not a pair of already-stored facts asserted to still be there,
/// which a materialiser that did nothing would also pass. One expected fact is
/// seeded and one is not, and a third is stored that no member returns, so the
/// report has to name one addition and one removal.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_group_keeps_every_members_facts_in_one_reconcile() {
    let mut client = openfga().await;

    let wired = wiring(SHARED_REGION).pooled("team_guests", "team_members");
    let member = wired.fed_by("team_members");
    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-group-union".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");

    // carol is stored and still granted, dave is stored and granted by nobody,
    // and alice is granted by the settled producer but not yet stored.
    backend
        .write_records(&[
            plain_membership(&member, "teams:3", "user:carol"),
            plain_membership(&member, "teams:3", "user:dave"),
        ])
        .await
        .expect("seed the store");
    // Outside the region entirely: another relation on another type, which no
    // member states and this reconcile has no authority over. Written through
    // the raw client because a `Record` carries a relation name only rls2fga
    // can mint.
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: vec![TupleKey {
                    user: "teams:1".to_owned(),
                    relation: "teams".to_owned(),
                    object: "docs:4".to_owned(),
                    condition: None,
                }],
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: String::new(),
        })
        .await
        .expect("seed the out-of-region tuple");

    let replay = CannedReplay {
        rows: vec![
            (
                "team_members",
                vec![plain_membership(&member, "teams:3", "user:alice")],
            ),
            (
                "team_guests",
                vec![plain_membership(&member, "teams:3", "user:carol")],
            ),
        ],
    };

    let [group] = shapes.materialisations() else {
        panic!(
            "one group over the shared region: {:?}",
            shapes.materialisations()
        );
    };
    let reports = backend
        .materialise([group], &replay)
        .await
        .expect("reconcile the group");
    let [report] = reports.as_slice() else {
        panic!("one report per group: {reports:?}");
    };

    assert_eq!(
        report.added,
        [plain_membership(&member, "teams:3", "user:alice")],
        "the settled producer's fact was missing and is written"
    );
    let [withdrawn] = report.removed.as_slice() else {
        panic!("one stale fact, got {:?}", report.removed);
    };
    assert_eq!(withdrawn.subject, "user:dave");

    let mut stored = stored_members(&mut client, &store, &member).await;
    stored.sort();
    assert_eq!(
        stored,
        ["user:alice".to_owned(), "user:carol".to_owned()],
        "both producers' facts stand, so neither member deleted the other's"
    );
    assert!(
        stored_relation(&mut client, &store, "teams", "docs:4")
            .await
            .contains(&"teams:1".to_owned()),
        "a fact outside the region is not this reconcile's to delete"
    );
}

/// Two members stating one fact under different conditions is refused, and
/// refused before anything is written.
///
/// A tuple carries one condition. Collapsing to whichever member came first
/// would hand somebody access on terms the other producer did not state, and
/// reading the last for the removal while writing the first would delete the
/// tuple and put nothing back.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn two_members_contradicting_one_fact_are_refused_before_any_write() {
    let mut client = openfga().await;

    let wired = wiring(SHARED_REGION).pooled("team_guests", "team_members");
    let member = wired.fed_by("team_members");
    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-group-contradiction".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");

    backend
        .write_records(&[plain_membership(&member, "teams:3", "user:carol")])
        .await
        .expect("seed the store");

    // Both members name carol on team 3. One grants outright, the other only
    // under a condition, and the store can hold one of the two.
    let conditional = Record {
        context: Some(RecordContextValue {
            condition: ConditionName::canonicalized("when_team_guests_expires_at"),
            values: BTreeMap::from([(
                "expires_at".to_owned(),
                "2027-01-01T00:00:00+00:00".to_owned(),
            )]),
        }),
        ..plain_membership(&member, "teams:3", "user:carol")
    };
    let replay = CannedReplay {
        rows: vec![
            (
                "team_members",
                vec![plain_membership(&member, "teams:3", "user:carol")],
            ),
            ("team_guests", vec![conditional]),
        ],
    };

    let [group] = shapes.materialisations() else {
        panic!(
            "one group over the shared region: {:?}",
            shapes.materialisations()
        );
    };
    let refused = backend
        .materialise([group], &replay)
        .await
        .expect_err("a contradiction is refused");
    assert!(
        matches!(&refused, ReconcileError::Contradiction(fact) if fact.contains("user:carol")),
        "named as a contradiction rather than as a store failure, which a \
         caller would retry for ever: {refused:?}"
    );

    assert_eq!(
        stored_members(&mut client, &store, &member).await,
        ["user:carol".to_owned()],
        "and the store is untouched, so nothing was written before the refusal"
    );
}

/// The point of the whole change: a fact the replay stopped returning is
/// removed even for an object the event never named.
///
/// A whole-shape grant moves with rows the changed row does not name, so the
/// object whose grant is withdrawn need have nothing to do with the event. A
/// keyed replay cannot reach it, which is why attaching one would have left it
/// standing with nothing saying so.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_reconcile_removes_a_fact_for_an_object_the_event_never_named() {
    let mut client = openfga().await;

    let wired = wiring(WHOLE_SHAPE);
    let share_table =
        catalog_helpers::table_id::<subql::backend::Postgres, _>(&wired.db, "paper_shares")
            .expect("shares are in the catalog");
    let shared = wired.fed_by("paper_shares");
    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-group-unnamed".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");

    backend
        .write_records(&[
            plain_membership(&shared, "papers:1", "user:alice"),
            plain_membership(&shared, "papers:2", "user:bob"),
        ])
        .await
        .expect("seed the store");

    // The event names paper 1 only. Paper 2 is not in it anywhere.
    let event = TestEvent::<Postgres>::delete(
        share_table,
        vec![
            Value::Int(1),
            Value::String("carol".into()),
            Value::String("7".into()),
        ],
    );
    let (diff, requeries) = shapes.diff(&event).expect("the previous image is whole");
    let [Requery::Whole(group)] = requeries.as_slice() else {
        panic!("the change schedules its whole group: {diff:?}");
    };

    // One share leaving moved the average, so bob's own share fell below it and
    // the replay stops returning his grant on a paper the event never named.
    let replay = CannedReplay {
        rows: vec![(
            "paper_shares",
            vec![plain_membership(&shared, "papers:1", "user:alice")],
        )],
    };

    let reports = backend
        .materialise([*group], &replay)
        .await
        .expect("reconcile the group");
    let [report] = reports.as_slice() else {
        panic!("one report per group: {reports:?}");
    };

    let [withdrawn] = report.removed.as_slice() else {
        panic!("one stale fact, got {:?}", report.removed);
    };
    assert_eq!(withdrawn.subject, "user:bob");
    assert_eq!(
        withdrawn.object, "papers:2",
        "the object the event never named is exactly the one that had to move"
    );
    assert!(
        report.added.is_empty(),
        "alice was already stored: {report:?}"
    );
}

/// A row-settled link from a doc to its team and a membership only a replay
/// reaches, each alone in its region, so the load is the whole truth for both.
const TWO_ALONE: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_setting('app.user_id', true)
            AND team_members.expires_at > now()));
";

/// A store loaded before a backup the database was then restored from.
struct Restored {
    client: OpenFgaServiceClient<Channel>,
    store: String,
    backend: OpenFgaPolicy<ParserDB, Channel, String, Postgres>,
    /// Facts the restored database still states.
    kept: Vec<Record>,
    /// Facts only the store still holds: mallory's membership and doc 2's facts.
    stale: Vec<Record>,
    /// The relation `team_members` rows feed.
    member: RelationName,
    /// The relation the doc links to its team under.
    link: RelationName,
}

/// [`TWO_ALONE`]'s facts over a store whose model is written from `model_sql`.
async fn restored(name: &str, model_sql: &str) -> Restored {
    let mut client = openfga().await;
    let wired_model = wiring(model_sql);
    let (store, model_id) = create_store_with_model(&mut client, name, &wired_model.model).await;
    let installed = installed_model(&wired_model.model, &model_id);

    let wired = wiring(TWO_ALONE);
    let members = catalog_helpers::table_id::<Postgres, _>(&wired.db, "team_members")
        .expect("members is in the catalog");
    let docs = catalog_helpers::table_id::<Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let member = wired.fed_by("team_members");
    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");

    let left = TestEvent::<Postgres>::delete(
        members,
        vec![
            Value::Int(3),
            Value::String("alice".into()),
            Value::String("2027-01-01T00:00:00Z".into()),
        ],
    );
    let (_, requeries) = shapes.diff(&left).expect("the previous image is whole");
    let [Requery::Keyed(requery)] = requeries.as_slice() else {
        panic!("the membership is keyed: {requeries:?}");
    };
    let condition = requery
        .query
        .condition()
        .expect("the membership is conditional");
    let link = shapes
        .load_regions()
        .iter()
        .flat_map(Region::parts)
        .find(|part| part.object_type() == "docs" && part.subject_type() == Some("teams"))
        .expect("the load owns the doc's link to its team")
        .relation()
        .clone();
    let expiry = "2027-01-01T00:00:00+00:00";
    let mut kept = vec![membership(&member, "user:alice", condition, expiry)];
    kept.extend(row_facts(&shapes, docs, vec![Value::Int(1), Value::Int(3)]));
    let mut stale = vec![membership(&member, "user:mallory", condition, expiry)];
    stale.extend(row_facts(&shapes, docs, vec![Value::Int(2), Value::Int(3)]));
    backend
        .write_records(&[kept.as_slice(), stale.as_slice()].concat())
        .await
        .expect("load the store before the backup");

    Restored {
        client,
        store,
        backend,
        kept,
        stale,
        member,
        link,
    }
}

/// Facts the restored database no longer states leave the store, keyed and
/// row-settled alike, and nothing else moves.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_restore_sweeps_keyed_and_row_settled_facts_alike() {
    let mut restored = restored("subql-sweep-restore", TWO_ALONE).await;
    let nothing = CannedReplay { rows: Vec::new() };

    let reports = restored
        .backend
        .reconcile_store(&restored.kept, &nothing)
        .await
        .expect("sweep the store");

    assert!(
        reports.iter().all(|report| report.added.is_empty()),
        "the load states nothing the store lacked: {reports:?}"
    );
    let mut withdrawn: Vec<(String, String)> = reports
        .iter()
        .flat_map(|report| &report.removed)
        .map(|fact| (fact.object.clone(), fact.subject.clone()))
        .collect();
    withdrawn.sort();
    let mut expected: Vec<(String, String)> = restored
        .stale
        .iter()
        .map(|fact| (fact.object.clone(), fact.subject.clone()))
        .collect();
    expected.sort();
    assert_eq!(withdrawn, expected);

    let link = restored.link.to_string();
    assert_eq!(
        stored_members(&mut restored.client, &restored.store, &restored.member).await,
        ["user:alice".to_owned()]
    );
    assert_eq!(
        stored_relation(&mut restored.client, &restored.store, &link, "docs:1").await,
        ["teams:3".to_owned()]
    );
    assert_eq!(
        stored_relation(&mut restored.client, &restored.store, &link, "docs:2").await,
        Vec::<String>::new()
    );

    let again = restored
        .backend
        .reconcile_store(&restored.kept, &nothing)
        .await
        .expect("sweep the healed store");
    assert_eq!(
        again,
        Vec::<Reconciled>::new(),
        "a healed store moves nothing"
    );
}

/// A fact in a relation no shape declares survives the sweep, so a store shared
/// with another writer keeps that writer's facts.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_fact_no_shape_declares_survives_the_sweep() {
    // The other writer's model adds folders, which this translation never sees.
    const SHARED: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
CREATE TABLE public.folders(id INTEGER PRIMARY KEY, owner_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
ALTER TABLE folders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_setting('app.user_id', true)
            AND team_members.expires_at > now()));
CREATE POLICY f ON folders FOR SELECT USING (owner_id = current_setting('app.user_id', true));
";
    let mut restored = restored("subql-sweep-shared", SHARED).await;
    restored
        .client
        .write(WriteRequest {
            store_id: restored.store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: vec![TupleKey {
                    user: "user:bob".to_owned(),
                    relation: "owner".to_owned(),
                    object: "folders:1".to_owned(),
                    condition: None,
                }],
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: String::new(),
        })
        .await
        .expect("the other writer's fact");

    restored
        .backend
        .reconcile_store(&restored.kept, &CannedReplay { rows: Vec::new() })
        .await
        .expect("sweep the store");

    assert_eq!(
        stored_members(&mut restored.client, &restored.store, &restored.member).await,
        ["user:alice".to_owned()],
        "the sweep ran"
    );
    assert_eq!(
        stored_relation(&mut restored.client, &restored.store, "owner", "folders:1").await,
        ["user:bob".to_owned()],
        "and left the other writer's fact alone"
    );
}

/// One sweep heals a group's region from its replays and the load's regions
/// from the records, reporting the load's regions first.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn one_sweep_heals_a_group_region_and_the_loads_regions() {
    let mut client = openfga().await;
    let wired = wiring(SHARED_REGION).pooled("team_guests", "team_members");
    let member = wired.fed_by("team_members");
    let docs = catalog_helpers::table_id::<Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-sweep-group", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");

    let mut seed = vec![
        plain_membership(&member, "teams:3", "user:carol"),
        plain_membership(&member, "teams:3", "user:dave"),
    ];
    seed.extend(row_facts(&shapes, docs, vec![Value::Int(4), Value::Int(1)]));
    backend.write_records(&seed).await.expect("seed the store");
    let replay = CannedReplay {
        rows: vec![
            (
                "team_members",
                vec![plain_membership(&member, "teams:3", "user:alice")],
            ),
            (
                "team_guests",
                vec![plain_membership(&member, "teams:3", "user:carol")],
            ),
        ],
    };

    let records = row_facts(&shapes, docs, vec![Value::Int(5), Value::Int(3)]);
    let reports = backend
        .reconcile_store(&records, &replay)
        .await
        .expect("sweep the store");

    let (group, loads) = reports
        .split_last()
        .expect("the grouped region is reported last");
    let by_key = |left: &&Record, right: &&Record| {
        (&left.object, left.relation.as_str(), &left.subject).cmp(&(
            &right.object,
            right.relation.as_str(),
            &right.subject,
        ))
    };
    let mut added: Vec<&Record> = loads.iter().flat_map(|report| &report.added).collect();
    added.sort_unstable_by(by_key);
    let mut expected: Vec<&Record> = records.iter().collect();
    expected.sort_unstable_by(by_key);
    assert_eq!(added, expected);
    let mut removed: Vec<_> = loads
        .iter()
        .flat_map(|report| &report.removed)
        .map(|fact| {
            (
                fact.object.as_str(),
                fact.relation.as_str(),
                fact.subject.as_str(),
            )
        })
        .collect();
    removed.sort_unstable();
    let stale = row_facts(&shapes, docs, vec![Value::Int(4), Value::Int(1)]);
    let mut expected: Vec<_> = stale
        .iter()
        .map(|fact| {
            (
                fact.object.as_str(),
                fact.relation.as_str(),
                fact.subject.as_str(),
            )
        })
        .collect();
    expected.sort_unstable();
    assert_eq!(removed, expected);
    assert_eq!(
        group.added,
        [plain_membership(&member, "teams:3", "user:alice")]
    );
    assert_eq!(
        group
            .removed
            .iter()
            .map(|fact| fact.subject.as_str())
            .collect::<Vec<_>>(),
        ["user:dave"]
    );

    let mut members = stored_members(&mut client, &store, &member).await;
    members.sort();
    assert_eq!(members, ["user:alice".to_owned(), "user:carol".to_owned()]);
}

fn membership(
    member: &RelationName,
    subject: &str,
    condition: &ConditionName,
    expires_at: &str,
) -> Record {
    Record {
        object: "teams:3".to_owned(),
        relation: member.clone(),
        subject: subject.to_owned(),
        context: Some(RecordContextValue {
            condition: condition.clone(),
            values: BTreeMap::from([("expires_at".to_owned(), expires_at.to_owned())]),
        }),
    }
}

async fn stored_members(
    client: &mut OpenFgaServiceClient<Channel>,
    store: &str,
    member: &RelationName,
) -> Vec<String> {
    stored_relation(client, store, &member.to_string(), "teams:3").await
}

async fn stored_relation_keys(
    client: &mut OpenFgaServiceClient<Channel>,
    store: &str,
    relation: &str,
    object: &str,
) -> Vec<TupleKey> {
    let mut continuation_token = String::new();
    let mut stored = Vec::new();
    loop {
        let response = client
            .read(ReadRequest {
                store_id: store.to_owned(),
                tuple_key: Some(ReadRequestTupleKey {
                    user: String::new(),
                    relation: relation.to_owned(),
                    object: object.to_owned(),
                }),
                page_size: None,
                continuation_token,
                // Discriminant extraction: the generated field is a bare `i32`.
                consistency: ConsistencyPreference::HigherConsistency as i32,
            })
            .await
            .expect("read every relation tuple")
            .into_inner();
        stored.extend(response.tuples.into_iter().filter_map(|tuple| tuple.key));
        continuation_token = response.continuation_token;
        if continuation_token.is_empty() {
            break;
        }
    }
    stored
}

/// Every subject the store holds for one relation on one object.
async fn stored_relation(
    client: &mut OpenFgaServiceClient<Channel>,
    store: &str,
    relation: &str,
    object: &str,
) -> Vec<String> {
    stored_relation_keys(client, store, relation, object)
        .await
        .into_iter()
        .map(|key| key.user)
        .collect()
}

/// A watcher that names itself and says which keys its request carried.
///
/// The held-keys arm is a condition the server completes from the question, so
/// a watcher that cannot state its keys is refused rather than answered.
#[derive(Clone, Debug)]
struct Principal {
    name: String,
    keys: Vec<String>,
}

impl Principal {
    fn new(name: &str, keys: &[&str]) -> Self {
        Self {
            name: name.to_owned(),
            keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        }
    }
}

impl Subject for Principal {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        core::iter::once(Cow::Borrowed(self.name.as_str()))
    }

    fn acting_role(&self) -> Option<&str> {
        Some(deployment::ROLE)
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        if parameter != "app_subjects" {
            return false;
        }
        for key in &self.keys {
            out.push(key);
        }
        true
    }
}

/// A change that moves only a conditional tuple's context is still applied.
///
/// This is the shape connetto writes on every table: an identity arm and a
/// held-keys arm in one policy. rls2fga models the held-keys arm as a condition
/// over the wildcard rather than as a subject, so every owner shares the subject
/// `user:*` and the owner lives in the condition's context. An owner change then
/// produces an addition and a removal carrying one tuple key, differing in a
/// field the server does not treat as part of the key, and it refuses a single
/// call holding both.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_change_of_only_a_conditional_context_is_applied() {
    const HELD_KEYS: &str = "
CREATE TABLE public.notes(id INTEGER PRIMARY KEY, owner TEXT NOT NULL);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes FOR ALL USING (
  owner = current_setting('app.user_id', true)
  OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
";

    let mut client = openfga().await;

    let wired = wiring(HELD_KEYS);
    let notes = catalog_helpers::table_id::<subql::backend::Postgres, _>(&wired.db, "notes")
        .expect("notes is in the catalog");

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility-store".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &wired.model).await;
    let constants = constants(&wired.outputs);
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, Principal, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries what the questions need");
    backend
        .write_records(&constants)
        .await
        .expect("write the acting constant");

    let created =
        TestEvent::<Postgres>::insert(notes, vec![Value::Int(1), Value::String("alice".into())])
            .with_pk_columns([0u16]);
    let (created_diff, _requeries) = shapes.diff(&created).expect("an insert is all additions");
    backend
        .apply(&created_diff)
        .await
        .expect("write the additions");

    // Each watcher holds its own name as a key, which is how the held-keys arm
    // grants at all, and what puts the owner into the condition's context.
    let watchers = [
        Principal::new("user:alice", &["alice"]),
        Principal::new("user:carol", &["carol"]),
    ];
    let row = EventRow::current(&created, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "the store says alice owns it"
    );

    let moved = TestEvent::<Postgres>::update(
        notes,
        vec![Value::Int(1), Value::String("alice".into())],
        vec![Value::Int(1), Value::String("carol".into())],
    )
    .with_pk_columns([0u16]);
    let (diff, _requeries) = shapes.diff(&moved).expect("both images are complete");
    let conditional = |record: &rls2fga::types::Record| record.context.is_some();
    assert!(
        diff.added.iter().any(conditional) && diff.removed.iter().any(conditional),
        "the held-keys arm states a conditional fact on both sides, \
         which is what collides: {diff:?}"
    );
    backend
        .apply(&diff)
        .await
        .expect("the difference is written even though one key is on both sides");

    let row = EventRow::current(&moved, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Deny, Verdict::Allow],
        "and the condition now carries carol rather than alice"
    );
}

/// A held-keys arm admits only watchers of the type its wildcard names, locally
/// as on the service.
///
/// Every watcher below sent the row's owner as a key, so the comparison alone
/// grants all of them. The service grants `user:*`, which neither a userset nor
/// an object of another type is a member of.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_held_key_admits_only_the_wildcards_type() {
    const HELD_KEYS: &str = "
CREATE TABLE public.notes(id INTEGER PRIMARY KEY, owner TEXT NOT NULL);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes FOR ALL USING (
  owner = current_setting('app.user_id', true)
  OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
";

    let mut client = openfga().await;
    let wired = wiring(HELD_KEYS);
    let constants = constants(&wired.outputs);
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-held-key-type", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();
    let service = OpenFgaPolicy::<_, _, Principal, Postgres>::new(
        Arc::clone(&shapes),
        client,
        store,
        &installed,
    )
    .expect("the index carries what the questions need");
    service
        .write_records(&constants)
        .await
        .expect("write the acting constant");
    let local = RowPolicy::new(Arc::clone(&shapes), Unasked::<Principal>::default());

    let notes = catalog_helpers::table_id::<Postgres, _>(shapes.catalog(), "notes")
        .expect("notes is in the catalog");
    let stored =
        TestEvent::<Postgres>::insert(notes, vec![Value::Int(1), Value::String("alice".into())])
            .with_pk_columns([0u16]);
    let (diff, _requeries) = shapes.diff(&stored).expect("an insert states its facts");
    service.apply(&diff).await.expect("write the row's facts");
    let row = EventRow::current(&stored, shapes.catalog()).expect("post-image");

    let watchers = [
        Principal::new("user:bob", &["alice"]),
        Principal::new("notes:2#owner", &["alice"]),
        Principal::new("notes:2", &["alice"]),
    ];
    let mut answered = Vec::new();
    Verdict::reset(&mut answered, watchers.len());
    local
        .may_see(&row, &watchers, &mut answered)
        .await
        .expect("the stand-in never fails");
    let mut expected = Vec::new();
    Verdict::reset(&mut expected, watchers.len());
    service
        .may_see(&row, &watchers, &mut expected)
        .await
        .expect("the service answered");

    assert_eq!(expected, [Verdict::Allow, Verdict::Deny, Verdict::Deny]);
    assert_eq!(answered, expected, "for {watchers:?}");
    assert_eq!(
        local.inner().take_asked(),
        0,
        "the row and the request settle every watcher"
    );
}

/// The four restrictive policies that hold a bot to its declared list, one per
/// command, each admitting the table's own `table:verb` entry or `*`.
fn bot_gates(table: &str) -> String {
    let listed = |verb: &str| {
        format!(
            "'{table}:{verb}' = ANY(string_to_array(current_setting('app.bot_list', true), ',')) \
             OR '*' = ANY(string_to_array(current_setting('app.bot_list', true), ','))"
        )
    };
    format!(
        "CREATE POLICY {table}_cap_read ON {table} AS RESTRICTIVE FOR SELECT USING ({read});
         CREATE POLICY {table}_cap_insert ON {table} AS RESTRICTIVE FOR INSERT WITH CHECK ({insert});
         CREATE POLICY {table}_cap_update ON {table} AS RESTRICTIVE FOR UPDATE
           USING ({update}) WITH CHECK ({update});
         CREATE POLICY {table}_cap_delete ON {table} AS RESTRICTIVE FOR DELETE USING ({delete});",
        read = listed("read"),
        insert = listed("insert"),
        update = listed("update"),
        delete = listed("delete"),
    )
}

/// A caller and the bot list its request carried, `None` for a request that
/// carried none.
#[derive(Clone, Debug)]
struct Bot {
    name: String,
    list: Option<Vec<String>>,
}

impl Subject for Bot {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        core::iter::once(Cow::Borrowed(self.name.as_str()))
    }

    fn acting_role(&self) -> Option<&str> {
        Some(deployment::ROLE)
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        let (Some(list), "app_bot_list") = (self.list.as_ref(), parameter) else {
            return false;
        };
        for entry in list {
            out.push(entry);
        }
        true
    }
}

/// A backend that grants nothing and counts the watchers it was asked about,
/// so a verdict it did not write is one the row and the request settled.
struct Unasked<W> {
    asked: std::sync::atomic::AtomicUsize,
    watcher: core::marker::PhantomData<fn() -> W>,
}

impl<W> Default for Unasked<W> {
    fn default() -> Self {
        Self {
            asked: std::sync::atomic::AtomicUsize::new(0),
            watcher: core::marker::PhantomData,
        }
    }
}

impl<W> Unasked<W> {
    /// How many watchers reached the backend since the last call.
    fn take_asked(&self) -> usize {
        self.asked.swap(0, std::sync::atomic::Ordering::Relaxed)
    }
}

impl<W: Subject + Clone + Send + Sync + 'static> VisibilityPolicy for Unasked<W> {
    type Watcher = W;
    type Error = core::convert::Infallible;
    type Backend = Postgres;

    fn may_see<R>(
        &self,
        _row: &R,
        watchers: &[W],
        _verdicts: &mut [Verdict],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        R: subql::visibility::RowView<Backend = Postgres> + Sync + ?Sized,
    {
        self.asked
            .fetch_add(watchers.len(), std::sync::atomic::Ordering::Relaxed);
        async { Ok(()) }
    }

    fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &W,
    ) -> impl Future<Output = Result<Verdict, Self::Error>> + Send
    where
        R: subql::visibility::RowView<Backend = Postgres> + Sync + ?Sized,
    {
        self.asked
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async { Ok(Verdict::Deny) }
    }
}

/// Callers each with a list holding `table`'s own entries, only `*`, another
/// table's entry, or nothing: the row's owner, somebody else, a userset and an
/// object of a type that is not the caller's.
fn bots(table: &str) -> Vec<Bot> {
    let lists = [
        vec![
            format!("{table}:read"),
            format!("{table}:insert"),
            format!("{table}:delete"),
        ],
        vec!["*".to_owned()],
        vec!["elsewhere:read".to_owned()],
        Vec::new(),
    ];
    ["user:alice", "user:carol", "notes:2#owner", "orders:2"]
        .into_iter()
        .flat_map(|caller| {
            lists.iter().map(move |list| Bot {
                name: caller.to_owned(),
                list: Some(list.clone()),
            })
        })
        .collect()
}

/// A table its owner column decides, one every caller may use, one only the
/// list admits to, and one the list or the owner column admits to.
fn bot_gated_schema() -> String {
    let listed = |table: &str| {
        format!(
            "'{table}:read' = ANY(string_to_array(current_setting('app.bot_list', true), ',')) \
             OR '*' = ANY(string_to_array(current_setting('app.bot_list', true), ','))"
        )
    };
    format!(
        "CREATE TABLE public.notes(id INTEGER PRIMARY KEY, owner TEXT);
         ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
         CREATE POLICY notes_p ON notes FOR ALL
           USING (owner = current_setting('app.user_id', true))
           WITH CHECK (owner = current_setting('app.user_id', true));
         {notes}
         CREATE TABLE public.orders(id INTEGER PRIMARY KEY, owner TEXT);
         ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
         CREATE POLICY orders_p ON orders FOR ALL USING (true) WITH CHECK (true);
         {orders}
         CREATE TABLE public.pages(id INTEGER PRIMARY KEY, owner TEXT);
         ALTER TABLE pages ENABLE ROW LEVEL SECURITY;
         CREATE POLICY pages_p ON pages FOR SELECT USING ({pages});
         CREATE TABLE public.posts(id INTEGER PRIMARY KEY, owner TEXT);
         ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
         CREATE POLICY posts_p ON posts FOR SELECT
           USING ({posts} OR owner = current_setting('app.user_id', true));",
        notes = bot_gates("notes"),
        orders = bot_gates("orders"),
        pages = listed("pages"),
        posts = listed("posts"),
    )
}

/// The records the translation states whatever the rows hold, such as a
/// request gate's entries, which the load writes once and no row's change
/// states.
fn constants(outputs: &Outputs) -> Vec<Record> {
    outputs
        .tuple_queries()
        .iter()
        .filter_map(|query| match &query.description.as_ref()?.derivation {
            RecordDerivation::Constant { record } => Some(record.clone()),
            _ => None,
        })
        .collect()
}

/// Every fact the load writes for one row of `table`, read off the event that
/// inserts it, so a seed states what the translation states.
fn row_facts(
    shapes: &Shapes<ParserDB>,
    table: TableId,
    values: Vec<Value<Postgres>>,
) -> Vec<Record> {
    let event = TestEvent::<Postgres>::insert(table, values).with_pk_columns([0u16]);
    shapes
        .diff(&event)
        .expect("the row states its facts")
        .0
        .added
}

/// The raw tuple keys of records that carry no condition, for a seed that
/// writes through the client rather than the policy.
fn tuples_of(records: &[Record]) -> Vec<TupleKey> {
    records
        .iter()
        .map(|record| {
            assert!(record.context.is_none(), "the seed states no condition");
            TupleKey {
                user: record.subject.clone(),
                relation: record.relation.to_string(),
                object: record.object.clone(),
                condition: None,
            }
        })
        .collect()
}

/// A bot gate answered from the row and the request agrees with the service,
/// on every table of [`bot_gated_schema`].
///
/// Every combination is asked: the row admitting the caller or not, against a
/// list holding the table's own entry, only `*`, another table's entry, or
/// nothing. A request carrying no list is not the row's to settle, so it alone
/// reaches the backend.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_bot_gate_is_answered_locally_as_the_service_answers() {
    let mut client = openfga().await;
    let wired = wiring(&bot_gated_schema());
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-bot-gate-parity", &wired.model).await;
    let constants = constants(&wired.outputs);
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();
    let service =
        OpenFgaPolicy::<_, _, Bot, Postgres>::new(Arc::clone(&shapes), client, store, &installed)
            .expect("the index carries the bot list parameter");
    service
        .write_records(&constants)
        .await
        .expect("write the gate entries");
    let local = RowPolicy::new(Arc::clone(&shapes), Unasked::default());

    for name in ["notes", "orders", "pages", "posts"] {
        let table = catalog_helpers::table_id::<Postgres, _>(shapes.catalog(), name)
            .expect("the table is in the catalog");
        let stored = TestEvent::<Postgres>::insert(
            table,
            vec![Value::Int(1), Value::String("alice".into())],
        )
        .with_pk_columns([0u16]);
        let (diff, _requeries) = shapes.diff(&stored).expect("an insert states its facts");
        service.apply(&diff).await.expect("write the row's facts");
        let row = EventRow::current(&stored, shapes.catalog()).expect("post-image");

        let watchers = bots(name);

        let mut answered = Vec::new();
        Verdict::reset(&mut answered, watchers.len());
        local
            .may_see(&row, &watchers, &mut answered)
            .await
            .expect("the stand-in never fails");
        let mut expected = Vec::new();
        Verdict::reset(&mut expected, watchers.len());
        service
            .may_see(&row, &watchers, &mut expected)
            .await
            .expect("the service answered");
        assert_eq!(answered, expected, "reading {name} for {watchers:?}");

        for watcher in &watchers {
            for write in [
                RowWrite::Insert {
                    new: &row,
                    assigned: &[],
                },
                RowWrite::Delete {
                    old: &row,
                    read: &[],
                },
            ] {
                assert_eq!(
                    local.may_write(write, watcher).await.expect("never fails"),
                    service
                        .may_write(write, watcher)
                        .await
                        .expect("the service answered"),
                    "{:?} on {name} for {watcher:?}",
                    write.op(),
                );
            }
        }
        assert_eq!(
            local.inner().take_asked(),
            0,
            "the row and the request settle every question on {name}"
        );

        // Somebody the row does not admit, since an owner is granted on `posts`
        // whatever the list says.
        let unset = [Bot {
            name: "user:carol".to_owned(),
            list: None,
        }];
        let mut verdict = Vec::new();
        Verdict::reset(&mut verdict, 1);
        local
            .may_see(&row, &unset, &mut verdict)
            .await
            .expect("the stand-in never fails");
        assert_eq!(
            local.inner().take_asked(),
            1,
            "a request carrying no list leaves the gate open, so {name} delegates it"
        );
    }
}

/// A team read restricted to the deployment's role.
const ROLE_SCOPED: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          PRIMARY KEY(team_id, user_id));
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT TO app_user USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id
            AND team_members.user_id = current_setting('app.user_id', true)));
";

/// Role grants remain per request when watchers share a subject.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_known_role_reaches_the_service_and_no_other_role_does() {
    let mut client = openfga().await;
    let wired = wiring(ROLE_SCOPED);
    let docs = catalog_helpers::table_id::<Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let member = wired.fed_by("team_members");
    let constants = constants(&wired.outputs);
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-acting-role", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);
    let shapes = wired.shapes();

    let facts = tuples_of(&row_facts(
        &shapes,
        docs,
        vec![Value::Int(4), Value::Int(1)],
    ));
    let mut tuples = vec![TupleKey {
        user: "user:alice".to_owned(),
        relation: member.to_string(),
        object: "teams:1".to_owned(),
        condition: None,
    }];
    tuples.extend(facts);
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: tuples,
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: model_id.clone(),
        })
        .await
        .expect("write the tuples");

    let service = OpenFgaPolicy::<_, _, Actor, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries the read relation");
    service
        .write_records(&constants)
        .await
        .expect("write the acting constant");

    let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(1)])
        .with_pk_columns([0u16]);
    let row = EventRow::current(&event, shapes.catalog()).expect("post-image");
    let watchers = [
        Actor::app_user("user:alice"),
        Actor {
            name: "user:alice".into(),
            role: None,
        },
        Actor {
            name: "user:alice".into(),
            role: Some("auditor"),
        },
        Actor::app_user("user:bob"),
    ];
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    service
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");

    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny, Verdict::Deny, Verdict::Deny],
        "only the row's member acting as the admitted role is granted"
    );

    let acting = constants
        .iter()
        .find(|record| record.subject.ends_with("#usage"))
        .expect("the load states the role's usage");
    assert_eq!(
        stored_relation(
            &mut client,
            &store,
            &acting.relation.to_string(),
            &acting.object
        )
        .await,
        core::slice::from_ref(&acting.subject),
        "the load wrote its one constant and nothing more"
    );
    let (role_object, usage) = acting
        .subject
        .rsplit_once('#')
        .expect("the constant stands under the role's usage");
    assert!(
        stored_relation(&mut client, &store, usage, role_object)
            .await
            .is_empty(),
        "no per-subject role fact is stored, the check carries it"
    );
}

/// A quoted role with configured subject and role types.
const QUOTED_ROLE_SCOPED: &str = "
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, owner_id TEXT);
GRANT SELECT ON docs TO \"reader:west\";
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT TO \"reader:west\"
  USING (owner_id = current_setting('app.user_id', true));
";

/// The encoded role object under the configured role type.
const QUOTED_ROLE_OBJECT: &str = "database_role:~7265616465723a77657374";

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_quoted_role_is_stated_with_its_encoded_name() {
    let mut client = openfga().await;

    let db = ParserDB::parse::<PostgreSqlDialect>(&format!(
        "CREATE ROLE \"reader:west\" LOGIN;\n{QUOTED_ROLE_SCOPED}"
    ))
    .expect("the quoted role parses");
    let setting = SessionSetting::value("app.user_id", SettingValue::Caller(DataType::Text))
        .expect("a caller setting");
    let deployment = Deployment::new("reader:west", ["reader:west"], [setting])
        .expect("a valid fixture deployment");
    let outputs = TranslatorBuilder::new(deployment)
        .with_well_known_types(
            WellKnownTypes::new("caller", "team", "database_role", "request_gate", "nobody")
                .expect("valid well-known types"),
        )
        .with_min_confidence(ConfidenceLevel::B)
        .build()
        .translate(&db)
        .expect("the visibility schema translates")
        .outputs_accepting_gaps();
    let model = outputs.json_model();
    let wired = Wiring {
        db,
        outputs,
        model,
        pool: None,
    };
    let docs = catalog_helpers::table_id::<Postgres, _>(&wired.db, "docs")
        .expect("docs is in the catalog");
    let constants = constants(&wired.outputs);
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-quoted-role", &wired.model).await;
    let installed = installed_model(&wired.model, &model_id);

    let shapes = wired.shapes();
    let service = OpenFgaPolicy::<_, _, Actor, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
        &installed,
    )
    .expect("the index carries the read relation");
    service
        .write_records(&constants)
        .await
        .expect("write the acting constant");

    let event =
        TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::String("alice".into())])
            .with_pk_columns([0u16]);
    let (diff, _requeries) = shapes.diff(&event).expect("an insert is all additions");
    service.apply(&diff).await.expect("write the owner fact");
    let row = EventRow::current(&event, shapes.catalog()).expect("post-image");

    let watchers = [
        Actor {
            name: "caller:alice".into(),
            role: Some("reader:west"),
        },
        Actor {
            name: "caller:alice".into(),
            role: None,
        },
        Actor {
            name: "caller:bob".into(),
            role: Some("reader:west"),
        },
    ];
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    service
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny, Verdict::Deny],
        "the owner acting as the quoted role is granted, and no one else is"
    );

    let acting = constants
        .iter()
        .find(|record| record.subject.ends_with("#usage"))
        .expect("the load states the role's usage");
    assert_eq!(
        acting.subject,
        format!("{QUOTED_ROLE_OBJECT}#usage"),
        "the configured type and the encoded role name, not the DDL's spelling"
    );
    assert_eq!(
        stored_relation(
            &mut client,
            &store,
            &acting.relation.to_string(),
            &acting.object
        )
        .await,
        [format!("{QUOTED_ROLE_OBJECT}#usage")],
        "the one constant stands under the encoded name"
    );
}

/// `docs` with a role holding `SELECT` and `UPDATE` on the whole table beside
/// roles holding them on some columns only, each role named under `prefix`.
fn column_grants(prefix: &str) -> String {
    format!(
        "CREATE ROLE {prefix}_login LOGIN NOINHERIT;
         CREATE ROLE {prefix}_keeper NOLOGIN;
         CREATE ROLE {prefix}_editor NOLOGIN;
         CREATE ROLE {prefix}_reader NOLOGIN;
         CREATE ROLE {prefix}_writer NOLOGIN;
         CREATE ROLE {prefix}_updater NOLOGIN;
         CREATE ROLE {prefix}_stranger NOLOGIN;
         CREATE TABLE public.docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT);
         ALTER TABLE docs OWNER TO {prefix}_keeper;
         ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
         CREATE POLICY docs_read ON docs FOR SELECT USING (true);
         CREATE POLICY docs_write ON docs FOR UPDATE USING (true);
         GRANT SELECT, UPDATE ON docs TO {prefix}_editor;
         GRANT SELECT (id, title) ON docs TO {prefix}_reader;
         GRANT SELECT (id, title) ON docs TO {prefix}_writer;
         GRANT UPDATE (title) ON docs TO {prefix}_writer;
         GRANT SELECT (id) ON docs TO {prefix}_updater;
         GRANT UPDATE (title) ON docs TO {prefix}_updater;"
    )
}

/// The acting roles of [`column_grants`].
const COLUMN_ROLES: [&str; 5] = ["editor", "reader", "writer", "updater", "stranger"];

/// A caller acting as one role, reading the columns its subscription names.
#[derive(Clone)]
struct Reading {
    role: String,
    read: Option<&'static [u16]>,
}

impl Subject for Reading {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        std::iter::once(Cow::Borrowed("user:alice"))
    }

    fn acting_role(&self) -> Option<&str> {
        Some(&self.role)
    }

    fn columns_read(&self, _table: TableId) -> Option<&[u16]> {
        self.read
    }
}

/// One statement, what subql is told it touches, and what PostgreSQL answers.
struct ColumnCase {
    role: &'static str,
    sql: &'static str,
    read: Option<&'static [u16]>,
    assigned: &'static [u16],
    allowed: bool,
}

const fn read_case(
    role: &'static str,
    sql: &'static str,
    read: Option<&'static [u16]>,
    allowed: bool,
) -> ColumnCase {
    ColumnCase {
        role,
        sql,
        read,
        assigned: &[],
        allowed,
    }
}

const fn update_case(
    role: &'static str,
    sql: &'static str,
    assigned: &'static [u16],
    read: &'static [u16],
    allowed: bool,
) -> ColumnCase {
    ColumnCase {
        role,
        sql,
        read: Some(read),
        assigned,
        allowed,
    }
}

/// `docs` is `id`, `title`, `body`, by ordinal.
const READS: [ColumnCase; 8] = [
    read_case("editor", "SELECT * FROM docs", None, true),
    read_case("reader", "SELECT id, title FROM docs", Some(&[0, 1]), true),
    read_case("reader", "SELECT id, body FROM docs", Some(&[0, 2]), false),
    read_case("reader", "SELECT * FROM docs", None, false),
    read_case("reader", "SELECT 1 FROM docs", Some(&[]), true),
    read_case("reader", "SELECT count(*) FROM docs", Some(&[]), true),
    read_case("editor", "SELECT 1 FROM docs", Some(&[]), true),
    read_case("stranger", "SELECT 1 FROM docs", Some(&[]), false),
];

const UPDATES: [ColumnCase; 7] = [
    update_case(
        "editor",
        "UPDATE docs SET body = 'x' WHERE id = 1",
        &[2],
        &[0],
        true,
    ),
    update_case(
        "writer",
        "UPDATE docs SET title = 'x' WHERE id = 1",
        &[1],
        &[0],
        true,
    ),
    update_case(
        "writer",
        "UPDATE docs SET title = body WHERE id = 1",
        &[1],
        &[0, 2],
        false,
    ),
    update_case(
        "writer",
        "UPDATE docs SET title = 'x' WHERE body = 'y'",
        &[1],
        &[2],
        false,
    ),
    update_case(
        "writer",
        "UPDATE docs SET body = 'x' WHERE id = 1",
        &[2],
        &[0],
        false,
    ),
    update_case(
        "updater",
        "UPDATE docs SET title = 'x' WHERE id = 1",
        &[1],
        &[0],
        true,
    ),
    update_case(
        "reader",
        "UPDATE docs SET title = 'x' WHERE id = 1",
        &[1],
        &[0],
        false,
    ),
];

/// Whether PostgreSQL runs `sql` as `role`, refusing only for a privilege.
fn postgres_allows(pg: &mut diesel::PgConnection, role: &str, sql: &str) -> bool {
    use diesel::connection::SimpleConnection;
    pg.batch_execute(&format!("SET ROLE {role}"))
        .expect("set the acting role");
    let ran = pg.batch_execute(sql);
    pg.batch_execute("RESET ROLE").expect("reset the role");
    match ran {
        Ok(()) => true,
        Err(diesel::result::Error::DatabaseError(_, info))
            if info.message().starts_with("permission denied") =>
        {
            false
        }
        Err(error) => panic!("{sql} as {role} failed for another reason: {error}"),
    }
}

/// What PostgreSQL answers for every case, on a database holding `schema`.
fn postgres_answers(prefix: &str, schema: &str) -> Vec<bool> {
    use diesel::connection::SimpleConnection;
    let db = crate::common::pg::pg_database();
    let mut pg = db.connect();
    pg.batch_execute(schema)
        .expect("create the roles and the table");
    pg.batch_execute("INSERT INTO docs VALUES (1, 't', 'b')")
        .expect("seed one row");
    let answers = READS
        .iter()
        .chain(&UPDATES)
        .map(|case| postgres_allows(&mut pg, &format!("{prefix}_{}", case.role), case.sql))
        .collect();
    // Roles are cluster-wide, and the table holds every grant they have.
    let roles = ["login", "keeper"]
        .iter()
        .chain(&COLUMN_ROLES)
        .map(|role| format!("{prefix}_{role}"))
        .collect::<Vec<_>>()
        .join(", ");
    pg.batch_execute(&format!("DROP TABLE docs; DROP ROLE {roles}"))
        .expect("drop the roles");
    answers
}

/// The policy over [`column_grants`] under `prefix`, with the row [`column_row`]
/// stores and the acting constants written to a fresh store.
async fn column_grant_policy(
    prefix: &str,
    schema: &str,
) -> (
    RowPolicy<ParserDB, OpenFgaPolicy<ParserDB, Channel, Reading, Postgres>>,
    TableId,
) {
    let mut client = openfga().await;
    let db = ParserDB::parse::<PostgreSqlDialect>(schema).expect("the schema parses");
    let acting = COLUMN_ROLES.map(|role| format!("{prefix}_{role}"));
    let outputs = TranslatorBuilder::new(
        Deployment::new(
            format!("{prefix}_login"),
            acting.iter(),
            Vec::<SessionSetting>::new(),
        )
        .expect("a valid deployment"),
    )
    .with_min_confidence(ConfidenceLevel::B)
    .build()
    .translate(&db)
    .expect("the schema translates")
    .outputs_accepting_gaps();
    let constants = constants(&outputs);
    let model = outputs.json_model();
    let docs =
        catalog_helpers::table_id::<Postgres, _>(&db, "docs").expect("docs is in the catalog");
    let (store, model_id) =
        create_store_with_model(&mut client, "subql-column-grants", &model).await;
    let installed = installed_model(&model, &model_id);
    let shapes = Wiring {
        db,
        outputs,
        model,
        pool: None,
    }
    .shapes();
    let backend = OpenFgaPolicy::<_, _, Reading, Postgres>::new(
        Arc::clone(&shapes),
        client,
        store,
        &installed,
    )
    .expect("the index carries what the questions need");
    let stored = TestEvent::<Postgres>::insert(docs, column_row("t")).with_pk_columns([0u16]);
    let (facts, _requeries) = shapes.diff(&stored).expect("the row states its facts");
    backend.apply(&facts).await.expect("write the row's facts");
    backend
        .write_records(&constants)
        .await
        .expect("write the acting constants");
    (RowPolicy::new(shapes, backend), docs)
}

/// A role holding a command on some columns only is asked about the columns a
/// statement touches, beside a role holding it on the whole table, as
/// PostgreSQL asks.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_column_grant_is_asked_per_column_as_postgres_asks() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let prefix = format!("bycol{}x{nanos}", std::process::id());
    let schema = column_grants(&prefix);
    let (postgres, prefix, schema) =
        tokio::task::spawn_blocking(move || (postgres_answers(&prefix, &schema), prefix, schema))
            .await
            .expect("ask postgres");
    let expected: Vec<bool> = READS
        .iter()
        .chain(&UPDATES)
        .map(|case| case.allowed)
        .collect();
    assert_eq!(postgres, expected, "PostgreSQL answers as the cases say");

    let (policy, docs) = column_grant_policy(&prefix, &schema).await;
    assert!(
        !policy.answers_locally(docs, ActionStatement::Select),
        "a role reading some columns only is asked on the gate"
    );

    let stored = TestEvent::<Postgres>::insert(docs, column_row("t")).with_pk_columns([0u16]);
    let row = EventRow::current(&stored, policy.catalog()).expect("an insert carries a post-image");
    let changed = TestEvent::<Postgres>::update(docs, column_row("t"), column_row("x"))
        .with_pk_columns([0u16]);
    let old = EventRow::previous(&changed, policy.catalog()).expect("pre-image");
    let new = EventRow::current(&changed, policy.catalog()).expect("post-image");
    let mut subql = Vec::new();
    for case in &READS {
        let watcher = [Reading {
            role: format!("{prefix}_{}", case.role),
            read: case.read,
        }];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, 1);
        policy
            .may_see(&row, &watcher, &mut verdicts)
            .await
            .expect("the service answered the read");
        subql.push(verdicts[0] == Verdict::Allow);
    }
    for case in &UPDATES {
        let watcher = Reading {
            role: format!("{prefix}_{}", case.role),
            read: None,
        };
        let write = RowWrite::Update {
            old: &old,
            new: &new,
            assigned: case.assigned,
            read: case.read.unwrap_or_default(),
        };
        let verdict = policy
            .may_write(write, &watcher)
            .await
            .expect("the service answered the update");
        subql.push(verdict == Verdict::Allow);
    }
    let statements: Vec<_> = READS
        .iter()
        .chain(&UPDATES)
        .map(|case| (case.role, case.sql))
        .collect();
    assert_eq!(
        statements.iter().zip(&subql).collect::<Vec<_>>(),
        statements.iter().zip(&postgres).collect::<Vec<_>>(),
        "subql answers each statement as PostgreSQL does"
    );
}

/// One row of `docs` with key 1.
fn column_row(title: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::String(title.into()),
        Value::String("b".into()),
    ]
}
