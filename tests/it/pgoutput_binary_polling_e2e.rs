//! A real PostgreSQL sends the same rows as text and as binary pgoutput, and
//! both read, dispatch, escalate and emit alike.
//!
//! Two polling sources peek one slot, one asking for `binary on`, and
//! neither acknowledges, so both read the same changes.

#![allow(clippy::unwrap_used)]

use crate::common::{self, decisions};

use std::time::Duration;

use diesel::{sql_query, RunQueryDsl};
use pg_walstream::{sqlstate::pg, ColumnValue, EventType, ServerError};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind, Value};
use subql::{
    catalog_helpers, BinaryUnavailable, CdcSource, DefaultIds, EventKind, PgChangeEvent,
    PgCommitPosition, PgStreamingCdcSource, PgStreamingConfig, PgStreamingError,
    PollingPgCdcConfig, PollingPgCdcError, PollingPgCdcSource, SourceItemOf, SubscriptionEngine,
    SubscriptionRequest,
};

const TYPES: [&str; 2] = [
    "CREATE TYPE mood AS ENUM ('ok', 'sad')",
    "CREATE DOMAIN posint AS INT CHECK (VALUE > 0)",
];

/// Columns before `acl` have a builtin type subql decodes.
const BUILTIN_COLUMNS: u16 = 19;

const TABLE: &str = "CREATE TABLE kinds (
    id INT PRIMARY KEY, big BIGINT, at TIMESTAMPTZ, amount NUMERIC(12, 3), uid UUID,
    body TEXT, flag BOOL, small SMALLINT, single REAL, double DOUBLE PRECISION,
    code VARCHAR(8), fixed CHAR(4), blob BYTEA, day DATE, clock TIME, stamp TIMESTAMP,
    span INTERVAL, doc JSON, docb JSONB, acl ACLITEM, feel mood, n posint
)";

/// The builtin columns of `kinds` and `aclitem`, which a replica can hold
/// from either format.
const PLAIN: &str = "CREATE TABLE plain (
    id INT PRIMARY KEY, big BIGINT, at TIMESTAMPTZ, amount NUMERIC(12, 3), uid UUID,
    body TEXT, flag BOOL, small SMALLINT, single REAL, double DOUBLE PRECISION,
    code VARCHAR(8), fixed CHAR(4), blob BYTEA, day DATE, clock TIME, stamp TIMESTAMP,
    span INTERVAL, doc JSON, docb JSONB, acl ACLITEM
)";

const FILTERS: &[&str] = &[
    "SELECT * FROM kinds WHERE big > 100",
    "SELECT * FROM kinds WHERE at > '2024-01-01 00:00:00+00'",
    "SELECT * FROM kinds WHERE amount > 1",
    "SELECT * FROM kinds WHERE uid = '550e8400-e29b-41d4-a716-446655440000'",
    "SELECT * FROM kinds WHERE body = 'héllo'",
    "SELECT * FROM kinds WHERE single > 0.05",
    "SELECT * FROM kinds WHERE span IS NOT NULL",
    "SELECT * FROM kinds WHERE docb IS NOT NULL",
];

// `aclitem`, the enum and the domain have no diesel SQL type, so the DML is raw.
const STATEMENTS: &[&str] = &[
    "INSERT INTO kinds VALUES (7, 9000000000, '2024-03-15 14:34:56.789+02', 12.500, \
     '550e8400-e29b-41d4-a716-446655440000', 'héllo', TRUE, -3, 0.1, 2.5, 'ab', 'ab', \
     '\\xdead', '2024-03-15', '12:34:56.789', '2024-03-15 12:34:56.789', \
     '1 year 2 mons 3 days 04:05:06', '{\"a\": 1}', '{\"a\": 1}', \
     makeaclitem(0, (SELECT oid FROM pg_roles WHERE rolname = current_user), 'SELECT', FALSE), \
     'ok', 5)",
    "UPDATE kinds SET amount = 99.990, body = 'bye', single = 1.5, feel = 'sad' WHERE id = 7",
    "DELETE FROM kinds WHERE id = 7",
    "INSERT INTO plain VALUES (7, 9000000000, '2024-03-15 14:34:56.789+02', 12.500, \
     '550e8400-e29b-41d4-a716-446655440000', 'héllo', TRUE, -3, 0.1, 2.5, 'ab', 'ab', \
     '\\xdead', '2024-03-15', '12:34:56.789', '2024-03-15 12:34:56.789', \
     '1 year 2 mons 3 days 04:05:06', '{\"a\": 1}', '{\"a\": 1}', \
     makeaclitem(0, (SELECT oid FROM pg_roles WHERE rolname = current_user), 'SELECT', FALSE))",
    "UPDATE plain SET amount = 99.990, body = 'bye', single = 1.5, span = '-1 days' WHERE id = 7",
    "DELETE FROM plain WHERE id = 7",
];

type Engine = SubscriptionEngine<PgChangeEvent, DefaultIds, ParserDB>;

fn engine(catalog: &ParserDB) -> Engine {
    let mut engine = Engine::new(catalog.clone(), PostgreSqlDialect {});
    for (consumer, filter) in (1u64..).zip(FILTERS) {
        engine
            .register(SubscriptionRequest::new(consumer, *filter))
            .unwrap_or_else(|e| panic!("{filter} registers: {e}"));
    }
    engine
}

/// The first `count` row events of `source`, once it has let go of the slot.
async fn drain_and_release(mut source: PollingPgCdcSource, count: usize) -> Vec<PgChangeEvent> {
    let mut events = Vec::new();
    while events.len() < count {
        let item = tokio::time::timeout(Duration::from_secs(10), source.next_item())
            .await
            .expect("an item within the timeout")
            .expect("the source reads")
            .expect("the source is open");
        if let Some(event) = item.into_event() {
            events.push(event);
        }
    }
    let exited = source.task_exited_handle();
    drop(source);
    let released = std::time::Instant::now();
    while !exited.load(std::sync::atomic::Ordering::Relaxed) {
        assert!(
            released.elapsed() < Duration::from_secs(10),
            "the poller exits"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    events
}

/// The format byte of column `name` in the event's first row image.
fn carried(event: &PgChangeEvent, name: &str) -> Option<&'static str> {
    let image = match &event.change().event_type {
        EventType::Insert { data, .. } => data,
        EventType::Update { new_data, .. } => new_data,
        EventType::Delete { old_data, .. } => old_data,
        other => panic!("not a row event: {other:?}"),
    };
    image.get(name).map(|cell| match cell {
        ColumnValue::Text(_) => "text",
        ColumnValue::Binary(_) => "binary",
        ColumnValue::Null => "null",
    })
}

/// The rows of `plain` emit the same patchset and changeset bytes from
/// either stream. The enum column of `kinds` arrives binary and travels as
/// its text output, which its send bytes cannot become, so the binary
/// stream refuses to emit it.
#[cfg(feature = "pgoutput-emit")]
fn emits_alike(catalog: &ParserDB, text: &[PgChangeEvent], binary: &[PgChangeEvent]) {
    use sqlite_diff_rs::wire::{ConversionError, DecodeError};
    use subql::emit::{pgoutput_changeset, pgoutput_patchset};

    let changes = |events: &[PgChangeEvent], table: &str| -> Vec<pg_walstream::ChangeEvent> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    &event.change().event_type,
                    EventType::Insert { table: name, .. }
                        | EventType::Update { table: name, .. }
                        | EventType::Delete { table: name, .. } if &**name == table
                )
            })
            .map(|event| event.change().clone())
            .collect()
    };
    let (text_plain, binary_plain) = (changes(text, "plain"), changes(binary, "plain"));
    assert_eq!(text_plain.len(), 3);
    assert_eq!(
        pgoutput_patchset(catalog, &binary_plain).expect("binary emits"),
        pgoutput_patchset(catalog, &text_plain).expect("text emits"),
    );
    assert_eq!(
        pgoutput_changeset(catalog, &binary_plain).expect("binary emits"),
        pgoutput_changeset(catalog, &text_plain).expect("text emits"),
    );

    pgoutput_patchset(catalog, &changes(text, "kinds")).expect("text emits an enum");
    assert_eq!(
        pgoutput_patchset(catalog, &changes(binary, "kinds")[..1]),
        Err(ConversionError::Decode(DecodeError::NoDecoderForType {
            column: "feel".into()
        }))
    );
}

/// Every cell of `binary` reads as its counterpart in `text`, and both
/// dispatch alike.
fn reads_alike(catalog: &ParserDB, text: &[PgChangeEvent], binary: &[PgChangeEvent]) {
    let (mut text_engine, mut binary_engine) = (engine(catalog), engine(catalog));
    assert_eq!(text.len(), binary.len());
    for (from_text, from_binary) in text.iter().zip(binary) {
        assert_eq!(from_text.kind(), from_binary.kind());
        assert_eq!(from_text.position(), from_binary.position());
        let table = from_text.table_id(catalog);
        let arity = u16::try_from(catalog_helpers::table_arity(catalog, table).unwrap()).unwrap();
        for row in [RowKind::New, RowKind::Old, RowKind::Pk] {
            for col in 0..arity {
                let read = from_text.value_at(catalog, row, col);
                let inserted = from_text.kind() == EventKind::Insert && row == RowKind::New;
                if inserted && col < BUILTIN_COLUMNS {
                    assert!(
                        !matches!(read, Ok(Value::Missing)),
                        "column {col} decodes from text"
                    );
                }
                assert_eq!(
                    from_binary.value_at(catalog, row, col),
                    read,
                    "{:?} {row:?} column {col}",
                    from_text.kind()
                );
            }
        }
        assert_eq!(
            decisions(&mut binary_engine, from_binary),
            decisions(&mut text_engine, from_text),
        );
    }
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn binary_and_text_streams_read_and_dispatch_alike() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    for ddl in TYPES {
        sql_query(ddl).execute(&mut setup).expect("create type");
    }
    for table in [TABLE, PLAIN] {
        sql_query(table).execute(&mut setup).expect("create table");
    }
    for table in ["kinds", "plain"] {
        sql_query(format!("ALTER TABLE {table} REPLICA IDENTITY FULL"))
            .execute(&mut setup)
            .expect("REPLICA IDENTITY FULL");
    }
    let slot = db.slot("subql_binary_parity");
    let publication = "subql_binary_parity_pub";
    common::create_publication(&mut setup, publication, "kinds, plain");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(&format!(
        "{}; {}; {TABLE}; {PLAIN};",
        TYPES[0], TYPES[1]
    ))
    .expect("parse DDL");
    let config = || {
        PollingPgCdcConfig::new(db.url(), &slot, publication)
            .poll_interval(Duration::from_millis(20))
    };

    for statement in STATEMENTS {
        sql_query(*statement).execute(&mut setup).expect(statement);
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            // A peek holds the slot, so each source reads alone and lets go first.
            let text = PollingPgCdcSource::connect(config(), catalog.clone())
                .await
                .expect("text source");
            let text_events = drain_and_release(text, STATEMENTS.len()).await;
            let binary = PollingPgCdcSource::connect(config().binary(true), catalog.clone())
                .await
                .expect("binary source");
            let binary_events = drain_and_release(binary, STATEMENTS.len()).await;

            assert_eq!(carried(&binary_events[0], "big"), Some("binary"));
            assert_eq!(carried(&binary_events[0], "feel"), Some("binary"));
            assert_eq!(carried(&binary_events[0], "n"), Some("binary"));
            assert_eq!(
                carried(&binary_events[0], "acl"),
                Some("text"),
                "aclitem has no send"
            );
            assert_eq!(carried(&text_events[0], "big"), Some("text"));
            reads_alike(&catalog, &text_events, &binary_events);
            #[cfg(feature = "pgoutput-emit")]
            emits_alike(&catalog, &text_events, &binary_events);

            let push = PgStreamingConfig::new(db.url(), &slot, publication).binary(true);
            let mut push = PgStreamingCdcSource::connect(push, catalog.clone())
                .await
                .expect("binary push source");
            let mut push_events = Vec::new();
            while push_events.len() < STATEMENTS.len() {
                let item = tokio::time::timeout(Duration::from_secs(10), push.next_item())
                    .await
                    .expect("an item within the timeout")
                    .expect("the source reads")
                    .expect("the source is open");
                push_events.extend(item.into_event());
            }
            assert_eq!(carried(&push_events[0], "feel"), Some("binary"));
            reads_alike(&catalog, &text_events, &push_events);
            #[cfg(feature = "pgoutput-emit")]
            emits_alike(&catalog, &text_events, &push_events);
        });

    common::drop_slot(&mut setup, &slot);
}

/// `array_send` raises when the element type has no send function, so a
/// binary source refuses a published `aclitem[]` column when it connects.
/// A column list that leaves the column out publishes nothing to refuse.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_binary_source_refuses_a_column_nesting_a_type_without_send() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    // DDL, which the query DSL does not express.
    sql_query("CREATE TABLE grants (id INT PRIMARY KEY, acls ACLITEM[])")
        .execute(&mut setup)
        .expect("create table");
    let slot = db.slot("subql_binary_unsendable");
    common::create_publication(&mut setup, "every_column", "grants");
    common::create_publication(&mut setup, "keys_only", "grants (id)");
    common::create_pgoutput_slot(&mut setup, &slot);
    let catalog = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE grants (id INT PRIMARY KEY, acls ACLITEM[]);",
    )
    .expect("parse DDL");
    let config = |publication: &str| PollingPgCdcConfig::new(db.url(), &slot, publication);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let refusal =
                PollingPgCdcSource::connect(config("every_column").binary(true), catalog.clone())
                    .await
                    .err()
                    .expect("the binary source refuses");
            let PollingPgCdcError::BinaryUnavailable(BinaryUnavailable::AtConnect(column)) =
                refusal
            else {
                panic!("expected an unsendable column, got {refusal}");
            };
            assert_eq!(
                (
                    column.table(),
                    column.column(),
                    column.unsendable_type_oid()
                ),
                ("grants", "acls", 1033)
            );
            PollingPgCdcSource::connect(config("every_column"), catalog.clone())
                .await
                .expect("a text source takes the column");
            PollingPgCdcSource::connect(config("keys_only").binary(true), catalog.clone())
                .await
                .expect("the column list leaves the column out");

            let push = |publication: &str| PgStreamingConfig::new(db.url(), &slot, publication);
            let refusal =
                PgStreamingCdcSource::connect(push("every_column").binary(true), catalog.clone())
                    .await
                    .err()
                    .expect("the binary push source refuses");
            let PgStreamingError::BinaryUnavailable(BinaryUnavailable::AtConnect(column)) = refusal
            else {
                panic!("expected an unsendable column, got {refusal}");
            };
            assert_eq!(
                (
                    column.table(),
                    column.column(),
                    column.unsendable_type_oid()
                ),
                ("grants", "acls", 1033)
            );
            drop(
                PgStreamingCdcSource::connect(push("every_column"), catalog.clone())
                    .await
                    .expect("a text push source takes the column"),
            );
            drop(
                PgStreamingCdcSource::connect(push("keys_only").binary(true), catalog.clone())
                    .await
                    .expect("the column list leaves the column out of the push source"),
            );
        });

    common::drop_slot(&mut setup, &slot);
}

/// `grants` as the catalog names it once `ADD_ACLS` has run.
const GRANTS_WITH_ACLS: &str = "CREATE TABLE grants (id INT PRIMARY KEY, acls ACLITEM[]);";

/// Adds a column a binary stream cannot carry after the source has
/// connected, which the connect-time check therefore never saw.
const ADD_ACLS: &str = "ALTER TABLE grants ADD COLUMN acls ACLITEM[]";

/// `grants` before its `aclitem[]` column exists, published as `grants_pub`,
/// with the slots `slots` created before its first row is written.
fn grants_before_acls(db: &common::PgDatabase, slots: &[&str]) -> diesel::PgConnection {
    let mut setup = db.connect();
    // DDL, which the query DSL does not express.
    sql_query("CREATE TABLE grants (id INT PRIMARY KEY)")
        .execute(&mut setup)
        .expect("create table");
    common::create_publication(&mut setup, "grants_pub", "grants");
    for slot in slots {
        common::create_pgoutput_slot(&mut setup, slot);
    }
    sql_query("INSERT INTO grants (id) VALUES (1)")
        .execute(&mut setup)
        .expect("first row");
    setup
}

/// Add the `aclitem[]` column and write a row holding a value in it.
fn add_acls_and_write_one(setup: &mut diesel::PgConnection) {
    sql_query(ADD_ACLS).execute(setup).expect("add column");
    // `aclitem` has no diesel SQL type, so the DML is raw.
    sql_query(
        "INSERT INTO grants (id, acls) VALUES (2, ARRAY[\
         makeaclitem(0, (SELECT oid FROM pg_roles WHERE rolname = current_user), 'SELECT', FALSE)])",
    )
        .execute(setup)
        .expect("second row");
}

/// The next item of `source`, failing the test past ten seconds.
async fn next_of<S: CdcSource>(source: &mut S) -> Result<Option<SourceItemOf<S>>, S::Error> {
    tokio::time::timeout(Duration::from_secs(10), source.next_item())
        .await
        .expect("an item within the timeout")
}

/// The position of the first commit `source` yields, after exactly one row.
async fn first_commit<S: CdcSource<Event = PgChangeEvent, Commit = subql::PgCommit>>(
    source: &mut S,
) -> PgCommitPosition {
    let mut rows = 0;
    loop {
        match next_of(source)
            .await
            .expect("the source reads")
            .expect("the source is open")
        {
            subql::SourceItem::Event(_) => rows += 1,
            subql::SourceItem::Relation(_) => {}
            subql::SourceItem::Commit(commit) => {
                assert_eq!(rows, 1, "the first transaction writes one row");
                return commit.position();
            }
        }
    }
}

/// The error `source` ends with, after yielding no further row.
async fn error_after_no_row<S: CdcSource>(source: &mut S) -> S::Error {
    loop {
        match next_of(source).await {
            Ok(Some(subql::SourceItem::Relation(_))) => {}
            Ok(Some(_)) => panic!("a row or commit past the refused row"),
            Ok(None) => panic!("the source closed without an error"),
            Err(error) => return error,
        }
    }
}

/// The first row `source` yields.
async fn first_row<S: CdcSource<Event = PgChangeEvent>>(source: &mut S) -> PgChangeEvent {
    loop {
        let item = next_of(source)
            .await
            .expect("the source reads")
            .expect("the source is open");
        if let Some(event) = item.into_event() {
            return event;
        }
    }
}

/// The server's refusal to send a value in binary, which names only the type.
fn assert_mid_stream_refusal(server: &ServerError) {
    assert_eq!(server.code, Some(pg::UNDEFINED_FUNCTION), "{server}");
    assert!(server.message.contains("aclitem"), "{server}");
}

/// A column added after a binary push source connects reaches the server's
/// send call, which ends the stream. The source reports it as binary
/// unavailable after the rows before it, and a text source resumed from the
/// last delivered commit reads the refused row.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_binary_push_source_reports_a_column_added_mid_stream() {
    common::assert_docker_available();
    let db = common::pg_database();
    let binary_slot = db.slot("subql_binary_mid_stream");
    // The walsender releases a slot only after it reports the error, and the
    // stream yields nothing at that release, so the text source resumes on a
    // second slot created at the same position.
    let text_slot = db.slot("subql_text_mid_stream");
    let mut setup = grants_before_acls(&db, &[&binary_slot, &text_slot]);
    let catalog = || ParserDB::parse::<PostgreSqlDialect>(GRANTS_WITH_ACLS).expect("parse DDL");

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let config = PgStreamingConfig::new(db.url(), &binary_slot, "grants_pub").binary(true);
            let mut binary = PgStreamingCdcSource::connect(config, catalog())
                .await
                .expect("no published column is refused yet");
            let delivered = first_commit(&mut binary).await;
            add_acls_and_write_one(&mut setup);

            let error = error_after_no_row(&mut binary).await;
            let PgStreamingError::BinaryUnavailable(BinaryUnavailable::MidStream(server)) = &error
            else {
                panic!("expected a mid-stream refusal, got {error}");
            };
            assert_mid_stream_refusal(server);
            drop(binary);

            let config =
                PgStreamingConfig::new(db.url(), &text_slot, "grants_pub").start(Some(delivered));
            let mut text = PgStreamingCdcSource::connect(config, catalog())
                .await
                .expect("text source");
            let row = first_row(&mut text).await;
            assert_eq!(carried(&row, "acls"), Some("text"), "the refused row");
        });

    for slot in [&binary_slot, &text_slot] {
        common::drop_slot(&mut setup, slot);
    }
}

/// A column added after a binary polling source connects fails its peek.
/// The source reports it as binary unavailable, and a text source on the
/// same slot, which the failed peek released, reads the refused row once
/// the delivered commit is acknowledged.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_binary_polling_source_reports_a_column_added_mid_stream() {
    common::assert_docker_available();
    let db = common::pg_database();
    let slot = db.slot("subql_binary_polling_mid_stream");
    let mut setup = grants_before_acls(&db, &[&slot]);
    let catalog = || ParserDB::parse::<PostgreSqlDialect>(GRANTS_WITH_ACLS).expect("parse DDL");
    let config = || {
        PollingPgCdcConfig::new(db.url(), &slot, "grants_pub")
            .poll_interval(Duration::from_millis(20))
    };

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut binary = PollingPgCdcSource::connect(config().binary(true), catalog())
                .await
                .expect("no published column is refused yet");
            let delivered = first_commit(&mut binary).await;
            // The loop consumes an acknowledged commit before its next peek.
            binary.ack(delivered).await.expect("ack");
            add_acls_and_write_one(&mut setup);

            let error = error_after_no_row(&mut binary).await;
            let PollingPgCdcError::BinaryUnavailable(BinaryUnavailable::MidStream(server)) = &error
            else {
                panic!("expected a mid-stream refusal, got {error}");
            };
            assert_mid_stream_refusal(server);
            drop(binary);

            let mut text = PollingPgCdcSource::connect(config(), catalog())
                .await
                .expect("text source");
            let row = first_row(&mut text).await;
            assert_eq!(carried(&row, "acls"), Some("text"), "the refused row");
        });

    common::drop_slot(&mut setup, &slot);
}
