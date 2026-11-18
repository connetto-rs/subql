//! The resolve path: what a triggered read answers, and how it fails.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// Full path through the async engine: register, snapshot (which
/// installs), unrelated insert (no connector call), delete of the
/// extreme (one connector call, ScalarUpdate emitted).
#[test]
fn async_engine_dispatch_round_trip() {
    // Two values for: snapshot bootstrap (5.0), delete re-execution (9.0).
    // Mock pops from the back so push in reverse order.
    let (mut e, tid) = engine_with_values(vec![Value::Float(9.0), Value::Float(5.0)]);

    let qid = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );

    // Snapshot bootstraps. Future is Send-bound and ready immediately.
    let snap = block_on(e.snapshot(qid))
        .unwrap()
        .expect("subscription_id exists");
    match snap {
        SnapshotResult::Scalar(Value::Float(v), None) => {
            assert!((v - 5.0).abs() < f64::EPSILON);
        }
        other => panic!("expected Scalar(5.0, None), got {other:?}"),
    }
    assert_eq!(e.connector().call_count(), 1);

    // Insert above the extreme: in-process Unchanged, no connector call.
    let n = e
        .apply_leaving_reads_queued(&insert_event(tid, 2, 9.0))
        .unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(e.connector().call_count(), 1);

    // Delete the extreme: trigger -> connector -> ScalarUpdate.
    let settled = block_on(
        e.apply(&delete_event(tid, 1, 5.0))
            .unwrap()
            .resolve_collect(),
    );
    let n = settled.reads.unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
    assert_eq!(e.connector().call_count(), 2);
}

/// `unrelated-column` UPDATE skip optimization still works under
/// the async engine.
#[test]
fn async_engine_unrelated_column_update_skips_connector() {
    let (mut e, tid) = engine_with_values(vec![]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MAX(price) FROM orders",
        10.0,
    );

    let event = update_status_only(tid, 1, 10.0);

    let n = e.apply_leaving_reads_queued(&event).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(n.outstanding, 0, "nothing was queued either");
    assert_eq!(e.connector().call_count(), 0);
}

/// `snapshot` on an unknown id returns `Ok(None)`.
#[test]
fn async_engine_snapshot_unknown_query_returns_none() {
    let (mut e, _tid) = engine_with_values(vec![]);
    assert!(block_on(e.snapshot(99999)).unwrap().is_none());
    assert_eq!(e.connector().call_count(), 0);
}

/// Connector failure aborts the batch with `ReExecError::Connector`.
#[test]
fn async_engine_connector_error_aborts_batch() {
    let (mut e, tid) = engine_with_values(vec![]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    match block_on(
        e.apply(&delete_event(tid, 1, 5.0))
            .unwrap()
            .resolve_collect(),
    )
    .reads
    {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError::Unstaged(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}

#[test]
fn async_connector_error_names_its_subscription() {
    let (mut e, tid) = engine_with_values(vec![]);
    let qid = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
            fence: None,
        },
    )
    .unwrap();
    match block_on(
        e.apply(&delete_event(tid, 1, 5.0))
            .unwrap()
            .resolve_collect(),
    )
    .reads
    {
        Ok(_) => panic!("expected the triggered read to fail"),
        Err(ReExecError::Connector {
            subscription,
            error: MockError::Unstaged(msg),
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert_eq!(msg, "queue empty");
        }
        Err(other) => panic!("expected Connector naming its subscription, got {other:?}"),
    }
}

#[test]
fn async_cursor_error_names_its_subscription() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::WholeRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected WholeRows, got {other:?}"),
    };
    match block_on(e.snapshot(qid)) {
        Ok(_) => panic!("expected the cursorless read to fail"),
        Err(ReExecError::Cursor {
            subscription,
            error,
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert!(matches!(error, crate::reexec::CursorError::Unsupported));
        }
        Err(other) => panic!("expected Cursor naming its subscription, got {other:?}"),
    }
}

/// A group read for an unadopted answer is refused, not aborted on.
///
/// Planning a grouped read needs no context, because the trigger carries
/// its own query, but running one does: the connector is handed the auth
/// the subscription registered under. An engine restored without adopting
/// has neither, and the read has to say so.
#[test]
fn a_grouped_read_without_a_context_is_refused_async() {
    let (mut e, _tid) = engine_with_values(Vec::new());
    // One adopted answer alongside, so a lookup that ignores the trigger's
    // own id finds a context anyway and the read wrongly proceeds.
    crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    e.enqueue_read(crate::reexec::ReExecutionTrigger {
        subscription_id: 7,
        consumer_id: 1u64,
        read: crate::reexec::ReExecutionRead::GroupedScalar {
            group: vec![1],
            query: crate::reexec::BoundQuery::new(
                "SELECT MIN(price) FROM orders GROUP BY status".to_string(),
                Vec::new(),
            ),
            column_kinds: [ScalarFamily::String, ScalarFamily::Float],
        },
        checkpoint: None,
    });

    let err = block_on(e.resolve(|_| {})).expect_err("the read cannot run");
    assert!(
        matches!(err, ReExecError::Unadopted { subscription: 7 }),
        "the refusal names the unadopted answer, got {err:?}"
    );
}

#[test]
fn permanent_read_failure_stops_only_its_subscription() {
    let (mut engine, table_id) = engine_with_values(vec![Value::Float(9.0)]);
    for consumer_id in [11, 22] {
        crate::reexec::test_fixtures::bootstrap_scalar_query(
            &mut engine,
            consumer_id,
            "SELECT MIN(price) FROM orders",
            5.0,
        );
    }
    engine
        .connector()
        .scalar_failures
        .lock()
        .extend([None, Some(MockError::Permanent)]);
    engine
        .apply_leaving_reads_queued(&delete_event(table_id, 1, 5.0))
        .unwrap();
    let triggers = engine.pending_reads.snapshot();
    let deliveries = block_on(engine.resolve_collect()).unwrap();
    assert_eq!(deliveries.stopped.len(), 1);
    assert_eq!(deliveries.scalar_updates.len(), 1);
    let stopped = &deliveries.stopped[0];
    assert!(matches!(
        stopped.error,
        ReExecError::Connector {
            error: MockError::Permanent,
            ..
        }
    ));
    assert!(triggers.iter().any(|trigger| {
        trigger.subscription_id == stopped.subscription_id
            && trigger.consumer_id == stopped.consumer_id
    }));
    assert_ne!(
        deliveries.scalar_updates[0].subscription_id,
        stopped.subscription_id
    );
    assert!(!engine.contexts.contains_key(&stopped.subscription_id));
    assert_eq!(engine.pending_read_count(), 0);
    assert!(!engine.unregister_subscription(stopped.subscription_id));
    assert!(block_on(engine.snapshot(stopped.subscription_id))
        .unwrap()
        .is_none());
    engine
        .apply_leaving_reads_queued(&delete_event(table_id, 2, 9.0))
        .unwrap();
    assert!(engine
        .pending_reads
        .snapshot()
        .iter()
        .all(|trigger| { trigger.subscription_id != stopped.subscription_id }));
}

#[test]
fn transient_read_failure_keeps_subscription_and_delivers_healthy_answer() {
    let (mut engine, table_id) = engine_with_values(vec![Value::Float(9.0)]);
    for consumer_id in [11, 22] {
        crate::reexec::test_fixtures::bootstrap_scalar_query(
            &mut engine,
            consumer_id,
            "SELECT MIN(price) FROM orders",
            5.0,
        );
    }
    engine
        .connector()
        .scalar_failures
        .lock()
        .extend([None, Some(MockError::Unstaged("temporarily unavailable"))]);
    engine
        .apply_leaving_reads_queued(&delete_event(table_id, 1, 5.0))
        .unwrap();
    let mut deliveries = crate::reexec::ResolvedReads::default();
    let failure = block_on(engine.resolve(|delivery| deliveries.push(delivery))).unwrap_err();
    let ReExecError::Connector { subscription, .. } = failure else {
        panic!("expected connector failure");
    };
    assert!(deliveries.stopped.is_empty());
    assert_eq!(deliveries.scalar_updates.len(), 1);
    assert_ne!(deliveries.scalar_updates[0].subscription_id, subscription);
    assert!(engine.contexts.contains_key(&subscription));
    assert_eq!(engine.pending_read_count(), 1);
    engine.connector().values.lock().push(Value::Float(12.0));
    let retried = block_on(engine.resolve_collect()).unwrap();
    assert_eq!(retried.scalar_updates.len(), 1);
    assert_eq!(retried.scalar_updates[0].subscription_id, subscription);
    assert_eq!(engine.pending_read_count(), 0);
}

#[test]
fn permanent_outcome_discards_same_subscription_answers_in_either_order() {
    for permanent_first in [false, true] {
        let (mut engine, table_id) = engine_with_values(vec![]);
        crate::reexec::test_fixtures::bootstrap_scalar_query(
            &mut engine,
            11,
            "SELECT MIN(price) FROM orders",
            5.0,
        );
        engine
            .apply_leaving_reads_queued(&delete_event(table_id, 1, 5.0))
            .unwrap();
        let trigger = engine.pending_reads.snapshot().pop().unwrap();
        let subscription_id = trigger.subscription_id;
        let success = Ok((
            trigger.clone(),
            Resolved::Scalar {
                value: Value::Float(9.0),
                fence: None,
            },
        ));
        let failure = Err((
            trigger.clone(),
            ReExecError::Connector {
                subscription: subscription_id,
                error: MockError::Permanent,
            },
        ));
        let mut outcomes = if permanent_first {
            vec![failure, success]
        } else {
            vec![success, failure]
        };
        outcomes.push(Err((
            trigger.clone(),
            ReExecError::Connector {
                subscription: subscription_id,
                error: MockError::Permanent,
            },
        )));
        outcomes.push(Err((
            trigger,
            ReExecError::Connector {
                subscription: subscription_id,
                error: MockError::Unstaged("temporary"),
            },
        )));
        let mut deliveries = crate::reexec::ResolvedReads::default();
        let pages = vec![crate::reexec::engine::RowsUpdate {
            subscription_id,
            consumer_id: 11,
            generation: 1,
            columns: vec!["price".into()],
            rows: vec![vec![Value::Float(9.0)]],
            more: false,
            checkpoint: None,
        }];
        engine
            .apply_outcomes(outcomes, pages, &mut |delivery| {
                deliveries.push(delivery);
            })
            .unwrap();
        assert_eq!(deliveries.stopped.len(), 1);
        assert_eq!(deliveries.stopped[0].subscription_id, subscription_id);
        assert!(deliveries.scalar_updates.is_empty());
        assert!(deliveries.rows_updates.is_empty());
        assert_eq!(engine.pending_read_count(), 0);
        assert!(!engine.unregister_subscription(subscription_id));
    }
}
