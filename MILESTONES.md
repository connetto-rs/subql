# MILESTONES

Deferred work tracked here, with the decisions and design context that have already been made so the next pass can pick up without re-deriving anything.

---

## Aggregate IVM v2

### Status

Live folding aggregates use in-process scalar and per-group accumulators. Scalar and grouped `MIN` and `MAX` use targeted source reads. [`ReplayDetector`](REPLAY_STATE.md) maintains its admitted single-table results from retained inputs and checkpoints the native fold and candidate state needed for recovery.

### Shape

Live folds seed their accumulators from source reads and apply admitted row deltas, including group creation, removal and existing `HAVING` visibility. An `UPDATE` that changes a group key contributes to the old and new groups.

Replay operators retain complete dependency input bags, native fold accumulators and extrema candidates. Their versioned `CBOR` checkpoints bind this state to the compiled plan and dependent catalog fingerprints.

### Architecturally excluded (do not add)

Captured so they do not drift back in by accident.

- Ordered extrema candidate trees. Live extrema use targeted reads, and replay extrema use retained candidate bags.
- In-process `JOIN` maintenance. Multi-table joins require database re-execution.
- Approximate distinct counting. Its semantics require a separate contract.
- Persistence of live-only accumulators. The replay operator's persistence contract is defined in [REPLAY_STATE.md](REPLAY_STATE.md).

---

## Concrete `AsyncConnector` implementation

### Status

Deferred. The `AsyncConnector` trait and `AsyncAutoResolvingEngine` shipped and are exercised by in-tree mocks (`MockAsyncConnector`, plus the throttle-test connectors). No production async connector exists. No `sqlx` or `diesel-async` dependency is in `Cargo.toml`.

### Design context already settled

- async-fn-in-trait with `+ Send` bounds on every method future, so it works on multi-threaded runtimes. A `!Send` consumer implements its own trait variant.
- Concurrent re-execution in `consumers_batch` runs through `FuturesUnordered` capped at `max_concurrent_reexecutions` (already wired on the async engine).
- Open choice: `sqlx` vs `diesel-async` for the first concrete impl. Picking one does not require a breaking bump, the trait surface is frozen.

---

## Other deferred items

Track future deferred work here as it gets pushed past v1.
