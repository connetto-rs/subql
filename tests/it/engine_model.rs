//! Generated engine sequences are compared with SQLite.
//!
//! The model's deterministic scenario exercises every required comparison kind.
//! The `fuzz_engine_model_sqlite` target accepts the same failing byte strings.

use proptest::collection::vec;
use proptest::prelude::any;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};

use subql::test_harnesses::harness_engine_model_sqlite;

/// Where a failing byte string is recorded.
const REGRESSIONS: &str = "tests/it/engine_model.proptest-regressions";

/// `SUBQL_SWEEP_ROWS` times two sequences per run, so the pull-request depth
/// is 96 and the weekly one 1200. A sequence opens a store and restarts, so
/// it costs about as much as fifty generated filters.
fn cases() -> u32 {
    std::env::var("SUBQL_SWEEP_ROWS")
        .ok()
        .and_then(|rows| rows.parse::<u32>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(48)
        .saturating_mul(2)
}

#[test]
fn generated_sequences_agree_with_sqlite() {
    let mut runner = TestRunner::new(Config {
        cases: cases(),
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(REGRESSIONS))),
        ..Config::default()
    });
    let outcome = runner.run(&vec(any::<u8>(), 256..2048), |bytes| {
        std::panic::catch_unwind(|| harness_engine_model_sqlite(&bytes)).map_err(|panic| {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("the harness panicked");
            TestCaseError::fail(message.to_string())
        })?;
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
}
