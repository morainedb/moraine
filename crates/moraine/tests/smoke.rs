//! Integration tests: exercise the public API only.

use moraine::Error;

#[test]
fn commit_conflict_displays_context() {
    let err = Error::CommitConflict("snapshot 42".to_string());
    assert_eq!(err.to_string(), "commit conflict: snapshot 42");
}

/// DuckLake's commit loop re-runs a failed commit whenever the error text
/// contains one of four substrings. A true conflict is retryable and keeps
/// them; an exhausted retry budget is terminal and must carry none, or
/// DuckLake spends its own budget re-running a commit that cannot settle.
#[test]
fn retry_budget_exhausted_avoids_ducklake_retry_substrings() {
    let text = Error::RetryBudgetExhausted("spent 10 attempts".to_string()).to_string();
    for substring in ["conflict", "concurrent", "unique", "primary key"] {
        assert!(
            !text.contains(substring),
            "{text:?} contains DuckLake's retry substring {substring:?}"
        );
    }
}

/// The transient counterpart still carries `conflict`: DuckLake retrying a
/// genuine race is correct, and the text is the wire contract that asks it to.
#[test]
fn commit_conflict_keeps_the_retry_substring() {
    assert!(
        Error::CommitConflict("concurrent commit 7 touched the same state".to_string())
            .to_string()
            .contains("conflict")
    );
}

/// The commit-slot log's failures cross into moraine's error type: an
/// unreachable log is terminal — not a conflict — so its text carries none
/// of the four substrings either; a corrupt slot is corruption.
///
/// The message stands in for what `moraine_wal` actually builds: the object
/// store's own Display, embedded verbatim except for the four substrings,
/// which `moraine_wal` redacts at construction (proven in that crate, which
/// can make a store fail). What this asserts is that the mapping adds none of
/// its own and passes a real store's diagnostic through intact.
#[test]
fn slot_log_errors_map_and_avoid_ducklake_retry_substrings() {
    let store_text = "slot 4: Generic S3 error: Error after 2 retries in 1.4s, \
                      max_retries: 3 ... HTTP status server error \
                      (503 Service Unavailable) for url (https://bucket.s3.amazonaws.com/\
                      cat/commits/00000000000000000004)";
    let transport = Error::from(moraine_wal::Error::Transport(store_text.to_string()));

    assert_eq!(
        transport.to_string(),
        format!("commit-slot log unavailable: {store_text}")
    );
    for substring in ["conflict", "concurrent", "unique", "primary key"] {
        assert!(
            !transport
                .to_string()
                .to_ascii_lowercase()
                .contains(substring),
            "{transport} contains DuckLake's retry substring {substring:?}"
        );
    }

    assert!(matches!(
        Error::from(moraine_wal::Error::Corruption(
            "slot: bad magic".to_string()
        )),
        Error::Corruption(_)
    ));
}

#[test]
fn logical_errors_display_context() {
    assert_eq!(
        Error::NotFound("table 9".to_string()).to_string(),
        "not found: table 9"
    );
    assert_eq!(
        Error::AlreadyExists("schema sales".to_string()).to_string(),
        "already exists: schema sales"
    );
    assert_eq!(
        Error::Constraint("cannot drop the last column".to_string()).to_string(),
        "constraint violation: cannot drop the last column"
    );
}
