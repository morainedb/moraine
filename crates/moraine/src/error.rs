//! Crate error types: one enum, variants per failure domain.
use tracing::warn;

/// Errors returned by moraine operations.
///
/// DuckLake's commit loop retries any error whose message contains
/// `conflict`, `concurrent`, `unique`, or `primary key`. Only
/// [`CommitConflict`](Self::CommitConflict) may carry one of those
/// substrings; every other variant's wording must stay clear of them.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Another writer committed a conflicting change; the transaction can be
    /// retried against the new state.
    #[error("commit conflict: {0}")]
    CommitConflict(String),

    /// A commit spent its whole internal retry budget on benign races
    /// without settling; the caller must re-drive the work itself, usually
    /// as smaller commits.
    #[error("retry budget exhausted: {0}")]
    RetryBudgetExhausted(String),

    /// Stored bytes failed to decode: corrupt, truncated, wrong-kind, or
    /// written by a newer encoding than this binary understands.
    #[error("corruption: {0}")]
    Corruption(String),

    /// An operation referenced an entity that does not exist (or is not
    /// live in the transaction's view).
    #[error("not found: {0}")]
    NotFound(String),

    /// An operation would violate name uniqueness.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// An operation would violate a structural constraint (e.g. dropping
    /// a schema that still contains tables).
    #[error("constraint violation: {0}")]
    Constraint(String),

    /// A DuckLake feature moraine does not implement (e.g. inlining a
    /// `VARIANT` column). Terminal: re-running cannot help.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A held or requested snapshot fell below the retention horizon and
    /// its record is gone; the reader must re-resolve from head.
    #[error("snapshot expired: {0}")]
    SnapshotExpired(String),

    /// A host interrupt cancelled the operation before its point of no
    /// return, or a durable write past that point never reported its
    /// outcome; after the latter the caller must re-resolve head.
    #[error("interrupted: {0}")]
    Interrupted(String),

    /// The store requires, is undergoing, or was written by a structural
    /// format this binary does not support. Terminal.
    #[error("migration required: {0}")]
    Migration(String),

    /// A lookup targeted an index whose staged backfill has not completed;
    /// it serves no reads until it flips ready.
    #[error("index building: {0}")]
    IndexBuilding(String),

    /// An environment or option value could not be parsed or is out of
    /// range.
    #[error("configuration: {0}")]
    Configuration(String),

    /// This writer has been fenced: another process opened the store
    /// read-write, and the newest writer wins.
    #[error("writer fenced: {0}")]
    Fenced(String),

    /// The commit-slot log could not be reached: a slot put or read failed,
    /// and a put's outcome may be unknown. The text carries none of the four
    /// substrings DuckLake's commit loop keys its retry decision on — an
    /// unreachable bucket is not a conflict.
    #[error("commit-slot log unavailable: {0}")]
    SlotLog(String),

    /// Another process created this store while this open was creating it;
    /// nothing was written, and opening again adopts it. Only reachable on
    /// an empty store.
    #[error("open raced: {0}")]
    OpenRaced(String),

    /// The underlying store failed (SlateDB / object-store I/O).
    #[error("store error")]
    Store(#[source] Box<slatedb::Error>),

    /// Concurrent modification detected during index maintenance.
    #[error("concurrent modification")]
    ConcurrentModification,
}

impl From<moraine_wal::Error> for Error {
    fn from(err: moraine_wal::Error) -> Self {
        match err {
            moraine_wal::Error::Transport(message) => Self::SlotLog(message),
            moraine_wal::Error::Corruption(message) => Self::Corruption(message),
            // The wal error type is `#[non_exhaustive]`: an unrecognized
            // failure is a log failure, never silently retryable.
            other => Self::SlotLog(other.to_string()),
        }
    }
}

/// What SlateDB reports when a manifest compare-and-swap loses to a version
/// that already landed. Matched as text because SlateDB reports it and a
/// damaged manifest under the same [`slatedb::ErrorKind::Data`]; the
/// crash-recovery suite pins the wording.
pub(crate) const MANIFEST_VERSION_EXISTS: &str =
    "transactional object (e.g. manifest) version already exists";

/// The marker SlateDB's message carries when a data error came from the
/// write-ahead log rather than from the store's own objects.
const WAL_DATA_ERROR: &str = "wal data error";

/// The fenced error, worded once for every path that reports it.
pub(crate) fn fenced() -> Error {
    warn!("another process attached this catalog read-write; this writer is fenced");
    Error::Fenced(
        "another process attached this catalog read-write and took over as \
         the writer; this handle can no longer commit — re-attach to write"
            .to_string(),
    )
}

impl From<slatedb::Error> for Error {
    fn from(err: slatedb::Error) -> Self {
        if err.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) {
            return fenced();
        }

        if err.kind() == slatedb::ErrorKind::Data
            && err.to_string().contains(MANIFEST_VERSION_EXISTS)
        {
            warn!("another process created this store first; nothing was written");
            return Self::OpenRaced(
                "another process created this store while this open was creating it; \
                 nothing was written — open again to adopt the store it created"
                    .to_string(),
            );
        }

        // Damage the store reports against its write-ahead log is damage to
        // the commit-slot log, which the store reads as one — a destroyed slot
        // arrives here. Other data errors are the store's own and stay store
        // errors; a missing manifest is how an uninitialized store reads.
        if err.kind() == slatedb::ErrorKind::Data && err.to_string().contains(WAL_DATA_ERROR) {
            return Self::Corruption(err.to_string());
        }

        Self::Store(Box::new(err))
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::Error;

    /// The substrings DuckLake's commit loop lowercases the error message
    /// and scans for, retrying the commit if any is present.
    const RETRY_SUBSTRINGS: [&str; 4] = ["conflict", "concurrent", "unique", "primary key"];

    /// Only `CommitConflict` renders with a retry substring.
    #[test]
    fn only_commit_conflict_carries_a_retry_substring() {
        let sample = "index \"unique_by_primary key\" saw a concurrent conflict";
        let non_retryable = [
            Error::RetryBudgetExhausted(sample.into()),
            Error::Corruption(sample.into()),
            Error::NotFound(sample.into()),
            Error::AlreadyExists(sample.into()),
            Error::Constraint(sample.into()),
            Error::IndexBuilding(sample.into()),
            Error::Configuration(sample.into()),
            Error::Fenced(sample.into()),
            Error::OpenRaced(sample.into()),
            Error::Unsupported(sample.into()),
            Error::SnapshotExpired(sample.into()),
            Error::Interrupted(sample.into()),
            Error::Migration(sample.into()),
        ];

        // Payload wording is the caller's responsibility; only the prefix
        // is asserted.
        for err in non_retryable {
            let rendered = err.to_string();
            let prefix = rendered
                .strip_suffix(sample)
                .expect("every variant renders as `<prefix>{sample}`")
                .to_lowercase();
            for needle in RETRY_SUBSTRINGS {
                assert!(
                    !prefix.contains(needle),
                    "{prefix:?} carries retry substring {needle:?}"
                );
            }
        }

        assert!(
            Error::CommitConflict(String::new())
                .to_string()
                .contains("conflict"),
            "CommitConflict must stay retryable"
        );
    }
}
