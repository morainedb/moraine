//! Crate error types: one enum, variants per failure domain.
use tracing::warn;

/// Errors returned by moraine operations.
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
    ///
    /// The text carries none of the four substrings DuckLake's commit loop
    /// keys its retry decision on (`conflict`, `concurrent`, `unique`,
    /// `primary key`), so an exhausted budget surfaces at once instead of
    /// being re-run against a premise that already failed to settle ten
    /// times. That wording is part of the wire contract, not incidental
    /// diagnostics.
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
    /// `VARIANT` column, RFC 0005). Terminal: re-running cannot help.
    ///
    /// Like every non-conflict variant, the message avoids the four
    /// substrings DuckLake's commit loop retries on (`conflict`,
    /// `concurrent`, `unique`, `primary key`) — a caller must not word a
    /// payload so it trips a pointless re-drive.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A held or requested snapshot fell below the retention horizon and
    /// its record is gone; the reader must re-resolve from head rather
    /// than dereference reclaimed files. Not a conflict — the message
    /// stays clear of DuckLake's retry substrings.
    #[error("snapshot expired: {0}")]
    SnapshotExpired(String),

    /// A host interrupt cancelled the operation before its point of no
    /// return, or a durable write past that point never reported its
    /// outcome. Distinct from a store failure so the bridge can raise
    /// DuckDB's interrupt, and free of retry substrings — an interrupted
    /// commit whose durability is ambiguous must never be re-driven as a
    /// conflict.
    ///
    /// The two cases differ only in what the caller must do next: nothing
    /// after a clean pre-write cancellation, and a re-resolve of head
    /// after an unreported write, which may or may not have landed.
    #[error("interrupted: {0}")]
    Interrupted(String),

    /// The store requires, is undergoing, or was written by a structural
    /// format this binary does not support (RFC 0015). Terminal, and free
    /// of retry substrings: a migration is never a commit conflict.
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
    /// and a put's outcome may be unknown.
    ///
    /// Like [`Error::RetryBudgetExhausted`], the text carries none of the
    /// four substrings DuckLake's commit loop keys its retry decision on —
    /// an unreachable bucket is not a conflict, and re-driving the
    /// transaction against the same unreachable bucket would only fail
    /// again.
    #[error("commit-slot log unavailable: {0}")]
    SlotLog(String),

    /// Another process created this store while this open was creating it.
    /// Benign: the store exists now, so opening again adopts it. Distinct
    /// from [`Error::Fenced`], which displaces a writer that had already
    /// attached — here nothing was ever attached, and nothing was written.
    ///
    /// Only reachable on an empty store. Once a store exists, the writer
    /// epoch is claimed under a retry that absorbs the same race, so a
    /// second open takes over rather than failing.
    ///
    /// Free of the four substrings DuckLake's commit loop retries on: this
    /// is an attach-time race, not a commit conflict, and re-driving a
    /// commit would be the wrong response to it.
    #[error("open raced: {0}")]
    OpenRaced(String),

    /// The underlying store failed (SlateDB / object-store I/O).
    #[error("store error")]
    Store(#[source] Box<slatedb::Error>),
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

/// What SlateDB reports when a compare-and-swap loses to a version that
/// already landed — for moraine, the first manifest two processes both try
/// to create on an empty store.
///
/// Matched as text because the condition behind it is `pub(crate)` upstream:
/// this and a damaged manifest both arrive as [`slatedb::ErrorKind::Data`],
/// so the kind alone cannot separate a benign race from real corruption.
/// The crash-recovery suite pins this wording by failing a real manifest
/// CAS, so a SlateDB bump that rewords it fails there rather than silently
/// sending every genesis race back to the untyped [`Error::Store`].
pub(crate) const MANIFEST_VERSION_EXISTS: &str =
    "transactional object (e.g. manifest) version already exists";

/// The marker SlateDB's message carries when a data error came from the
/// write-ahead log rather than from the store's own objects.
const WAL_DATA_ERROR: &str = "wal data error";

impl From<slatedb::Error> for Error {
    fn from(err: slatedb::Error) -> Self {
        // Every store error crosses here, so this is the one place fencing
        // can be told apart from ordinary I/O failure.
        if err.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) {
            warn!("another process attached this catalog read-write; this writer is fenced");
            return Self::Fenced(
                "another process attached this catalog read-write and took over as \
                 the writer; this handle can no longer commit — re-attach to write"
                    .to_string(),
            );
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

    /// Only `CommitConflict` may carry a retry substring: it is the one
    /// error DuckLake should re-drive. Every other variant that can surface
    /// from a commit must be free of them, or an unretryable failure gets
    /// retried until the budget is spent. This pins the wording as the wire
    /// contract it is.
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

        // The variants' own prefixes carry no retry substring — a payload
        // that does is the caller's responsibility, so this asserts the
        // prefix alone by stripping the shared sample.
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
