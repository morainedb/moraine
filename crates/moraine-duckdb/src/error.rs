//! C-ABI error codes and the `(code, message)` pair carried across the
//! boundary.
//!
//! Every `extern "C"` entry point returns an [`i32`] code (`codes::OK` is
//! the only success value) and, on failure, may fill a caller-owned
//! [`MoraineError`] with the same code plus a heap-allocated message,
//! freed exactly once via
//! [`moraine_error_free`](crate::abi::moraine_error_free) — never `free()`.

use std::ffi::{CString, c_char};

/// Named error codes returned by every `moraine_*` entry point.
///
/// The C++ shim maps these to DuckDB exception kinds:
///
/// | Code | Meaning | Shim maps to |
/// |---|---|---|
/// | [`OK`](codes::OK) | success | — |
/// | [`NOT_FOUND`](codes::NOT_FOUND) | referenced entity does not exist | `CatalogException` |
/// | [`ALREADY_EXISTS`](codes::ALREADY_EXISTS) | name uniqueness violated | `CatalogException` |
/// | [`CONSTRAINT`](codes::CONSTRAINT) | structural constraint violated | `CatalogException` |
/// | [`COMMIT_CONFLICT`](codes::COMMIT_CONFLICT) | concurrent commit conflict; message contains the substring `conflict` | `TransactionException` |
/// | [`RETRY_EXHAUSTED`](codes::RETRY_EXHAUSTED) | the commit's internal retry budget ran out; message carries none of DuckLake's retry substrings | `TransactionException` |
/// | [`FENCED`](codes::FENCED) | another process took over as the writer; the handle can no longer commit | `IOException` |
/// | [`OPEN_RACED`](codes::OPEN_RACED) | another process created the store while this attach was creating it; nothing was written, and attaching again adopts it | `IOException` |
/// | [`CORRUPTION`](codes::CORRUPTION) | stored bytes failed to decode, or a catalog string cannot round-trip through a C string | `IOException` |
/// | [`STORE`](codes::STORE) | the underlying object store / SlateDB failed | `IOException` |
/// | [`INVALID_ARGUMENT`](codes::INVALID_ARGUMENT) | a null pointer, non-UTF-8 string, or unsupported ABI input | `InvalidInputException` |
/// | [`INTERNAL`](codes::INTERNAL) | a panic was caught at the FFI boundary | `InternalException` |
/// | [`INTERRUPTED`](codes::INTERRUPTED) | cancellation — the call's interrupt probe cancelled the read in flight (or about to start) on this handle | `InterruptException` |
pub mod codes {
    /// Success; no error occurred.
    pub const OK: i32 = 0;
    /// [`moraine::Error::NotFound`].
    pub const NOT_FOUND: i32 = 1;
    /// [`moraine::Error::AlreadyExists`].
    pub const ALREADY_EXISTS: i32 = 2;
    /// [`moraine::Error::Constraint`].
    pub const CONSTRAINT: i32 = 3;
    /// [`moraine::Error::CommitConflict`].
    pub const COMMIT_CONFLICT: i32 = 4;
    /// [`moraine::Error::Corruption`], and a catalog string with an
    /// embedded NUL byte.
    pub const CORRUPTION: i32 = 5;
    /// [`moraine::Error::Store`].
    pub const STORE: i32 = 6;
    /// A null pointer, invalid UTF-8, or unsupported argument value.
    /// ABI-layer only.
    pub const INVALID_ARGUMENT: i32 = 7;
    /// A panic was caught at the FFI boundary.
    pub const INTERNAL: i32 = 8;
    /// The call's interrupt probe cancelled it. ABI-layer only.
    pub const INTERRUPTED: i32 = 9;
    /// [`moraine::Error::RetryBudgetExhausted`]. Terminal: the message
    /// must carry none of the substrings DuckLake's commit loop retries on.
    pub const RETRY_EXHAUSTED: i32 = 10;
    /// [`moraine::Error::Fenced`]: another process took over as the
    /// writer; this handle can no longer commit. Terminal, and the message
    /// must avoid DuckLake's retry substrings.
    pub const FENCED: i32 = 11;
    /// [`moraine::Error::Migration`]: the store needs, is undergoing, or
    /// is newer than a structural format this binary supports.
    pub const MIGRATION: i32 = 12;
    /// [`moraine::Error::SnapshotExpired`]: a time-travel target fell
    /// below the retention horizon.
    pub const SNAPSHOT_EXPIRED: i32 = 13;
    /// [`moraine::Error::Unsupported`]: a DuckLake feature moraine does
    /// not implement.
    pub const UNSUPPORTED: i32 = 14;
    /// [`moraine::Error::OpenRaced`]: another process created this store
    /// while the attach was creating it. Nothing was written; attaching
    /// again adopts the store that won.
    pub const OPEN_RACED: i32 = 15;
}

/// Fixed message for a caught panic; never derived from the panic
/// payload.
pub(crate) const INTERNAL_PANIC_MESSAGE: &str =
    "moraine-duckdb: internal error (a panic was caught at the FFI boundary)";

/// An error to report back across the FFI boundary: a code plus an owned
/// message. Internal to the crate; [`write_into`](AbiError::write_into)
/// turns it into the C representation.
#[derive(Debug)]
pub(crate) struct AbiError {
    pub code: i32,
    pub message: String,
}

impl AbiError {
    pub(crate) fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(codes::INVALID_ARGUMENT, message)
    }

    /// Appends read-only-attach guidance when the code signals a missing or
    /// unreadable catalog ([`STORE`](codes::STORE) or
    /// [`CORRUPTION`](codes::CORRUPTION)), the shape a read-only open of an
    /// uninitialized store takes. Other codes pass through unchanged.
    pub(crate) fn with_read_only_attach_hint(mut self) -> Self {
        if self.code == codes::STORE || self.code == codes::CORRUPTION {
            self.message.push_str(
                "; a read-only attach cannot create a catalog — DuckDB opens remote \
                 paths (e.g. s3://) read-only by default, so to create or write a new \
                 lake add READ_WRITE to the ATTACH",
            );
        }
        self
    }

    /// The fixed error a cancellable read reports when the call's
    /// interrupt probe cancelled it.
    pub(crate) fn interrupted() -> Self {
        Self::new(
            codes::INTERRUPTED,
            "moraine-duckdb: operation was interrupted",
        )
    }

    /// Writes `self` into a caller-owned [`MoraineError`], if `err` is
    /// non-null. The message is sanitized (embedded NUL bytes stripped) so
    /// the `CString` construction below cannot fail.
    ///
    /// # Safety
    ///
    /// `err`, if non-null, must point to a valid, writable [`MoraineError`]
    /// for the duration of this call.
    pub(crate) unsafe fn write_into(self, err: *mut MoraineError) {
        if err.is_null() {
            return;
        }
        // The retry cannot fail after stripping; its fallback is unreachable.
        let c_message = CString::new(self.message).unwrap_or_else(|failed| {
            let mut bytes = failed.into_vec();
            bytes.retain(|byte| *byte != 0);
            CString::new(bytes).unwrap_or_default()
        });
        // SAFETY: caller contract above; checked non-null just above.
        unsafe {
            (*err).code = self.code;
            (*err).message = c_message.into_raw();
        }
    }
}

impl From<moraine::Error> for AbiError {
    fn from(err: moraine::Error) -> Self {
        let code = match &err {
            moraine::Error::NotFound(_) => codes::NOT_FOUND,
            moraine::Error::AlreadyExists(_) => codes::ALREADY_EXISTS,
            moraine::Error::Constraint(_) => codes::CONSTRAINT,
            // The shim's retry loop matches the literal substring "conflict"
            // in the message; core's `Display` includes it.
            moraine::Error::CommitConflict(_) => codes::COMMIT_CONFLICT,
            moraine::Error::RetryBudgetExhausted(_) => codes::RETRY_EXHAUSTED,
            moraine::Error::Fenced(_) => codes::FENCED,
            moraine::Error::Corruption(_) => codes::CORRUPTION,
            moraine::Error::Unsupported(_) => codes::UNSUPPORTED,
            moraine::Error::SnapshotExpired(_) => codes::SNAPSHOT_EXPIRED,
            moraine::Error::Interrupted(_) => codes::INTERRUPTED,
            moraine::Error::Migration(_) => codes::MIGRATION,
            moraine::Error::OpenRaced(_) => codes::OPEN_RACED,
            // Covers `Store`, `IndexBuilding`, `Configuration`, and any
            // future `#[non_exhaustive]` variant.
            _ => codes::STORE,
        };
        Self::new(code, error_chain(&err))
    }
}

/// Formats an error with its full `source()` chain.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// The `(code, message)` pair carried across the FFI boundary.
///
/// Caller-allocated and passed by pointer to every fallible `moraine_*`
/// entry point; on failure the callee fills in both fields. `message` is
/// null when there is nothing to free, and must be passed to
/// [`moraine_error_free`](crate::abi::moraine_error_free) exactly once;
/// the entry point never frees a previous message.
#[repr(C)]
#[derive(Debug)]
pub struct MoraineError {
    /// One of the [`codes`] constants.
    pub code: i32,
    /// A UTF-8, NUL-terminated, heap-allocated message, or null.
    pub message: *mut c_char,
}

impl Default for MoraineError {
    fn default() -> Self {
        Self {
            code: codes::OK,
            message: std::ptr::null_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_hint_added_for_missing_catalog_codes() {
        for code in [codes::STORE, codes::CORRUPTION] {
            let err = AbiError::new(code, "failed to find latest manifest version")
                .with_read_only_attach_hint();
            assert!(
                err.message.contains("READ_WRITE"),
                "code {code} message missing READ_WRITE hint: {}",
                err.message
            );
            // The original cause is preserved, not replaced.
            assert!(
                err.message
                    .contains("failed to find latest manifest version")
            );
        }
    }

    #[test]
    fn read_only_hint_left_off_unrelated_codes() {
        let err =
            AbiError::new(codes::INVALID_ARGUMENT, "bad argument").with_read_only_attach_hint();
        assert!(!err.message.contains("READ_WRITE"), "{}", err.message);
    }

    /// The `MORAINE_*` values hand-written in `cbindgen.toml` match the
    /// consts here, in value and in count.
    #[test]
    fn every_code_matches_the_c_enum_the_shim_switches_on() {
        let header = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("cpp/moraine_abi.h"),
        )
        .expect("read the generated ABI header");

        let declared: std::collections::HashMap<&str, i32> = header
            .lines()
            .filter_map(|line| line.trim().strip_prefix("MORAINE_"))
            .filter_map(|entry| {
                let (name, value) = entry.split_once(" = ")?;
                let value = value.trim_end_matches(',').parse().ok()?;
                Some((name, value))
            })
            .collect();

        let expected = [
            ("OK", codes::OK),
            ("NOT_FOUND", codes::NOT_FOUND),
            ("ALREADY_EXISTS", codes::ALREADY_EXISTS),
            ("CONSTRAINT", codes::CONSTRAINT),
            ("COMMIT_CONFLICT", codes::COMMIT_CONFLICT),
            ("CORRUPTION", codes::CORRUPTION),
            ("STORE", codes::STORE),
            ("INVALID_ARGUMENT", codes::INVALID_ARGUMENT),
            ("INTERNAL", codes::INTERNAL),
            ("INTERRUPTED", codes::INTERRUPTED),
            ("RETRY_EXHAUSTED", codes::RETRY_EXHAUSTED),
            ("FENCED", codes::FENCED),
            ("MIGRATION", codes::MIGRATION),
            ("SNAPSHOT_EXPIRED", codes::SNAPSHOT_EXPIRED),
            ("UNSUPPORTED", codes::UNSUPPORTED),
            ("OPEN_RACED", codes::OPEN_RACED),
        ];

        assert_eq!(
            declared.len(),
            expected.len(),
            "the header declares {} codes and this test knows {}; a code was added on one \
             side only",
            declared.len(),
            expected.len()
        );
        for (name, value) in expected {
            assert_eq!(
                declared.get(name),
                Some(&value),
                "MORAINE_{name} in the header does not match `codes::{name}`"
            );
        }
    }

    /// Every distinctly handled core variant carries its own code rather
    /// than the store catch-all.
    #[test]
    fn distinctly_handled_errors_do_not_fall_into_the_store_catch_all() {
        let sample = || "x".to_string();
        for (error, expected) in [
            (moraine::Error::NotFound(sample()), codes::NOT_FOUND),
            (
                moraine::Error::AlreadyExists(sample()),
                codes::ALREADY_EXISTS,
            ),
            (moraine::Error::Constraint(sample()), codes::CONSTRAINT),
            (
                moraine::Error::CommitConflict(sample()),
                codes::COMMIT_CONFLICT,
            ),
            (
                moraine::Error::RetryBudgetExhausted(sample()),
                codes::RETRY_EXHAUSTED,
            ),
            (moraine::Error::Fenced(sample()), codes::FENCED),
            (moraine::Error::OpenRaced(sample()), codes::OPEN_RACED),
            (moraine::Error::Corruption(sample()), codes::CORRUPTION),
            (moraine::Error::Unsupported(sample()), codes::UNSUPPORTED),
            (
                moraine::Error::SnapshotExpired(sample()),
                codes::SNAPSHOT_EXPIRED,
            ),
            (moraine::Error::Interrupted(sample()), codes::INTERRUPTED),
            (moraine::Error::Migration(sample()), codes::MIGRATION),
        ] {
            let rendered = format!("{error:?}");
            assert_eq!(AbiError::from(error).code, expected, "{rendered}");
        }
    }

    /// A lost commit's message still contains `conflict` after crossing
    /// this boundary (DuckLake retries by substring), and no other error a
    /// commit raises carries any of DuckLake's retry substrings.
    #[test]
    fn the_commit_conflict_message_keeps_its_retry_substring() {
        let conflict = AbiError::from(moraine::Error::CommitConflict(
            "a concurrent commit changed state this one read".to_string(),
        ));
        assert_eq!(conflict.code, codes::COMMIT_CONFLICT);
        assert!(
            conflict.message.to_lowercase().contains("conflict"),
            "the lost-commit message must stay retryable: {}",
            conflict.message
        );

        for error in [
            moraine::Error::RetryBudgetExhausted("budget".to_string()),
            moraine::Error::Fenced("fenced".to_string()),
            moraine::Error::Constraint("constrained".to_string()),
            moraine::Error::Unsupported("unsupported".to_string()),
        ] {
            let rendered = format!("{error:?}");
            let message = AbiError::from(error).message.to_lowercase();
            for substring in ["conflict", "concurrent", "unique", "primary key"] {
                assert!(
                    !message.contains(substring),
                    "{rendered} must not carry retry substring {substring:?}: {message}"
                );
            }
        }
    }
}
