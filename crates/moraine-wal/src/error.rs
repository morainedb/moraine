//! The crate's error type: two failure domains, transport and corruption.

/// Substrings a SQL-shaped retry loop keys its retry decision on. No message
/// this crate builds may contain one, so store-supplied text is redacted
/// before it is embedded.
const RETRY_SUBSTRINGS: [&str; 4] = ["conflict", "concurrent", "unique", "primary key"];

/// What a redacted retry substring is replaced with.
const REDACTION: &str = "[redacted]";

/// Errors returned by commit-slot log operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The operation could not complete; a put's outcome may be unknown, and
    /// the same sequence may be retried.
    #[error("commit-slot log unavailable: {0}")]
    Transport(String),

    /// The slot's bytes are not a valid envelope, or the log is damaged.
    #[error("commit-slot log corruption: {0}")]
    Corruption(String),
}

impl Error {
    /// A transport failure whose message may embed store-supplied text.
    pub(crate) fn transport(message: String) -> Self {
        Self::Transport(redact_retry_substrings(message))
    }

    /// A corruption failure whose message may embed decoder-supplied text.
    pub(crate) fn corruption(message: String) -> Self {
        Self::Corruption(redact_retry_substrings(message))
    }
}

/// Replaces every [`RETRY_SUBSTRINGS`] occurrence, in any ASCII case, with
/// [`REDACTION`].
fn redact_retry_substrings(text: String) -> String {
    let mut redacted = text;
    for pattern in RETRY_SUBSTRINGS {
        let mut from = 0;
        // ASCII-lowercasing preserves byte offsets, and an ASCII pattern can
        // only match at a character boundary.
        while let Some(offset) = redacted[from..].to_ascii_lowercase().find(pattern) {
            let at = from + offset;
            redacted.replace_range(at..at + pattern.len(), REDACTION);
            from = at + REDACTION.len();
        }
    }

    redacted
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// A store's own text reaches the embedder's retry loop, so a 5xx body
    /// that happens to say "conflicting" must not read as a conflict.
    #[test]
    fn store_supplied_text_loses_the_retry_substrings() {
        let err = Error::transport(
            "slot 7: Generic S3 error: response body: <Error><Code>OperationAborted</Code>\
             <Message>A conflicting conditional operation is currently in progress</Message>\
             </Error> (CONCURRENT writes to a UNIQUE primary key)"
                .to_string(),
        );

        let text = err.to_string();
        for substring in RETRY_SUBSTRINGS {
            assert!(
                !text.to_ascii_lowercase().contains(substring),
                "{text:?} still contains {substring:?}"
            );
        }
        // Redaction is surgical: the rest of the diagnostic survives.
        assert!(text.contains("OperationAborted"), "{text}");
        assert!(text.contains("slot 7"), "{text}");
    }

    #[test]
    fn corruption_text_is_redacted_too() {
        let text = Error::corruption("envelope: CONFLICT".to_string()).to_string();
        assert!(!text.to_ascii_lowercase().contains("conflict"), "{text}");
    }

    proptest! {
        // Redaction is total: any text, including multi-byte characters
        // around a match, comes back free of every substring.
        #[test]
        fn arbitrary_text_never_keeps_a_retry_substring(text in ".*") {
            let redacted = redact_retry_substrings(text).to_ascii_lowercase();
            for substring in RETRY_SUBSTRINGS {
                prop_assert!(!redacted.contains(substring));
            }
        }
    }
}
