//! The SlateDB layer: key layout and value codecs.
//!
//! Knows nothing about DuckLake semantics.

pub(crate) mod cache;
pub(crate) mod census;
pub(crate) mod compaction;
pub(crate) mod frame;
pub(crate) mod handle;
pub(crate) mod index_encoding;
pub(crate) mod inline;
pub(crate) mod key;
pub(crate) mod open;
pub(crate) mod proto;
pub(crate) mod read;
pub(crate) mod retry;
pub(crate) mod segment;
pub(crate) mod value;

/// Key and value bytes staged onto one write batch, before the store's own
/// per-entry framing.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StagedBytes(pub(crate) u64);

impl StagedBytes {
    /// Adds one staged key/value pair. A delete stages its key alone.
    pub(crate) fn add(&mut self, key: usize, value: usize) {
        self.0 = self
            .0
            .saturating_add(key as u64)
            .saturating_add(value as u64);
    }
}
