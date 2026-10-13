//! The plug boundary for a bucket-resident commit log: a library that keeps
//! a store's commits as slot objects supplies SlateDB write-ahead-log plugs
//! here, and the catalog opens its stores over them. This crate holds the
//! boundary and nothing of the log; a store stamped for the log cannot be
//! opened without a provider.
//!
//! A host supplies a provider one of two ways: per open, through
//! [`CatalogOptions::commit_log`](crate::CatalogOptions::commit_log), or
//! once per process, through [`register_commit_log`], which is how a
//! library linked into the DuckDB extension reaches an attach it never sees
//! the options of. The per-open provider outranks the registered one.
//!
//! In this version the plug reaches the read-only open alone: a reader
//! follows the folded store and tails the log through the provider's
//! [`WalReader`]. A read-write open with a provider is refused, because the
//! committer that writes slots instead of the store's own log is not yet
//! here.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use object_store::ObjectStore;
use slatedb::wal::{WalReader, WriterInit};

use crate::error::{Error, Result};

/// What a commit-log library hands the catalog: SlateDB write-ahead-log
/// plugs for the store at a path on the store that holds its log.
///
/// Both methods receive the object store the catalog records as its log
/// store — the catalog store itself unless a separate one was configured —
/// already wrapped in the catalog's retrying transport, and the catalog's
/// path within it.
pub trait CommitLogProvider: fmt::Debug + Send + Sync + 'static {
    /// The writer-side plug one read-write open builds its store with.
    fn writer_init(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Box<dyn WriterInit>;

    /// The reader-side plug a read-only open builds its store with.
    fn reader(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalReader>;
}

static REGISTERED: OnceLock<Arc<dyn CommitLogProvider>> = OnceLock::new();

/// Registers the process's commit-log provider, for opens that carry no
/// provider of their own. Refuses a second registration: one process, one
/// library.
///
/// # Errors
///
/// [`Error::Configuration`] when a provider is already registered.
pub fn register_commit_log(provider: Arc<dyn CommitLogProvider>) -> Result<()> {
    REGISTERED.set(provider).map_err(|_| {
        Error::Configuration(
            "a commit-log provider is already registered for this process".to_string(),
        )
    })
}

/// The provider registered for this process, if any.
pub(crate) fn registered_commit_log() -> Option<Arc<dyn CommitLogProvider>> {
    REGISTERED.get().map(Arc::clone)
}

#[cfg(test)]
mod tests;
