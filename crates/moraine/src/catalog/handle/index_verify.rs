//! Recomputes an index's entries from the rows and diffs them against what
//! the index holds.

use std::collections::HashMap;

use tracing::info;

use super::{
    Catalog, ReadOnlyCatalog, backfill,
    index_build::{EntrySink, IndexEncoder},
};
use crate::{
    catalog::{IndexState, TableId},
    data_file::DataStore,
    error::{Error, Result},
    store::{
        handle::ScanShape,
        key::{IndexKey, IndexKind, Key, index_index_prefix},
        proto::InlineBuildCursorValue,
    },
    transaction::EncodedIndexEntry,
};

/// How many offending row ids a report names, per kind.
const SAMPLE: usize = 32;

/// What one verifier pass found. A clean index has `missing`, `dead` and
/// `mismatched` all zero and `stored == expected`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexVerification {
    /// Entries the index holds.
    pub stored: u64,
    /// Entries the live rows require.
    pub expected: u64,
    /// Entries the rows require that the index does not hold.
    pub missing: u64,
    /// Entries the index holds that no live row requires.
    pub dead: u64,
    /// Unique entries present under the right value but naming another row.
    pub mismatched: u64,
    /// A bounded sample of row ids whose entry is missing or mismatched.
    pub sample_missing: Vec<u64>,
    /// A bounded sample of row ids named by an entry no live row requires.
    pub sample_dead: Vec<u64>,
    /// The snapshot the pass read, which a certificate names.
    pub verified_at: Option<crate::SnapshotId>,
    /// The index definition's version when the pass read it. A certificate
    /// is refused if the definition moved after this.
    pub definition: Option<crate::SnapshotId>,
}

impl IndexVerification {
    /// Whether the index agrees with the rows.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.missing == 0 && self.dead == 0 && self.mismatched == 0
    }
}

/// One stored entry: the row it names and whether a derived entry claimed it.
struct StoredEntry {
    row_id: u64,
    claimed: bool,
}

/// Diffs derived entries against a snapshot of the stored ones.
struct VerifySink {
    encoder: IndexEncoder,
    stored: HashMap<Vec<u8>, StoredEntry>,
    report: IndexVerification,
}

impl VerifySink {
    fn observe(&mut self, entry: &EncodedIndexEntry) {
        self.report.expected = self.report.expected.saturating_add(1);
        match self.stored.get_mut(&entry.key[..]) {
            None => {
                self.report.missing = self.report.missing.saturating_add(1);
                if self.report.sample_missing.len() < SAMPLE {
                    self.report.sample_missing.push(entry.row_id);
                }
            }
            Some(stored) => {
                // A unique key stores its row id as the value, so an entry
                // under the right value can still name the wrong row.
                if stored.row_id != entry.row_id {
                    self.report.mismatched = self.report.mismatched.saturating_add(1);
                    if self.report.sample_missing.len() < SAMPLE {
                        self.report.sample_missing.push(entry.row_id);
                    }
                }
                stored.claimed = true;
            }
        }
    }

    /// Counts every stored entry no derived entry claimed.
    fn sweep_unclaimed(&mut self) {
        for entry in self.stored.values() {
            if entry.claimed {
                continue;
            }
            self.report.dead = self.report.dead.saturating_add(1);
            if self.report.sample_dead.len() < SAMPLE {
                self.report.sample_dead.push(entry.row_id);
            }
        }
    }
}

/// Every entry the index holds, keyed by its encoded bytes.
///
/// Both keyspaces are scanned whatever the index's uniqueness, because a
/// NULL is collision-exempt: an entry carrying one takes the multi shape
/// even on a unique index, so a unique index holds entries of both kinds.
/// Only a unique-shaped entry names its row in the value; a multi-shaped
/// one carries the row id in the key.
async fn stored_entries(
    handle: crate::store::handle::ReadHandle<'_>,
    info: &crate::IndexInfo,
) -> Result<HashMap<Vec<u8>, StoredEntry>> {
    let mut stored = HashMap::new();
    for kind in [IndexKind::Unique, IndexKind::Multi] {
        let mut entries = handle
            .scan_prefix(index_index_prefix(kind, info.id.get()), .., ScanShape::Bulk)
            .await?;
        while let Some(entry) = entries.next().await? {
            let row_id = match Key::decode(&entry.key)? {
                Key::Index(IndexKey::Unique { .. }) => <[u8; 8]>::try_from(entry.value.as_ref())
                    .map(u64::from_be_bytes)
                    .map_err(|_| {
                        Error::Corruption(format!(
                            "unique index {} holds a {}-byte row id",
                            info.name,
                            entry.value.len()
                        ))
                    })?,
                Key::Index(IndexKey::Multi { row_id, .. }) => row_id,
                _ => return Err(Error::Corruption("unexpected index entry key".into())),
            };
            stored.insert(
                entry.key.to_vec(),
                StoredEntry {
                    row_id,
                    claimed: false,
                },
            );
        }
    }
    Ok(stored)
}

impl EntrySink for VerifySink {
    fn encoder(&self) -> &IndexEncoder {
        &self.encoder
    }

    async fn push(&mut self, entry: EncodedIndexEntry, _source: Option<(u64, u64)>) -> Result<()> {
        self.observe(&entry);
        Ok(())
    }

    fn cover_source(&mut self, _file_id: u64, _position: u64) {}

    fn inline_cursor(&self) -> Option<InlineBuildCursorValue> {
        None
    }

    fn set_inline_cursor(&mut self, _cursor: InlineBuildCursorValue) {}

    fn record_inline_peaks(&mut self, _body_bytes: usize, _decoded_bytes: usize) {}
}

impl ReadOnlyCatalog {
    /// Recomputes `index`'s entries from the table's live rows and reports
    /// how they differ from the entries the index holds.
    ///
    /// The pass reads every live data file of the table, so it costs what
    /// building the index costs, and it holds the stored entry set in
    /// memory — the same bound a backfill accepts. It writes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the table has no index of that
    /// name, and [`Error::Constraint`] when the index is still building or
    /// when the table's registered files need a data store this call was
    /// not given.
    pub async fn verify_index(
        &self,
        table: TableId,
        index: &str,
        data_store: Option<DataStore>,
        data_prefix: &str,
    ) -> Result<IndexVerification> {
        let started = std::time::Instant::now();
        let epoch = super::cache_epoch(&self.projections);
        let session = self.begin_read().await?;
        let snapshot = self.head_view(session.handle(), epoch).await?;
        super::index_build::require_data_store(&snapshot, table, data_store.is_some())?;

        let info = snapshot
            .indexes_of(table)
            .into_iter()
            .find(|info| info.name == index)
            .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;
        if info.state != IndexState::Ready {
            return Err(Error::Constraint(format!(
                "index {index} is {:?}; only a ready index can be verified",
                info.state
            )));
        }

        let read_at = snapshot.current_snapshot().id;
        let definition = snapshot
            .indexes
            .get(&table.get())
            .and_then(|indexes| indexes.get(&info.id.get()))
            .map(|value| crate::SnapshotId::new(value.begin_snapshot));
        let stored = stored_entries(session.handle(), &info).await?;
        let mut sink = VerifySink {
            encoder: IndexEncoder::of(&info),
            report: IndexVerification {
                stored: u64::try_from(stored.len()).unwrap_or(u64::MAX),
                verified_at: Some(read_at),
                definition,
                ..IndexVerification::default()
            },
            stored,
        };

        let source = backfill::BackfillSource {
            snapshot: &snapshot,
            table,
            handle: session.handle(),
        };
        super::index_build::inline_sources::stream_inline_sources(
            source,
            &info.columns,
            None,
            &mut sink,
        )
        .await?;
        if let Some(store) = data_store {
            self.stream_backfill_files(
                store,
                data_prefix,
                source,
                &info.columns,
                None,
                None,
                None,
                &mut sink,
            )
            .await?;
        }
        session.finish();

        sink.sweep_unclaimed();

        info!(
            table = table.get(),
            index = info.id.get(),
            index_name = %index,
            stored = sink.report.stored,
            expected = sink.report.expected,
            missing = sink.report.missing,
            dead = sink.report.dead,
            mismatched = sink.report.mismatched,
            sample_missing = ?sink.report.sample_missing,
            sample_dead = ?sink.report.sample_dead,
            verify_ms = crate::telemetry::milliseconds(started.elapsed()),
            "index verified"
        );
        Ok(sink.report)
    }
}

impl Catalog {
    /// Verifies `index` and records the outcome on its definition: a clean
    /// pass certifies the snapshot it agreed at, and a pass that found any
    /// discrepancy withdraws an earlier certificate rather than leaving a
    /// stale one standing.
    ///
    /// The certificate survives ordinary maintained writes, which is the
    /// trust it extends, and is withdrawn by a rebuild or a deferred
    /// repair. Nothing is written when the outcome already matches what
    /// the definition records.
    ///
    /// # Errors
    ///
    /// As [`ReadOnlyCatalog::verify_index`], plus a commit error.
    pub async fn certify_index(
        &self,
        table: TableId,
        index: &str,
        data_store: Option<DataStore>,
        data_prefix: &str,
    ) -> Result<IndexVerification> {
        let report = self
            .verify_index(table, index, data_store, data_prefix)
            .await?;
        let snapshot = self.snapshot().await?;
        let info = snapshot
            .index_by_name(table, index)
            .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;
        let at = report.is_clean().then_some(report.verified_at).flatten();
        if info.verified_at_snapshot == at {
            return Ok(report);
        }
        let Some(observed) = report.definition else {
            return Ok(report);
        };
        let index_id = info.id;
        self.commit(move |tx| tx.record_index_verification(index_id, observed, at))
            .await?;
        Ok(report)
    }
}

#[cfg(test)]
mod tests;
