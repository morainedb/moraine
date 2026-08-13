//! What a store weighs, and the request to merge it down.
//!
//! A store's keyspace is divided into subspaces, and each is its own tree
//! beneath the catalog — so a census is per-subspace, and so is a merge.

use std::{fmt, time::Duration};

use crate::store::key::{Subspace, subspace_prefix};

/// One subspace of the keyspace, as a census names it.
///
/// The names are moraine's own. A store carrying a segment this build does
/// not recognize — written by a newer format, or damaged — is reported as
/// [`Unknown`](Self::Unknown) rather than dropped: a census exists to
/// describe a store that has gone wrong.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum SubspaceName {
    /// Store-level singletons: format version, head pointer, migration
    /// marker.
    System,
    /// One record per snapshot.
    Snapshot,
    /// Live catalog state — what every attach scans.
    Current,
    /// Ended entity versions.
    History,
    /// Inlined data and its archive forms.
    Inline,
    /// Equality-index entries.
    Index,
    /// Per-table schema-version records.
    SchemaVersion,
    /// Per-commit changelogs a reader replays to refresh.
    Changelog,
    /// A segment whose prefix names no subspace this build knows. The raw
    /// prefix is carried verbatim; empty means the unsegmented root tree,
    /// which a store moraine wrote never has.
    Unknown(Vec<u8>),
}

impl SubspaceName {
    /// The subspace whose keys carry `prefix`.
    pub(crate) fn of_prefix(prefix: &[u8]) -> Self {
        Subspace::ALL
            .into_iter()
            .find(|subspace| subspace_prefix(*subspace) == prefix)
            .map_or_else(|| Self::Unknown(prefix.to_vec()), Self::from)
    }

    /// The subspace this name refers to, or `None` for an unknown one,
    /// which has no keys to address.
    pub(crate) fn subspace(&self) -> Option<Subspace> {
        match self {
            Self::System => Some(Subspace::System),
            Self::Snapshot => Some(Subspace::Snapshot),
            Self::Current => Some(Subspace::Current),
            Self::History => Some(Subspace::History),
            Self::Inline => Some(Subspace::Inline),
            Self::Index => Some(Subspace::Index),
            Self::SchemaVersion => Some(Subspace::SchemaVersion),
            Self::Changelog => Some(Subspace::Changelog),
            Self::Unknown(_) => None,
        }
    }
}

impl From<Subspace> for SubspaceName {
    fn from(subspace: Subspace) -> Self {
        match subspace {
            Subspace::System => Self::System,
            Subspace::Snapshot => Self::Snapshot,
            Subspace::Current => Self::Current,
            Subspace::History => Self::History,
            Subspace::Inline => Self::Inline,
            Subspace::Index => Self::Index,
            Subspace::SchemaVersion => Self::SchemaVersion,
            Subspace::Changelog => Self::Changelog,
        }
    }
}

impl fmt::Display for SubspaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System => f.write_str("system"),
            Self::Snapshot => f.write_str("snapshot"),
            Self::Current => f.write_str("current"),
            Self::History => f.write_str("history"),
            Self::Inline => f.write_str("inline"),
            Self::Index => f.write_str("index"),
            Self::SchemaVersion => f.write_str("schema_version"),
            Self::Changelog => f.write_str("changelog"),
            Self::Unknown(prefix) => {
                f.write_str("unknown(")?;
                for byte in prefix {
                    write!(f, "{byte:02x}")?;
                }
                f.write_str(")")
            }
        }
    }
}

/// How a [`ReadOnlyCatalog::store_census`](crate::ReadOnlyCatalog::store_census) call
/// should run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CensusRequest {
    /// Also scan every subspace and count its live keys.
    ///
    /// Off by default, because it costs a full read of the store where the
    /// physical measurement costs two object reads. It is the only way to
    /// learn what fraction of a subspace is live — that is, how much a
    /// merge would reclaim rather than merely rewrite.
    pub count_live_entries: bool,
}

/// Live keys of one subspace, and the bytes they occupy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LiveCount {
    /// Keys a reader would see.
    pub keys: u64,
    /// Encoded bytes of those keys.
    pub key_bytes: u64,
    /// Encoded bytes of their values.
    pub value_bytes: u64,
    /// Deletion-schedule entries among `keys`. Only `current` carries any;
    /// they are live rows a merge cannot reclaim and only DuckLake's
    /// `cleanup_old_files` drains.
    pub scheduled_files: u64,
}

/// What one subspace weighs.
///
/// The physical figures come from the store's manifest and count what has
/// been written out — a write still in the write-ahead log is in none of
/// them. They include superseded versions and tombstones, which is the
/// point: the gap between `bytes` and `live` is what a merge reclaims.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubspaceCensus {
    /// The subspace measured.
    pub subspace: SubspaceName,
    /// Physical bytes across its SSTs.
    pub bytes: u64,
    /// Bloom-filter bytes recorded across the subspace's SSTs.
    pub filter_bytes: u64,
    /// SST index-block bytes recorded across the subspace's SSTs.
    pub index_bytes: u64,
    /// SST statistics-block bytes recorded across the subspace's SSTs.
    pub stats_bytes: u64,
    /// SSTs not yet merged into a sorted run.
    pub l0_ssts: u32,
    /// Sorted runs. A merge collapses these to one.
    pub sorted_runs: u32,
    /// SSTs across those runs.
    pub sorted_run_ssts: u32,
    /// Live keys, when the request asked for them.
    pub live: Option<LiveCount>,
}

/// What the object store holds, by the kind of object holding it.
///
/// The manifest accounts for SST bytes only, so a store whose weight is in
/// its write-ahead log reads as nearly empty subspace by subspace while
/// costing every reader dearly — a read attach replays the log before it
/// materializes anything, and no merge touches those bytes. Comparing
/// `sst_bytes` against `total_bytes` is what tells the two apart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreObjects {
    /// Every object under the store's prefix.
    pub total_objects: u64,
    /// Bytes across all of them.
    pub total_bytes: u64,
    /// Write-ahead log objects, replayed by a read attach that is not
    /// pinned to a checkpoint.
    pub wal_objects: u64,
    /// Bytes across those.
    pub wal_bytes: u64,
    /// Manifest versions.
    pub manifest_objects: u64,
    /// Bytes across those.
    pub manifest_bytes: u64,
    /// Sorted-string tables — the only bytes a merge reclaims, and the only
    /// ones the per-subspace figures account for.
    pub sst_objects: u64,
    /// Bytes across those.
    pub sst_bytes: u64,
    /// Everything else the layout carries. The four counts sum to
    /// `total_objects`, so nothing measured goes unattributed.
    pub other_objects: u64,
    /// Bytes across those.
    pub other_bytes: u64,
}

/// What a store weighs, subspace by subspace.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreCensus {
    /// The manifest version measured.
    pub manifest_id: u64,
    /// One entry per subspace the store has written out, in prefix order.
    pub subspaces: Vec<SubspaceCensus>,
    /// What the object store holds, by kind — `None` when the store could
    /// not be listed, which read-only credentials granting `GetObject`
    /// without `ListBucket` produce.
    pub objects: Option<StoreObjects>,
}

impl StoreCensus {
    /// Bytes the object store holds that no subspace accounts for — the
    /// write-ahead log, the manifest, and anything else. `None` when the
    /// store could not be listed.
    ///
    /// A large figure here is the signature of a slow read attach that a
    /// merge will not fix: a reader replays the log at open.
    #[must_use]
    pub fn unaccounted_bytes(&self) -> Option<u64> {
        self.objects
            .map(|objects| objects.total_bytes.saturating_sub(objects.sst_bytes))
    }

    /// Physical bytes across every subspace.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.subspaces
            .iter()
            .fold(0, |total, subspace| total.saturating_add(subspace.bytes))
    }
}

/// Which trees a
/// [`Catalog::compact_store`](crate::Catalog::compact_store) call merges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompactionTarget {
    /// Every subspace holding sorted runs; the rest are skipped.
    #[default]
    WholeStore,
    /// One subspace.
    Subspace(SubspaceName),
}

/// How a [`Catalog::compact_store`](crate::Catalog::compact_store) call
/// should run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompactStoreRequest {
    /// Which trees to merge.
    pub target: CompactionTarget,
    /// How long to wait for every submitted merge to commit. `None` returns
    /// as soon as they are submitted. A merge that outlives the wait is not
    /// cancelled: it keeps running, and a later census shows the result.
    pub wait: Option<Duration>,
    /// Return an error unless every targeted merge completed or had no
    /// sorted runs to merge. Requires [`wait`](Self::wait).
    pub require_completed: bool,
}

/// How one subspace's merge ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MergeOutcome {
    /// The merge committed.
    Completed,
    /// The merge ended without committing.
    Failed(String),
    /// Submitted and still running when the wait ran out, or not waited for
    /// at all.
    Pending,
    /// Nothing was submitted, for the reason given.
    Skipped(&'static str),
}

/// What one subspace's merge did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubspaceMerge {
    /// The subspace merged.
    pub subspace: SubspaceName,
    /// How it ended.
    pub outcome: MergeOutcome,
    /// Physical bytes before the merge was submitted.
    pub bytes_before: u64,
    /// Physical bytes after it committed; `None` unless it did.
    pub bytes_after: Option<u64>,
}

/// What a store merge did, subspace by subspace.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompactStoreReport {
    /// One entry per subspace the request covered.
    pub merges: Vec<SubspaceMerge>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every subspace round-trips through its prefix, and an unrecognized
    /// prefix is named rather than lost.
    #[test]
    fn subspace_names_round_trip_through_prefixes() {
        for subspace in Subspace::ALL {
            let name = SubspaceName::from(subspace);
            assert_eq!(SubspaceName::of_prefix(&subspace_prefix(subspace)), name);
            assert_eq!(name.subspace(), Some(subspace));
        }

        let unknown = SubspaceName::of_prefix(&[0xfe]);
        assert_eq!(unknown, SubspaceName::Unknown(vec![0xfe]));
        assert_eq!(unknown.subspace(), None);
        assert_eq!(unknown.to_string(), "unknown(fe)");

        // The root tree of a store written without a segment extractor.
        assert_eq!(SubspaceName::of_prefix(&[]), SubspaceName::Unknown(vec![]));
    }

    #[test]
    fn subspaces_display_as_their_written_names() {
        assert_eq!(SubspaceName::Current.to_string(), "current");
        assert_eq!(SubspaceName::SchemaVersion.to_string(), "schema_version");
    }

    /// The bytes no subspace accounts for — the write-ahead log and the
    /// manifest — are what a merge cannot touch, so a census that omitted
    /// them would point an operator at the wrong lever.
    #[test]
    fn unaccounted_bytes_is_everything_outside_the_ssts() {
        let mut census = StoreCensus {
            manifest_id: 1,
            objects: None,
            subspaces: Vec::new(),
        };
        assert_eq!(census.unaccounted_bytes(), None, "unlisted stays unknown");

        census.objects = Some(StoreObjects {
            total_bytes: 1_000,
            sst_bytes: 250,
            wal_bytes: 700,
            manifest_bytes: 50,
            ..StoreObjects::default()
        });
        assert_eq!(census.unaccounted_bytes(), Some(750));
    }

    #[test]
    fn total_bytes_sums_every_subspace() {
        let census = StoreCensus {
            manifest_id: 7,
            objects: None,
            subspaces: vec![
                SubspaceCensus {
                    subspace: SubspaceName::Current,
                    bytes: 10,
                    filter_bytes: 1,
                    index_bytes: 2,
                    stats_bytes: 3,
                    l0_ssts: 1,
                    sorted_runs: 0,
                    sorted_run_ssts: 0,
                    live: None,
                },
                SubspaceCensus {
                    subspace: SubspaceName::Index,
                    bytes: 32,
                    filter_bytes: 4,
                    index_bytes: 5,
                    stats_bytes: 6,
                    l0_ssts: 0,
                    sorted_runs: 1,
                    sorted_run_ssts: 2,
                    live: None,
                },
            ],
        };

        assert_eq!(census.total_bytes(), 42);
    }
}
