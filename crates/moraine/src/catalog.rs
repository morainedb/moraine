//! The DuckLake domain model: snapshots, schemas, tables, data-file metadata.
//!
//! This layer never performs store I/O itself; the commit protocol in
//! [`crate::transaction`] drives it.

mod census;
mod handle;
pub(crate) mod index_policy;
pub(crate) mod inline;
pub(crate) mod inline_policy;
pub(crate) mod projection;
mod schema_projection;
mod snapshot;
#[cfg(test)]
mod tests;
mod types;

pub use census::{
    CensusRequest, CompactStoreReport, CompactStoreRequest, CompactionTarget, LiveCount,
    MergeOutcome, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
};
#[cfg(test)]
pub(crate) use handle::BACKFILL_FILE_READ_CONCURRENCY;
pub(crate) use handle::Store;
pub use handle::{
    CachePreload, Catalog, CatalogOptions, CommitMember, DeleteFileRegistration,
    ExistingDeleteFile, LocatedDeletion, LocatedPositions, LocatedRows, MaintenanceReport,
    MaintenanceRequest, MaintenanceStatusPass, MaintenanceStatusStep, MigrationRequest,
    ReadOnlyCatalog, RowSummaryWarmth,
};
pub use snapshot::CatalogSnapshot;
pub(crate) use snapshot::ScopedNames;

/// Resolves a data-file or delete-file path recorded relative to its table
/// (or, when `path_is_relative` is false, already store-absolute) to the
/// store key it lives under: `table_prefix` first, then `data_prefix` ahead
/// of that when the store keeps data under a prefix of its own.
pub(crate) fn resolve_data_path(
    data_prefix: &str,
    table_prefix: &str,
    path: &str,
    path_is_relative: bool,
) -> crate::error::Result<object_store::path::Path> {
    let data_prefix = data_prefix.trim_end_matches('/');
    let resolved = match (path_is_relative, data_prefix.is_empty()) {
        (false, _) => path.to_owned(),
        (true, true) => format!("{table_prefix}{path}"),
        (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
    };
    object_store::path::Path::parse(resolved)
        .map_err(|error| crate::error::Error::Corruption(format!("invalid data path: {error}")))
}
pub use types::{
    BuildStep, ColumnAlteration, ColumnDef, ColumnId, ColumnInfo, ColumnOrder, ColumnStats,
    DataFile, DataFileId, DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo, FileColumnStats,
    FileIndexEntry, FileIndexRemoval, FileRowCandidate, FlushedDataFile, IndexDef, IndexEntry,
    IndexId, IndexInfo, IndexMaintenance, IndexState, InlineChunk, MacroId, MacroImplementationDef,
    MacroInfo, MacroParameterDef, MappingId, MappingInfo, NameMappingDef, OptionScope,
    PartitionColumnDef, PartitionId, PartitionSpec, RecentRow, ScheduledDeletion, SchemaId,
    SchemaInfo, SnapshotId, SnapshotInfo, SortId, SortKeyDef, SortSpec, TableId, TableInfo,
    TableStats, TagEntry, TagTarget, Timestamp, ViewId, ViewInfo,
};
