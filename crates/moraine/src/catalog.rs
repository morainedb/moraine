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
pub(crate) mod scoped_read;
mod snapshot;
mod types;

pub use census::{
    CensusRequest, CompactStoreReport, CompactStoreRequest, CompactionTarget, LiveCount,
    MergeOutcome, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
};
pub use handle::{
    CachePreload, Catalog, CatalogOptions, CommitMember, MaintenanceReport, MaintenanceRequest,
    MaintenanceStatusPass, MaintenanceStatusStep, MigrationRequest, ReadOnlyCatalog,
};
pub use snapshot::CatalogSnapshot;
pub(crate) use snapshot::ScopedNames;
pub use types::{
    BuildStep, ColumnAlteration, ColumnDef, ColumnId, ColumnInfo, ColumnOrder, ColumnStats,
    DataFile, DataFileId, DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo, FileColumnStats,
    FileIndexEntry, FileIndexRemoval, FlushedDataFile, IndexDef, IndexEntry, IndexId, IndexInfo,
    IndexMaintenance, IndexState, InlineChunk, MacroId, MacroImplementationDef, MacroInfo,
    MacroParameterDef, MappingId, MappingInfo, NameMappingDef, OptionScope, PartitionColumnDef,
    PartitionId, PartitionSpec, RecentRow, RowHolder, RowLocation, ScheduledDeletion, SchemaId,
    SchemaInfo, SnapshotId, SnapshotInfo, SortId, SortKeyDef, SortSpec, TableId, TableInfo,
    TableStats, TagEntry, TagTarget, ViewId, ViewInfo,
};
