//! Key layout.
//!
//! A key is a typed tree — subspace, kind, components — encoded by
//! `storekey` in order-preserving form: one discriminant byte per enum
//! level (assigned by declaration order), then fixed-width big-endian
//! `u64` components in field order. Variant order is permanent once
//! written; the golden-vector tests pin the exact bytes.

use storekey::{Decode, Encode};

use crate::{
    error::{Error, Result},
    store::index_encoding::{CanonicalKey, IndexKeyValue, nominal_key_bytes},
};

/// Length in bytes of the encoded subspace prefix — one discriminant
/// byte. The segment extractor and the prefix builders must agree on
/// this.
pub(crate) const TAG_PREFIX_LEN: usize = 1;

/// A fully addressed store key. Each variant is a subspace, and each
/// subspace is also a SlateDB segment (the store is created with a
/// one-byte segment extractor over the leading discriminant).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum Key {
    /// Store-level singletons: format version, head pointer, migration
    /// marker, and maintenance status. Overwritten in place.
    Sys(SysKey),
    /// One record per snapshot — `ducklake_snapshot` +
    /// `ducklake_snapshot_changes`, merged. Append-only.
    Snapshot {
        /// Snapshot id.
        snapshot_id: u64,
    },
    /// Live catalog state.
    Current(CurrentKey),
    /// Ended entity versions; append-only.
    History(HistoryKey),
    /// Inlined data and its archive forms.
    Inline(InlineKey),
    /// Equality-index entries. Live-only; keyed by index and canonical
    /// value.
    Index(IndexKey),
    /// `ducklake_schema_versions`: one record per snapshot at which a
    /// table's shape changed, holding the catalog schema version that
    /// snapshot minted. Retained across snapshot expiry — a data file
    /// resolves its schema version through these long after the snapshot
    /// that wrote it is gone — and removed only with the table.
    SchemaVersion {
        /// The created-or-altered table (or view).
        table_id: u64,
        /// The snapshot the change landed in.
        begin_snapshot: u64,
    },
    /// The `current` keys one commit's batch wrote — the changelog a reader
    /// replays to advance a held view across that commit. Its own subspace
    /// rather than a field of the snapshot record: snapshot records are
    /// scanned in full on a read path DuckLake takes every transaction, and
    /// the changelog is several times their size. Only a refresh reads
    /// these, one point read per snapshot in the gap. A sliding window of
    /// the most recent commits' records is retained; older ones are deleted
    /// by later commits, so nothing else has to reclaim them.
    Changelog {
        /// The snapshot whose batch wrote these keys.
        snapshot_id: u64,
    },
}

/// An equality-index entry key. The unique kind keys on the value alone —
/// the store key *is* the uniqueness claim, so two commits inserting the
/// same value collide in the store's write-write detection. The non-unique
/// kind appends the row id, so rows sharing a value occupy distinct keys
/// and concurrent appends stay benign.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum IndexKey {
    /// A unique-index entry; value is the holding row id.
    Unique {
        /// The index this entry belongs to.
        index_id: u64,
        /// The canonical indexed value.
        key: CanonicalKey,
    },
    /// A non-unique-index entry; value is empty.
    Multi {
        /// The index this entry belongs to.
        index_id: u64,
        /// The canonical indexed value.
        key: CanonicalKey,
        /// The holding row id, disambiguating rows that share a value.
        row_id: u64,
    },
}

/// Store-level singleton keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum SysKey {
    /// Layout format version + moraine version that wrote the store.
    Format,
    /// Latest committed snapshot id.
    Head,
    /// Structural-migration marker. Reserved from format v1.
    Migration,
    /// Bounded history of completed maintenance passes.
    MaintenanceStatus,
}

/// A live record: a temporally versioned entity, or the `current`-only
/// gc-file bookkeeping (no begin/end lifecycle, so no `history` mirror).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum CurrentKey {
    /// A live entity version.
    Entity(EntityKey),
    /// `ducklake_files_scheduled_for_deletion`. Keyed by the scheduled
    /// file's id — the row's identity in DuckLake's own schema (inserts
    /// carry it, cleanup deletes by it), unique because a file's catalog
    /// rows are removed in the same transaction that schedules it.
    GcFile {
        /// The scheduled data or delete file's id.
        data_file_id: u64,
    },
}

/// An ended entity version; `end_snapshot` is the final key component,
/// so a single entity's versions sort by when they ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) struct HistoryKey {
    /// The ended entity.
    pub(crate) entity: EntityKey,
    /// Snapshot at which the version ended.
    pub(crate) end_snapshot: u64,
}

/// A temporally versioned catalog entity. Lives in `current` while live; its
/// `history` mirror appends `end_snapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum EntityKey {
    /// `ducklake_schema`.
    Schema {
        /// Global catalog id.
        schema_id: u64,
    },
    /// `ducklake_table`.
    Table {
        /// Global catalog id.
        table_id: u64,
    },
    /// `ducklake_view`.
    View {
        /// Global catalog id.
        view_id: u64,
    },
    /// `ducklake_column` — one record per row, nested fields included.
    Column {
        /// Owning table.
        table_id: u64,
        /// Per-table field id (never reused across the table's history).
        column_id: u64,
    },
    /// `ducklake_partition_info` (+ partition columns embedded).
    Partition {
        /// Owning table.
        table_id: u64,
        /// Partition spec id.
        partition_id: u64,
    },
    /// `ducklake_data_file`.
    File {
        /// Owning table.
        table_id: u64,
        /// Global file id.
        data_file_id: u64,
    },
    /// `ducklake_delete_file`.
    DeleteFile {
        /// Owning table.
        table_id: u64,
        /// Global file id.
        delete_file_id: u64,
    },
    /// `ducklake_file_column_stats`, keyed file-major.
    FileColumnStats {
        /// Owning table.
        table_id: u64,
        /// Data file the stats describe.
        data_file_id: u64,
        /// Column the stats describe.
        column_id: u64,
    },
    /// `ducklake_table_stats`.
    TableStats {
        /// Owning table.
        table_id: u64,
    },
    /// `ducklake_table_column_stats`.
    TableColumnStats {
        /// Owning table.
        table_id: u64,
        /// Column the stats describe.
        column_id: u64,
    },
    /// `ducklake_tag` — object ids are unique across entity types.
    Tag {
        /// The tagged object's id.
        object_id: u64,
    },
    /// `ducklake_metadata` / `set_option` scopes — one record per scope.
    Option {
        /// Scope kind: global = 0, schema = 1, table = 2.
        scope_kind: u64,
        /// Scope id (0 for global).
        scope_id: u64,
    },
    /// `ducklake_sort_info` (+ sort expressions embedded).
    Sort {
        /// Owning table.
        table_id: u64,
        /// Sort spec id.
        sort_id: u64,
    },
    /// `ducklake_macro` (+ impls and parameters embedded).
    Macro {
        /// Global catalog id.
        macro_id: u64,
    },
    /// `ducklake_column_mapping` (+ name-mapping rows embedded).
    /// Unversioned and immutable: written once, never overwritten, never
    /// mirrored to history.
    Mapping {
        /// Owning table.
        table_id: u64,
        /// Mapping id, allocated by DuckLake from the file-id counter.
        mapping_id: u64,
    },
    /// A moraine-native equality-index definition. No DuckLake analog.
    Index {
        /// Owning table.
        table_id: u64,
        /// Index id, allocated from the global catalog-id counter.
        index_id: u64,
    },
}

impl EntityKey {
    /// Whether records of this kind carry a begin/end lifecycle and mirror
    /// to history when ended. Unversioned kinds (statistics, options, tags,
    /// mappings) are overwritten in place — or written once — and never
    /// appear in the history subspace.
    pub(crate) const fn is_versioned(self) -> bool {
        !matches!(
            self,
            Self::FileColumnStats { .. }
                | Self::TableStats { .. }
                | Self::TableColumnStats { .. }
                | Self::Option { .. }
                | Self::Tag { .. }
                | Self::Mapping { .. }
        )
    }
}

/// An inlined-data key: the per-schema-version Arrow schema, a live
/// record, the archived (post-flush) form of a live record, or a
/// delete-table existence marker, or a live chunk's row-range locator.
/// `Live` and `Arch` share [`InlineOperation`], so an archive key has exactly
/// the components of its live form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum InlineKey {
    /// Arrow IPC schema message, once per `(table, schema_version)`. Has
    /// no archive form (schemas are never flushed away).
    Schema {
        /// Owning table.
        table_id: u64,
        /// Schema version the layout is pinned to.
        schema_version: u64,
    },
    /// A live inlined-data record.
    Live(InlineOperation),
    /// The archived form of an inlined-data record.
    Arch(InlineOperation),
    /// Marks that a table's `ducklake_inlined_delete_<table_id>` exists,
    /// once per table. Appended last so the variants above keep their
    /// discriminants.
    ///
    /// Existence has to be recorded rather than derived from whether any
    /// `FileDelete` record is currently live: a flush materializes those
    /// into a real delete file and clears them, and an emptied SQL table
    /// still exists — which is what DuckLake, having cached the table's
    /// existence for the life of the catalog, goes on assuming.
    FileDeleteTable {
        /// Owning table.
        table_id: u64,
    },
    /// One live inline chunk's inclusive row-id end. Appended last so every
    /// previously shipped variant keeps its discriminant.
    ChunkRange {
        /// Owning table.
        table_id: u64,
        /// Inclusive end of the chunk's contiguous row-id range.
        row_id_end: u64,
    },
}

/// An inlined-data record that exists in both live and archived form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum InlineOperation {
    /// One inlined-insert chunk (Arrow record-batch body).
    Insert {
        /// Owning table.
        table_id: u64,
        /// Schema version the chunk was written under.
        schema_version: u64,
        /// Commit snapshot of the insert.
        begin_snapshot: u64,
        /// Disambiguates multiple chunks in one commit.
        chunk_seq: u64,
    },
    /// Tombstone for an inlined insert row.
    InlineDelete {
        /// Owning table.
        table_id: u64,
        /// Deleted row.
        row_id: u64,
    },
    /// Inlined delete against a Parquet-file row.
    FileDelete {
        /// Owning table.
        table_id: u64,
        /// Targeted data file.
        data_file_id: u64,
        /// Deleted row.
        row_id: u64,
    },
}

impl Key {
    /// A live entity key.
    pub(crate) const fn current(entity: EntityKey) -> Self {
        Self::Current(CurrentKey::Entity(entity))
    }

    /// An ended entity-version key.
    pub(crate) const fn history(entity: EntityKey, end_snapshot: u64) -> Self {
        Self::History(HistoryKey {
            entity,
            end_snapshot,
        })
    }

    /// Encode to the on-disk byte form.
    pub(crate) fn encode(&self) -> Vec<u8> {
        // Infallible by construction: a `Vec` sink raises no io error and
        // the derived `Encode` raises no custom error.
        #[allow(clippy::expect_used)]
        storekey::encode_vec(self).expect("storekey encode into a Vec cannot fail")
    }

    /// Decode from the on-disk byte form. Fails as `Corruption` on an
    /// unknown discriminant, wrong arity, or trailing bytes
    /// (`storekey::decode` verifies the reader is exhausted).
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        storekey::decode(bytes).map_err(|err| Error::Corruption(format!("key: {err}")))
    }
}

/// Encodes one equality-index entry from a borrowed canonical value. This is
/// byte-identical to [`Key::Index`] but avoids constructing an owned key and
/// cloning its potentially large canonical payload before serialization.
pub(crate) fn encode_index_entry(
    index_id: u64,
    unique: bool,
    key: &CanonicalKey,
    row_id: u64,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = storekey::Writer::new(&mut bytes);
    // Infallible by construction: a `Vec` sink raises no I/O error and these
    // primitive/canonical encoders raise no custom error. The discriminants
    // are the pinned `Key::Index` and `IndexKey::{Unique,Multi}` variants.
    let result = (|| -> std::result::Result<(), storekey::EncodeError> {
        writer.write_u8(7)?;
        writer.write_u8(if unique { 2 } else { 3 })?;
        writer.write_u64(index_id)?;
        <CanonicalKey as Encode<()>>::encode(key, &mut writer)?;
        if !unique {
            writer.write_u64(row_id)?;
        }
        Ok(())
    })();
    #[allow(clippy::expect_used)]
    result.expect("storekey encode into a Vec cannot fail");
    bytes
}

/// A subspace, fieldless — for building scan prefixes without naming
/// discriminant bytes; prefixes are derived by encoding a sample key and
/// truncating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Subspace {
    /// Store-level singletons.
    System,
    /// Snapshot records.
    Snapshot,
    /// Live catalog state.
    Current,
    /// Ended entity versions.
    History,
    /// Inlined data.
    Inline,
    /// Equality-index entries.
    Index,
    /// Per-table schema-version records.
    SchemaVersion,
    /// Per-commit changelogs.
    Changelog,
}

impl Subspace {
    /// Every subspace, in discriminant order. A census walks this, so a
    /// subspace added to [`Key`] and left out here goes unreported.
    pub(crate) const ALL: [Self; 8] = [
        Self::System,
        Self::Snapshot,
        Self::Current,
        Self::History,
        Self::Inline,
        Self::Index,
        Self::SchemaVersion,
        Self::Changelog,
    ];

    /// A minimal key inside this subspace, for prefix derivation.
    const fn sample(self) -> Key {
        match self {
            Self::System => Key::Sys(SysKey::Format),
            Self::Snapshot => Key::Snapshot { snapshot_id: 0 },
            Self::Current => Key::current(EntityKey::Schema { schema_id: 0 }),
            Self::History => Key::history(EntityKey::Schema { schema_id: 0 }, 0),
            Self::Inline => Key::Inline(InlineKey::Schema {
                table_id: 0,
                schema_version: 0,
            }),
            Self::Index => Key::Index(IndexKey::Unique {
                index_id: 0,
                key: CanonicalKey::empty(),
            }),
            Self::SchemaVersion => Key::SchemaVersion {
                table_id: 0,
                begin_snapshot: 0,
            },
            Self::Changelog => Key::Changelog { snapshot_id: 0 },
        }
    }
}

/// The kinds whose live keys are scoped `table_id`-first — the only kinds
/// [`current_table_prefix`] accepts.
// No caller needs "everything about table T" yet (cascading table drop and
// per-table GC land in later slices); the prefix math is pinned by tests.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableScopedKind {
    /// `ducklake_column`.
    Column,
    /// `ducklake_partition_info`.
    Partition,
    /// `ducklake_data_file`.
    File,
    /// `ducklake_delete_file`.
    DeleteFile,
    /// `ducklake_file_column_stats`.
    FileColumnStats,
    /// `ducklake_table_stats`.
    TableStats,
    /// `ducklake_table_column_stats`.
    TableColumnStats,
    /// `ducklake_sort_info`.
    Sort,
    /// `ducklake_column_mapping`.
    Mapping,
    /// Equality-index definition.
    Index,
}

impl TableScopedKind {
    /// An entity of this kind with its table id set and every other
    /// component zeroed, for prefix derivation.
    #[allow(dead_code)]
    const fn sample(self, table_id: u64) -> EntityKey {
        match self {
            Self::Column => EntityKey::Column {
                table_id,
                column_id: 0,
            },
            Self::Partition => EntityKey::Partition {
                table_id,
                partition_id: 0,
            },
            Self::File => EntityKey::File {
                table_id,
                data_file_id: 0,
            },
            Self::DeleteFile => EntityKey::DeleteFile {
                table_id,
                delete_file_id: 0,
            },
            Self::FileColumnStats => EntityKey::FileColumnStats {
                table_id,
                data_file_id: 0,
                column_id: 0,
            },
            Self::TableStats => EntityKey::TableStats { table_id },
            Self::TableColumnStats => EntityKey::TableColumnStats {
                table_id,
                column_id: 0,
            },
            Self::Sort => EntityKey::Sort {
                table_id,
                sort_id: 0,
            },
            Self::Mapping => EntityKey::Mapping {
                table_id,
                mapping_id: 0,
            },
            Self::Index => EntityKey::Index {
                table_id,
                index_id: 0,
            },
        }
    }
}

/// Discriminant bytes preceding an entity's components in a live key:
/// subspace, `CurrentKey::Entity`, entity kind.
// Only `current_table_prefix` consumes this, and it has no caller yet.
#[allow(dead_code)]
const CUR_KIND_PREFIX_LEN: usize = 3;

/// The three live-record kinds inside the inline subspace — the only kinds
/// [`inline_live_table_prefix`] builds a prefix for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InlineOperationKind {
    /// `inline/insert`.
    Insert,
    /// `inline/inline_delete`.
    InlineDelete,
    /// `inline/file_delete`.
    FileDelete,
}

impl InlineOperationKind {
    /// An op of this kind with its table id set and every other component
    /// zeroed, for prefix derivation.
    const fn sample(self, table_id: u64) -> InlineOperation {
        match self {
            Self::Insert => InlineOperation::Insert {
                table_id,
                schema_version: 0,
                begin_snapshot: 0,
                chunk_seq: 0,
            },
            Self::InlineDelete => InlineOperation::InlineDelete {
                table_id,
                row_id: 0,
            },
            Self::FileDelete => InlineOperation::FileDelete {
                table_id,
                data_file_id: 0,
                row_id: 0,
            },
        }
    }
}

/// Discriminant bytes preceding a live inline op's components: subspace,
/// `InlineKey::Live`, op kind.
const INLINE_LIVE_KIND_PREFIX_LEN: usize = 3;

/// Encodes `key` and keeps its first `len` bytes.
fn prefix_of(key: &Key, len: usize) -> Vec<u8> {
    let mut bytes = key.encode();
    bytes.truncate(len);
    bytes
}

/// Byte prefix of every live `inline/*` key of `kind` scoped to
/// `table_id`.
pub(crate) fn inline_live_table_prefix(kind: InlineOperationKind, table_id: u64) -> Vec<u8> {
    prefix_of(
        &Key::Inline(InlineKey::Live(kind.sample(table_id))),
        INLINE_LIVE_KIND_PREFIX_LEN + size_of::<u64>(),
    )
}

/// Discriminant bytes preceding an `inline/schema` key's components:
/// subspace, `InlineKey::Schema`.
const INLINE_SCHEMA_PREFIX_LEN: usize = 2;

/// Byte prefix of every `inline/schema` key scoped to `table_id`, across
/// all schema versions.
pub(crate) fn inline_schema_table_prefix(table_id: u64) -> Vec<u8> {
    prefix_of(
        &Key::Inline(InlineKey::Schema {
            table_id,
            schema_version: 0,
        }),
        INLINE_SCHEMA_PREFIX_LEN + size_of::<u64>(),
    )
}

/// Byte prefix of every `inline/schema` key, across every table.
pub(crate) fn inline_schema_prefix() -> Vec<u8> {
    prefix_of(
        &Key::Inline(InlineKey::Schema {
            table_id: 0,
            schema_version: 0,
        }),
        INLINE_SCHEMA_PREFIX_LEN,
    )
}

/// Discriminant bytes preceding an `inline/chunk_range` key's components:
/// subspace, then `InlineKey::ChunkRange`.
const INLINE_CHUNK_RANGE_PREFIX_LEN: usize = 2;

/// Byte prefix of every chunk-range locator scoped to `table_id`.
pub(crate) fn inline_chunk_range_table_prefix(table_id: u64) -> Vec<u8> {
    prefix_of(
        &Key::Inline(InlineKey::ChunkRange {
            table_id,
            row_id_end: 0,
        }),
        INLINE_CHUNK_RANGE_PREFIX_LEN + size_of::<u64>(),
    )
}

/// The range suffix that seeks to the first chunk ending at or after
/// `row_id` within [`inline_chunk_range_table_prefix`].
pub(crate) fn inline_chunk_range_suffix(table_id: u64, row_id: u64) -> Vec<u8> {
    let prefix = inline_chunk_range_table_prefix(table_id);
    let encoded = Key::Inline(InlineKey::ChunkRange {
        table_id,
        row_id_end: row_id,
    })
    .encode();
    encoded[prefix.len()..].to_vec()
}

/// Discriminant bytes preceding an index entry's components: the `index`
/// subspace byte and the [`IndexKey`] kind byte.
const INDEX_KIND_PREFIX_LEN: usize = 2;
/// Bytes preceding an index entry's value: the kind discriminants and the
/// eight-byte index id.
const INDEX_ENTRY_PREFIX_LEN: usize = INDEX_KIND_PREFIX_LEN + size_of::<u64>();

/// The bytes one index entry stages, key and value together. Both kinds
/// cost the same: the entry prefix and the framed value, plus a row id
/// carried in the key (non-unique, where it is the final component) or as
/// the whole value (unique).
///
/// Nominal, in the sense [`nominal_key_bytes`] is: escaping can make the
/// real entry larger, never smaller.
pub(crate) fn index_entry_bytes(values: &[Option<IndexKeyValue>]) -> u64 {
    let bytes = INDEX_ENTRY_PREFIX_LEN + nominal_key_bytes(values) + size_of::<u64>();
    bytes as u64
}

/// The two entry kinds inside the `index` subspace. An index is exclusively
/// one kind, so its entries form one contiguous `(kind, index_id)` range.
// The prefix builders below have no caller until index lookups and
// reclamation land; the ranges are pinned by tests.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexKind {
    /// `index/unique`.
    Unique,
    /// `index/multi`.
    Multi,
}

/// Byte prefix of every entry of one kind, across all indexes — the range
/// the orphan sweep walks, seeking by index id.
pub(crate) fn index_kind_prefix(kind: IndexKind) -> Vec<u8> {
    let key = match kind {
        IndexKind::Unique => Key::Index(IndexKey::Unique {
            index_id: 0,
            key: CanonicalKey::empty(),
        }),
        IndexKind::Multi => Key::Index(IndexKey::Multi {
            index_id: 0,
            key: CanonicalKey::empty(),
            row_id: 0,
        }),
    };

    prefix_of(&key, INDEX_KIND_PREFIX_LEN)
}

/// Byte prefix of every entry of one index — the range reclamation sweeps
/// after a drop.
pub(crate) fn index_index_prefix(kind: IndexKind, index_id: u64) -> Vec<u8> {
    let key = match kind {
        IndexKind::Unique => Key::Index(IndexKey::Unique {
            index_id,
            key: CanonicalKey::empty(),
        }),
        IndexKind::Multi => Key::Index(IndexKey::Multi {
            index_id,
            key: CanonicalKey::empty(),
            row_id: 0,
        }),
    };
    prefix_of(&key, INDEX_ENTRY_PREFIX_LEN)
}

/// Byte prefix of every non-unique entry sharing one `(index_id, value)` —
/// the ascending scan a non-unique lookup runs. The row id is the fixed
/// eight-byte final component, so dropping it yields the value prefix.
#[allow(dead_code)]
pub(crate) fn index_multi_value_prefix(index_id: u64, value: &CanonicalKey) -> Vec<u8> {
    let mut bytes = Key::Index(IndexKey::Multi {
        index_id,
        key: value.clone(),
        row_id: 0,
    })
    .encode();
    bytes.truncate(bytes.len() - size_of::<u64>());
    bytes
}

/// The framed value bytes as they appear in an entry key of `kind`, after the
/// `(kind, index_id)` prefix — the suffix a subrange bounds against.
pub(crate) fn index_value_suffix(kind: IndexKind, index_id: u64, value: &CanonicalKey) -> Vec<u8> {
    let mut bytes = match kind {
        IndexKind::Unique => Key::Index(IndexKey::Unique {
            index_id,
            key: value.clone(),
        })
        .encode(),
        IndexKind::Multi => index_multi_value_prefix(index_id, value),
    };

    bytes.drain(..INDEX_ENTRY_PREFIX_LEN);
    bytes
}

/// The framed suffix without its trailing terminator: the escaped body that
/// every key extending the value — further columns, or the row id a
/// non-unique entry ends with — carries as its own prefix.
pub(crate) fn index_value_body(kind: IndexKind, index_id: u64, value: &CanonicalKey) -> Vec<u8> {
    let mut body = index_value_suffix(kind, index_id, value);
    body.pop();
    body
}

/// The exclusive byte bound just above every entry whose value starts with
/// `value`: the value's own entries and, when it names only a leading prefix
/// of the index's columns, every extension of it. `None` when the body is all
/// `0xff` and so has no increment.
pub(crate) fn index_value_above(
    kind: IndexKind,
    index_id: u64,
    value: &CanonicalKey,
) -> Option<Vec<u8>> {
    increment_prefix(index_value_body(kind, index_id, value))
}

/// The smallest byte string lexicographically greater than every string that
/// begins with `prefix`, or `None` when `prefix` is all `0xff`.
pub(crate) fn increment_prefix(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(last) = bytes.last_mut() {
        if *last != u8::MAX {
            *last += 1;
            return Some(bytes);
        }
        bytes.pop();
    }
    None
}

/// Byte prefix of every key in a subspace (exactly `TAG_PREFIX_LEN`
/// bytes — the same prefix the segment extractor derives).
pub(crate) fn subspace_prefix(subspace: Subspace) -> Vec<u8> {
    prefix_of(&subspace.sample(), TAG_PREFIX_LEN)
}

/// Byte prefix of every live key of `kind` scoped to `table_id`.
// No caller yet; pinned by the prefix tests below.
#[allow(dead_code)]
pub(crate) fn current_table_prefix(kind: TableScopedKind, table_id: u64) -> Vec<u8> {
    prefix_of(
        &Key::current(kind.sample(table_id)),
        CUR_KIND_PREFIX_LEN + size_of::<u64>(),
    )
}

#[cfg(test)]
mod tests {

    use proptest::prelude::*;

    use super::*;
    use crate::store::index_encoding::{IndexKeyValue, IntWidth, encode_key};

    fn be(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    // Golden vectors: the exact on-disk bytes, pinned per kind. The derive
    // assigns each enum level's discriminant as declaration index + 2;
    // these tests make that assignment part of the on-disk format.

    #[test]
    fn golden_sys_keys() {
        assert_eq!(Key::Sys(SysKey::Format).encode(), vec![0x02, 0x02]);
        assert_eq!(Key::Sys(SysKey::Head).encode(), vec![0x02, 0x03]);
        assert_eq!(Key::Sys(SysKey::Migration).encode(), vec![0x02, 0x04]);
        assert_eq!(
            Key::Sys(SysKey::MaintenanceStatus).encode(),
            vec![0x02, 0x05]
        );
    }

    #[test]
    fn golden_snapshot_key() {
        let mut expect = vec![0x03];
        expect.extend(be(42));
        assert_eq!(Key::Snapshot { snapshot_id: 42 }.encode(), expect);
    }

    /// Appended after every existing subspace, so no key written before it
    /// existed changes a byte.
    #[test]
    fn golden_changelog_key() {
        let mut expect = vec![0x09];
        expect.extend(be(42));
        assert_eq!(Key::Changelog { snapshot_id: 42 }.encode(), expect);
    }

    #[test]
    fn golden_current_file_key() {
        let mut expect = vec![0x04, 0x02, 0x07];
        expect.extend(be(1));
        expect.extend(be(2));
        let key = Key::current(EntityKey::File {
            table_id: 1,
            data_file_id: 2,
        });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_current_sort_key() {
        let mut expect = vec![0x04, 0x02, 0x0e];
        expect.extend(be(7));
        expect.extend(be(9));
        let key = Key::current(EntityKey::Sort {
            table_id: 7,
            sort_id: 9,
        });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_current_macro_key() {
        let mut expect = vec![0x04, 0x02, 0x0f];
        expect.extend(be(3));
        let key = Key::current(EntityKey::Macro { macro_id: 3 });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_history_file_key_appends_end_snapshot() {
        let mut expect = vec![0x05, 0x07];
        expect.extend(be(1));
        expect.extend(be(2));
        expect.extend(be(9));
        let key = Key::history(
            EntityKey::File {
                table_id: 1,
                data_file_id: 2,
            },
            9,
        );
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_history_macro_key_appends_end_snapshot() {
        let mut expect = vec![0x05, 0x0f];
        expect.extend(be(3));
        expect.extend(be(9));
        let key = Key::history(EntityKey::Macro { macro_id: 3 }, 9);
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_current_index_key() {
        let mut expect = vec![0x04, 0x02, 0x11];
        expect.extend(be(4));
        expect.extend(be(7));
        let key = Key::current(EntityKey::Index {
            table_id: 4,
            index_id: 7,
        });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_current_mapping_key() {
        let mut expect = vec![0x04, 0x02, 0x10];
        expect.extend(be(4));
        expect.extend(be(21));
        let key = Key::current(EntityKey::Mapping {
            table_id: 4,
            mapping_id: 21,
        });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_gcfile_key() {
        let mut expect = vec![0x04, 0x03];
        expect.extend(be(5));
        let key = Key::Current(CurrentKey::GcFile { data_file_id: 5 });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_inline_ins_key() {
        let mut expect = vec![0x06, 0x03, 0x02];
        for v in [7, 3, 11, 0] {
            expect.extend(be(v));
        }
        let key = Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id: 7,
            schema_version: 3,
            begin_snapshot: 11,
            chunk_seq: 0,
        }));
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_inline_file_delete_table_key() {
        let mut expect = vec![0x06, 0x05];
        expect.extend(be(7));
        let key = Key::Inline(InlineKey::FileDeleteTable { table_id: 7 });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_inline_chunk_range_key() {
        let mut expect = vec![0x06, 0x06];
        expect.extend(be(7));
        expect.extend(be(29));
        let key = Key::Inline(InlineKey::ChunkRange {
            table_id: 7,
            row_id_end: 29,
        });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_arch_kinds_are_distinct_from_live() {
        let ops = [
            InlineOperation::Insert {
                table_id: 7,
                schema_version: 3,
                begin_snapshot: 11,
                chunk_seq: 0,
            },
            InlineOperation::InlineDelete {
                table_id: 7,
                row_id: 9,
            },
            InlineOperation::FileDelete {
                table_id: 7,
                data_file_id: 2,
                row_id: 9,
            },
        ];

        for op in ops {
            let live = Key::Inline(InlineKey::Live(op)).encode();
            let arch = Key::Inline(InlineKey::Arch(op)).encode();
            // Same subspace and components; only the form byte differs,
            // and every archived key sorts after every live key.
            assert_eq!(live[0], 0x06);
            assert_eq!(arch[0], 0x06);
            assert_ne!(live[1], arch[1]);
            assert_eq!(live[2..], arch[2..]);
            assert!(arch > live);
        }
    }

    fn canon(values: &[IndexKeyValue]) -> CanonicalKey {
        encode_key(values).unwrap()
    }

    #[test]
    fn golden_index_unique_key_leads_with_subspace_and_index() {
        let key = Key::Index(IndexKey::Unique {
            index_id: 1,
            key: canon(&[IndexKeyValue::Int {
                value: 7,
                width: IntWidth::I64,
            }]),
        });
        let bytes = key.encode();
        // Subspace byte, then the `Unique` discriminant, then the index id.
        let mut prefix = vec![0x07, 0x02];
        prefix.extend(be(1));
        assert!(bytes.starts_with(&prefix), "{bytes:?}");
    }

    #[test]
    fn borrowed_index_entry_encoder_matches_owned_key() {
        let canonical = canon(&[
            IndexKeyValue::Str("a\0\u{1}z".to_owned()),
            IndexKeyValue::Bytes(vec![0, 1, 0xff]),
        ]);
        for unique in [false, true] {
            for row_id in [0, 1_u64 << 56, u64::MAX] {
                let owned = if unique {
                    Key::Index(IndexKey::Unique {
                        index_id: 7,
                        key: canonical.clone(),
                    })
                } else {
                    Key::Index(IndexKey::Multi {
                        index_id: 7,
                        key: canonical.clone(),
                        row_id,
                    })
                };
                assert_eq!(
                    encode_index_entry(7, unique, &canonical, row_id),
                    owned.encode()
                );
            }
        }
    }

    /// A non-unique entry key ends in a raw row id, directly after the
    /// value's terminator. A row id whose leading byte is the escape byte
    /// (`0x01`) must not be mistaken for an escaped byte of the value —
    /// that is every row id in `[2^56, 2^57)`.
    #[test]
    fn index_multi_key_roundtrips_when_the_row_id_leads_with_the_escape_byte() {
        // The last case is the shortest possible value — no components at
        // all, which frames to the bare terminator.
        let values = [
            vec![IndexKeyValue::Str("a".to_owned())],
            vec![IndexKeyValue::Int {
                value: 0,
                width: IntWidth::I64,
            }],
            vec![IndexKeyValue::Bytes(vec![0, 1, 2])],
            vec![],
        ];
        for row_id in [1u64 << 56, (1 << 56) + 1, (1 << 57) - 1] {
            for value in &values {
                let key = Key::Index(IndexKey::Multi {
                    index_id: 1,
                    key: canon(value),
                    row_id,
                });
                assert_eq!(Key::decode(&key.encode()).unwrap(), key, "row_id {row_id}");
            }
        }
    }

    /// The value rides as a storekey byte string — `0x00` and `0x01` escape
    /// behind `0x01`, a `0x00` terminates — and the raw row id follows it.
    /// A [`CanonicalKey`](crate::store::index_encoding::CanonicalKey) already
    /// holds framed bytes, so the entry key frames them a second time.
    /// Pinned because these bytes are on disk: the decode fix must not move
    /// them.
    #[test]
    fn golden_index_multi_value_framing_and_trailing_row_id() {
        let key = Key::Index(IndexKey::Multi {
            index_id: 1,
            key: canon(&[IndexKeyValue::Bytes(vec![0x00, 0x01, 0x41])]),
            row_id: 1 << 56,
        });
        let mut expected = vec![0x07, 0x03];
        expected.extend(be(1));
        // The canonical key is a non-null flag (`00`, the ascending
        // NULLS-LAST flag) then the value framed as a storekey byte string
        // (`00 01 41` -> `01 00 01 01 41 00`); the outer key framing escapes
        // each of those bytes again and terminates.
        expected.extend([
            0x01, 0x00, // 0x00 — non-null flag
            0x01, 0x01, // 0x01 — framed value byte 0 (escape lead)
            0x01, 0x00, // 0x00 — framed value byte 1 (escaped 0x00)
            0x01, 0x01, // 0x01 — framed value byte 2 (escape lead)
            0x01, 0x01, // 0x01 — framed value byte 3 (escaped 0x01)
            0x41, // 'A' — framed value byte 4
            0x01, 0x00, // 0x00 — framed value byte 5 (terminator)
            0x00, // outer terminator
        ]);
        expected.extend(be(1 << 56));
        assert_eq!(key.encode(), expected);
    }

    #[test]
    fn golden_index_multi_key_leads_with_subspace_and_index() {
        let key = Key::Index(IndexKey::Multi {
            index_id: 1,
            key: canon(&[IndexKeyValue::Int {
                value: 7,
                width: IntWidth::I64,
            }]),
            row_id: 3,
        });
        let bytes = key.encode();
        let mut prefix = vec![0x07, 0x03];
        prefix.extend(be(1));
        assert!(bytes.starts_with(&prefix), "{bytes:?}");
        // The row id is the final component, appended after the value.
        assert!(bytes.ends_with(&be(3)), "{bytes:?}");
    }

    #[test]
    fn index_composite_framing_distinct_at_key_level() {
        let ab_c = Key::Index(IndexKey::Unique {
            index_id: 1,
            key: canon(&[
                IndexKeyValue::Str("ab".into()),
                IndexKeyValue::Str("c".into()),
            ]),
        });
        let a_bc = Key::Index(IndexKey::Unique {
            index_id: 1,
            key: canon(&[
                IndexKeyValue::Str("a".into()),
                IndexKeyValue::Str("bc".into()),
            ]),
        });
        assert_ne!(ab_c.encode(), a_bc.encode());
    }

    #[test]
    fn index_index_prefix_covers_one_index_and_kind() {
        let value = canon(&[IndexKeyValue::Str("v".into())]);

        let unique = Key::Index(IndexKey::Unique {
            index_id: 4,
            key: value.clone(),
        })
        .encode();
        assert!(unique.starts_with(&index_index_prefix(IndexKind::Unique, 4)));
        // A different index id, and the other kind, do not match.
        assert!(!unique.starts_with(&index_index_prefix(IndexKind::Unique, 5)));
        assert!(!unique.starts_with(&index_index_prefix(IndexKind::Multi, 4)));

        let multi = Key::Index(IndexKey::Multi {
            index_id: 4,
            key: value,
            row_id: 1,
        })
        .encode();
        assert!(multi.starts_with(&index_index_prefix(IndexKind::Multi, 4)));
        assert!(!multi.starts_with(&index_index_prefix(IndexKind::Unique, 4)));
    }

    /// The orphan sweep seeks past a live index by scanning from
    /// `index_index_prefix(kind, id + 1)`, which is only sound if index ids
    /// sort ascending within a kind and every index prefix carries the
    /// kind prefix.
    #[test]
    fn index_index_prefixes_sort_ascending_within_a_kind() {
        for kind in [IndexKind::Unique, IndexKind::Multi] {
            let kind_prefix = index_kind_prefix(kind);
            let mut previous = index_index_prefix(kind, 0);
            assert!(previous.starts_with(&kind_prefix));

            for index_id in [1, 2, 255, 256, u64::MAX - 1, u64::MAX] {
                let prefix = index_index_prefix(kind, index_id);
                assert!(prefix.starts_with(&kind_prefix));
                assert!(
                    prefix > previous,
                    "index {index_id} must sort after its predecessor in {kind:?}"
                );
                previous = prefix;
            }
        }

        // The two kinds occupy disjoint ranges, so a per-kind scan never
        // walks into the other.
        assert_ne!(
            index_kind_prefix(IndexKind::Unique),
            index_kind_prefix(IndexKind::Multi)
        );
    }

    #[test]
    fn index_multi_value_prefix_covers_all_row_ids_of_one_value() {
        let value = canon(&[IndexKeyValue::Str("shared".into())]);
        let prefix = index_multi_value_prefix(9, &value);
        for row_id in [0, 1, u64::MAX] {
            let key = Key::Index(IndexKey::Multi {
                index_id: 9,
                key: value.clone(),
                row_id,
            });
            assert!(key.encode().starts_with(&prefix), "row_id {row_id}");
        }
        // A different value, and a different index, do not match.
        let other_value = canon(&[IndexKeyValue::Str("different".into())]);
        assert!(
            !Key::Index(IndexKey::Multi {
                index_id: 9,
                key: other_value,
                row_id: 0,
            })
            .encode()
            .starts_with(&prefix)
        );
        assert!(
            !Key::Index(IndexKey::Multi {
                index_id: 10,
                key: value,
                row_id: 0,
            })
            .encode()
            .starts_with(&prefix)
        );
    }

    #[test]
    fn index_keys_roundtrip() {
        let keys = [
            Key::Index(IndexKey::Unique {
                index_id: 5,
                key: canon(&[IndexKeyValue::Str("hello".into())]),
            }),
            Key::Index(IndexKey::Multi {
                index_id: 9,
                key: canon(&[
                    IndexKeyValue::Int {
                        value: -3,
                        width: IntWidth::I32,
                    },
                    IndexKeyValue::Bool(true),
                ]),
                row_id: 42,
            }),
        ];
        for key in keys {
            let decoded = Key::decode(&key.encode()).unwrap();
            assert_eq!(decoded, key);
        }
    }

    #[test]
    fn decode_roundtrips_representative_keys() {
        let keys = [
            Key::Sys(SysKey::Head),
            Key::Snapshot {
                snapshot_id: u64::MAX,
            },
            Key::current(EntityKey::Column {
                table_id: 3,
                column_id: 4,
            }),
            Key::history(
                EntityKey::Option {
                    scope_kind: 2,
                    scope_id: 3,
                },
                8,
            ),
            Key::Current(CurrentKey::GcFile { data_file_id: 0 }),
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                table_id: 1,
                data_file_id: 2,
                row_id: 3,
            })),
            Key::Inline(InlineKey::Arch(InlineOperation::FileDelete {
                table_id: 1,
                data_file_id: 2,
                row_id: 3,
            })),
            Key::Inline(InlineKey::FileDeleteTable { table_id: 1 }),
        ];
        for key in keys {
            let decoded = Key::decode(&key.encode()).unwrap();
            assert_eq!(decoded, key);
        }
    }

    #[test]
    fn decode_rejects_unknown_discriminants_and_bad_arity() {
        // Unknown subspace discriminant (below and above the valid range).
        assert!(Key::decode(&[0x00, 0x02]).is_err());
        assert!(Key::decode(&[0x7f, 0x02]).is_err());
        // Unknown kind within a valid subspace.
        assert!(Key::decode(&[0x02, 0x7f]).is_err());
        assert!(Key::decode(&[0x06, 0x7f]).is_err());
        // Truncated components.
        let mut short = Key::Snapshot { snapshot_id: 1 }.encode();
        short.pop();
        assert!(Key::decode(&short).is_err());
        // Trailing bytes.
        let mut long = Key::Sys(SysKey::Head).encode();
        long.push(0);
        assert!(Key::decode(&long).is_err());
        // Empty.
        assert!(Key::decode(&[]).is_err());
    }

    // Prefixes: derived by encode-and-truncate, so they are byte prefixes
    // of full keys by construction — these tests pin the truncation
    // lengths against the structure.

    #[test]
    fn prefixes_are_byte_prefixes_of_full_keys() {
        let key = Key::current(EntityKey::File {
            table_id: 1,
            data_file_id: 2,
        });
        let bytes = key.encode();
        assert!(bytes.starts_with(&subspace_prefix(Subspace::Current)));
        assert!(bytes.starts_with(&current_table_prefix(TableScopedKind::File, 1)));
        // A different table's prefix must not match.
        assert!(!bytes.starts_with(&current_table_prefix(TableScopedKind::File, 2)));
        // A different kind's prefix must not match.
        assert!(!bytes.starts_with(&current_table_prefix(TableScopedKind::DeleteFile, 1)));
    }

    /// Every table-scoped kind's prefix matches a live key of that kind
    /// with the same table id and rejects a different table id.
    #[test]
    fn table_scoped_prefixes_cover_all_kinds() {
        let kinds = [
            TableScopedKind::Column,
            TableScopedKind::Partition,
            TableScopedKind::File,
            TableScopedKind::DeleteFile,
            TableScopedKind::FileColumnStats,
            TableScopedKind::TableStats,
            TableScopedKind::TableColumnStats,
            TableScopedKind::Sort,
        ];
        for kind in kinds {
            let key = Key::current(kind.sample(9));
            let bytes = key.encode();
            assert!(
                bytes.starts_with(&current_table_prefix(kind, 9)),
                "{kind:?} prefix must match its own key"
            );
            assert!(
                !bytes.starts_with(&current_table_prefix(kind, 10)),
                "{kind:?} prefix must not match another table"
            );
        }
    }

    /// Every inline-op kind's live prefix matches a live key of that kind
    /// with the same table id and rejects a different table id or kind.
    #[test]
    fn inline_live_table_prefixes_cover_all_operation_kinds() {
        let kinds = [
            InlineOperationKind::Insert,
            InlineOperationKind::InlineDelete,
            InlineOperationKind::FileDelete,
        ];
        for kind in kinds {
            let key = Key::Inline(InlineKey::Live(kind.sample(9)));
            let bytes = key.encode();
            assert!(
                bytes.starts_with(&inline_live_table_prefix(kind, 9)),
                "{kind:?} prefix must match its own key"
            );
            assert!(
                !bytes.starts_with(&inline_live_table_prefix(kind, 10)),
                "{kind:?} prefix must not match another table"
            );
        }
        // Different op kinds must not share a prefix even for the same table.
        assert!(
            !inline_live_table_prefix(InlineOperationKind::InlineDelete, 9)
                .starts_with(&inline_live_table_prefix(InlineOperationKind::Insert, 9))
        );
        // An archived key never matches a live prefix.
        let arch = Key::Inline(InlineKey::Arch(InlineOperationKind::Insert.sample(9))).encode();
        assert!(!arch.starts_with(&inline_live_table_prefix(InlineOperationKind::Insert, 9)));
    }

    /// The `inline/schema` prefix matches every schema version of the
    /// same table and rejects a different table.
    #[test]
    fn inline_schema_table_prefix_covers_all_versions() {
        for schema_version in [0, 1, 42] {
            let key = Key::Inline(InlineKey::Schema {
                table_id: 9,
                schema_version,
            });
            assert!(key.encode().starts_with(&inline_schema_table_prefix(9)));
        }
        let other_table = Key::Inline(InlineKey::Schema {
            table_id: 10,
            schema_version: 0,
        });
        assert!(
            !other_table
                .encode()
                .starts_with(&inline_schema_table_prefix(9))
        );
    }

    // Property tests: roundtrip, order preservation, uniqueness.

    fn arb_entity() -> impl Strategy<Value = EntityKey> {
        prop_oneof![
            any::<u64>().prop_map(|schema_id| EntityKey::Schema { schema_id }),
            any::<u64>().prop_map(|table_id| EntityKey::Table { table_id }),
            any::<u64>().prop_map(|view_id| EntityKey::View { view_id }),
            any::<(u64, u64)>().prop_map(|(table_id, column_id)| EntityKey::Column {
                table_id,
                column_id
            }),
            any::<(u64, u64)>().prop_map(|(table_id, partition_id)| EntityKey::Partition {
                table_id,
                partition_id
            }),
            any::<(u64, u64)>().prop_map(|(table_id, data_file_id)| EntityKey::File {
                table_id,
                data_file_id
            }),
            any::<(u64, u64)>().prop_map(|(table_id, delete_file_id)| EntityKey::DeleteFile {
                table_id,
                delete_file_id
            }),
            any::<(u64, u64, u64)>().prop_map(|(table_id, data_file_id, column_id)| {
                EntityKey::FileColumnStats {
                    table_id,
                    data_file_id,
                    column_id,
                }
            }),
            any::<u64>().prop_map(|table_id| EntityKey::TableStats { table_id }),
            any::<(u64, u64)>().prop_map(|(table_id, column_id)| EntityKey::TableColumnStats {
                table_id,
                column_id
            }),
            any::<(u64, u64)>()
                .prop_map(|(table_id, sort_id)| EntityKey::Sort { table_id, sort_id }),
            any::<u64>().prop_map(|object_id| EntityKey::Tag { object_id }),
            any::<(u64, u64)>().prop_map(|(scope_kind, scope_id)| EntityKey::Option {
                scope_kind,
                scope_id
            }),
            any::<u64>().prop_map(|macro_id| EntityKey::Macro { macro_id }),
            any::<(u64, u64)>().prop_map(|(table_id, mapping_id)| EntityKey::Mapping {
                table_id,
                mapping_id
            }),
            any::<(u64, u64)>()
                .prop_map(|(table_id, index_id)| EntityKey::Index { table_id, index_id }),
        ]
    }

    fn arb_inline_op() -> impl Strategy<Value = InlineOperation> {
        prop_oneof![
            any::<(u64, u64, u64, u64)>().prop_map(
                |(table_id, schema_version, begin_snapshot, chunk_seq)| InlineOperation::Insert {
                    table_id,
                    schema_version,
                    begin_snapshot,
                    chunk_seq
                }
            ),
            any::<(u64, u64)>()
                .prop_map(|(table_id, row_id)| InlineOperation::InlineDelete { table_id, row_id }),
            any::<(u64, u64, u64)>().prop_map(|(table_id, data_file_id, row_id)| {
                InlineOperation::FileDelete {
                    table_id,
                    data_file_id,
                    row_id,
                }
            }),
        ]
    }

    fn arb_canonical_key() -> impl Strategy<Value = CanonicalKey> {
        // Arbitrary component byte strings, kept well under the size cap so
        // `encode_key` never rejects.
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..8), 0..4).prop_map(
            |components| {
                let values = components
                    .into_iter()
                    .map(IndexKeyValue::Bytes)
                    .collect::<Vec<_>>();
                encode_key(&values).unwrap()
            },
        )
    }

    fn arb_index() -> impl Strategy<Value = IndexKey> {
        prop_oneof![
            (any::<u64>(), arb_canonical_key())
                .prop_map(|(index_id, key)| IndexKey::Unique { index_id, key }),
            (any::<u64>(), arb_canonical_key(), any::<u64>()).prop_map(
                |(index_id, key, row_id)| {
                    IndexKey::Multi {
                        index_id,
                        key,
                        row_id,
                    }
                }
            ),
        ]
    }

    fn arb_key() -> impl Strategy<Value = Key> {
        prop_oneof![
            Just(Key::Sys(SysKey::Format)),
            Just(Key::Sys(SysKey::Head)),
            Just(Key::Sys(SysKey::Migration)),
            Just(Key::Sys(SysKey::MaintenanceStatus)),
            any::<u64>().prop_map(|snapshot_id| Key::Snapshot { snapshot_id }),
            arb_entity().prop_map(Key::current),
            (arb_entity(), any::<u64>()).prop_map(|(entity, end)| Key::history(entity, end)),
            any::<u64>().prop_map(|data_file_id| Key::Current(CurrentKey::GcFile { data_file_id })),
            any::<(u64, u64)>().prop_map(|(table_id, schema_version)| {
                Key::Inline(InlineKey::Schema {
                    table_id,
                    schema_version,
                })
            }),
            arb_inline_op().prop_map(|op| Key::Inline(InlineKey::Live(op))),
            arb_inline_op().prop_map(|op| Key::Inline(InlineKey::Arch(op))),
            any::<u64>().prop_map(|table_id| Key::Inline(InlineKey::FileDeleteTable { table_id })),
            any::<(u64, u64)>().prop_map(|(table_id, row_id_end)| {
                Key::Inline(InlineKey::ChunkRange {
                    table_id,
                    row_id_end,
                })
            }),
            arb_index().prop_map(Key::Index),
            any::<u64>().prop_map(|snapshot_id| Key::Changelog { snapshot_id }),
        ]
    }

    proptest! {
        #[test]
        fn borrowed_index_entry_encoding_matches_owned(
            index_id in any::<u64>(),
            key in arb_canonical_key(),
            row_id in any::<u64>(),
            unique in any::<bool>(),
        ) {
            let owned = if unique {
                Key::Index(IndexKey::Unique {
                    index_id,
                    key: key.clone(),
                })
            } else {
                Key::Index(IndexKey::Multi {
                    index_id,
                    key: key.clone(),
                    row_id,
                })
            };
            prop_assert_eq!(
                encode_index_entry(index_id, unique, &key, row_id),
                owned.encode()
            );
        }

        #[test]
        fn roundtrip(key in arb_key()) {
            let decoded = Key::decode(&key.encode()).unwrap();
            prop_assert_eq!(decoded, key);
        }

        #[test]
        fn encoded_keys_are_unique(a in arb_key(), b in arb_key()) {
            if a != b {
                prop_assert_ne!(a.encode(), b.encode());
            }
        }

        /// The encoding preserves the typed order: every prefix scan and
        /// range bound relies on byte order agreeing with key order.
        #[test]
        fn encoding_preserves_order(a in arb_key(), b in arb_key()) {
            prop_assert_eq!(a.cmp(&b), a.encode().cmp(&b.encode()));
        }

        // Decode is total: arbitrary bytes decode or fail as
        // `Corruption`, never panic.
        #[test]
        fn decode_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let _ = Key::decode(&bytes);
        }

        // Steer past the subspace discriminant so component parsing
        // sees the garbage too.
        #[test]
        fn decode_garbage_in_valid_subspace_never_panics(
            subspace in 0x02u8..=0x07,
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let mut encoded = vec![subspace];
            encoded.extend(&bytes);
            let _ = Key::decode(&encoded);
        }

        // One corrupted byte in a valid key decodes to some key or
        // fails loudly, never panics.
        #[test]
        fn decode_corrupted_valid_key_never_panics(
            key in arb_key(),
            index in any::<proptest::sample::Index>(),
            byte in any::<u8>(),
        ) {
            let mut bytes = key.encode();
            let position = index.index(bytes.len());
            bytes[position] = byte;
            let _ = Key::decode(&bytes);
        }
    }

    mod index_value_bounds {
        use proptest::prelude::*;

        use super::*;
        use crate::store::index_encoding::{
            Direction, IndexKeyValue, IntWidth, NullOrder, encode_ordered_values,
        };

        /// Values across the categories whose framing the bound arithmetic has
        /// to survive: fixed-width integers, and the variable-length
        /// strings and blobs whose escaped bodies can end in `0xff`.
        fn arbitrary_value() -> impl Strategy<Value = IndexKeyValue> {
            prop_oneof![
                any::<i64>().prop_map(|value| IndexKeyValue::Int {
                    value: i128::from(value),
                    width: IntWidth::I64,
                }),
                any::<u32>().prop_map(|value| IndexKeyValue::UInt {
                    value: u128::from(value),
                    width: IntWidth::I32,
                }),
                any::<bool>().prop_map(IndexKeyValue::Bool),
                ".{0,8}".prop_map(IndexKeyValue::Str),
                proptest::collection::vec(any::<u8>(), 0..8).prop_map(IndexKeyValue::Bytes),
            ]
        }

        /// A two-column key, or its one-column leading prefix, under one shared
        /// direction. NULLS placement is irrelevant here: every value is
        /// present.
        fn encode(values: &[IndexKeyValue], descending: bool) -> CanonicalKey {
            let direction = if descending {
                Direction::Descending
            } else {
                Direction::Ascending
            };
            encode_ordered_values(
                &values.iter().cloned().map(Some).collect::<Vec<_>>(),
                &[direction, direction],
                &[NullOrder::Last, NullOrder::Last],
            )
            .expect("values within the key size cap encode")
        }

        /// The bytes a stored non-unique entry carries after the index prefix:
        /// the framed value, then the raw row id.
        fn multi_entry(index_id: u64, canon: &CanonicalKey, row_id: u64) -> Vec<u8> {
            let full = Key::Index(IndexKey::Multi {
                index_id,
                key: canon.clone(),
                row_id,
            })
            .encode();
            full[INDEX_ENTRY_PREFIX_LEN..].to_vec()
        }

        proptest! {
            /// The bound above a one-column prefix outranks every key that
            /// extends it: the value's own entry, an entry carrying a further
            /// column, and — on a non-unique index — the trailing row id.
            #[test]
            fn above_a_prefix_outranks_every_extension(
                leading in arbitrary_value(),
                extension in arbitrary_value(),
                row_id: u64,
                descending: bool,
            ) {
                let index_id = 7;
                let prefix = encode(std::slice::from_ref(&leading), descending);
                let extended = encode(&[leading, extension], descending);

                for kind in [IndexKind::Unique, IndexKind::Multi] {
                    let bound = index_value_above(kind, index_id, &prefix)
                        .expect("a framed body is never all 0xff");
                    prop_assert!(bound > index_value_suffix(kind, index_id, &prefix));
                    prop_assert!(bound > index_value_suffix(kind, index_id, &extended));
                }
                prop_assert!(
                    index_value_above(IndexKind::Multi, index_id, &prefix)
                        .expect("a framed body is never all 0xff")
                        > multi_entry(index_id, &extended, row_id)
                );
            }

            /// The bound stops below every value that sorts above the prefix, so
            /// a range ending there admits no row it should exclude.
            #[test]
            fn above_a_prefix_stops_below_a_larger_value(
                one in arbitrary_value(),
                two in arbitrary_value(),
                descending: bool,
            ) {
                let index_id = 7;
                let kind = IndexKind::Unique;
                let first = encode(&[one], descending);
                let second = encode(&[two], descending);

                let (lower, higher) = if index_value_suffix(kind, index_id, &first)
                    < index_value_suffix(kind, index_id, &second)
                {
                    (&first, &second)
                } else {
                    (&second, &first)
                };
                prop_assume!(lower != higher);

                let bound = index_value_above(kind, index_id, lower)
                    .expect("a framed body is never all 0xff");
                prop_assert!(bound <= index_value_suffix(kind, index_id, higher));
            }
        }
    }
}
