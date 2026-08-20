//! Public domain types: ids and value structs, hand-written and decoupled
//! from the wire types so on-disk field evolution never becomes a public
//! breaking change.

/// Declares a newtype id over the catalog's `u64` id space.
macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            /// Wraps a raw id.
            #[must_use]
            pub const fn new(id: u64) -> Self {
                Self(id)
            }

            /// The raw id.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(
    /// Identifies a committed snapshot.
    SnapshotId
);
id_type!(
    /// Identifies a schema.
    SchemaId
);
id_type!(
    /// Identifies a table.
    TableId
);
id_type!(
    /// Identifies a column within its table (a field id: stable across
    /// renames and never reused, even after the column is dropped).
    ColumnId
);
id_type!(
    /// Identifies a data file.
    DataFileId
);
id_type!(
    /// Identifies a delete file.
    DeleteFileId
);
id_type!(
    /// Identifies a view.
    ViewId
);
id_type!(
    /// Identifies a macro.
    MacroId
);
id_type!(
    /// Identifies a column mapping (allocated by DuckLake from the same
    /// counter as data-file ids).
    MappingId
);
id_type!(
    /// Identifies an equality index within its table (allocated from the
    /// global catalog-id counter).
    IndexId
);
id_type!(
    /// Identifies a partition spec within its table (allocated from the
    /// global catalog-id counter).
    PartitionId
);
id_type!(
    /// Identifies a sort spec within its table (allocated from the global
    /// catalog-id counter).
    SortId
);

/// An instant, as microseconds from the Unix epoch in UTC — the precision
/// the catalog persists and the extension ABI carries. Negative counts
/// precede the epoch.
///
/// Conversion to and from the raw count is total in both directions, so a
/// timestamp survives a round trip through storage or the ABI unchanged and
/// neither boundary needs a range check.
///
/// ```
/// use moraine::Timestamp;
///
/// let stamp = Timestamp::from_micros(1_700_000_000_000_000);
/// assert_eq!(stamp.as_micros(), 1_700_000_000_000_000);
/// assert!(stamp > Timestamp::UNIX_EPOCH);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The Unix epoch itself.
    pub const UNIX_EPOCH: Self = Self(0);

    /// Wraps a microsecond count from the Unix epoch.
    #[must_use]
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }

    /// The microsecond count from the Unix epoch.
    #[must_use]
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Reads the system clock. Clamped, never panicking: a clock before the
    /// epoch stamps the epoch.
    #[must_use]
    pub fn now() -> Self {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(Self::UNIX_EPOCH, |elapsed| {
                Self(i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX))
            })
    }
}

/// A data file to register: the file already exists on object storage
/// (data before metadata). `row_id_start` is allocated by the commit,
/// never caller-provided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFile {
    /// Object-store path of the file.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format (e.g. `"parquet"`).
    pub file_format: String,
    /// Number of rows in the file.
    pub record_count: u64,
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// Encryption key material, verbatim — an opaque string moraine
    /// stores and returns but never interprets.
    pub encryption_key: Option<String>,
    /// The partition this file falls in: one value per key of the table's
    /// live partition spec, in key order, each rendered as text and stored
    /// verbatim. Empty for a table with no live spec; a partitioned table
    /// requires one value per key. The spec itself is not named here — a
    /// file is always written under the one in force, and the commit
    /// records which that was.
    pub partition_values: Vec<String>,
    /// Per-column statistics carried with the registration. Every entry
    /// must reference a live column of the table.
    pub column_stats: Vec<FileColumnStats>,
}

/// Per-column statistics of one data file. Min/max are opaque strings,
/// stored verbatim — moraine never interprets or merges them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileColumnStats {
    /// The column these statistics describe.
    pub column_id: ColumnId,
    /// Compressed size of this column in the file.
    pub column_size_bytes: u64,
    /// Number of values (rows) for this column.
    pub value_count: u64,
    /// Number of NULLs.
    pub null_count: u64,
    /// Minimum value, verbatim.
    pub min_value: Option<String>,
    /// Maximum value, verbatim.
    pub max_value: Option<String>,
    /// Whether the column contains NaN (floating-point columns).
    pub contains_nan: Option<bool>,
    /// Extra statistics, verbatim.
    pub extra_stats: Option<String>,
}

/// A delete file to register, targeting one data file's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFile {
    /// The data file whose rows this delete file removes.
    pub data_file_id: DataFileId,
    /// Object-store path of the delete file.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format (e.g. `"parquet"`).
    pub format: String,
    /// Number of deleted rows recorded in the file.
    pub delete_count: u64,
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// Encryption key material, verbatim — an opaque string moraine
    /// stores and returns but never interprets.
    pub encryption_key: Option<String>,
}

/// One key of a partition spec: a source column and the transform applied
/// to it. Transforms are stored verbatim as DuckLake writes them
/// (`identity`, `year`, `bucket(16)`, …); moraine never parses or applies
/// one. A bare partition column carries `identity` rather than an empty
/// transform, so every key is explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumnDef {
    /// The source column, by field id — never by name, so a rename leaves
    /// the spec intact.
    pub column: ColumnId,
    /// The transform, verbatim.
    pub transform: String,
}

/// A table's partition spec: its partition keys in order. A table has at
/// most one live spec; setting a new one ends the old, and files written
/// under the old spec keep referencing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    /// The spec's id.
    pub id: PartitionId,
    /// The partition keys, in `partition_key_index` order.
    pub columns: Vec<PartitionColumnDef>,
}

/// One key of a sort spec: a SQL expression and how it orders. Every
/// field is stored verbatim as DuckLake writes it — the expression and its
/// `dialect`, the `sort_direction` (`ASC`/`DESC`), the `null_order`
/// (`NULLS_FIRST`/`NULLS_LAST`) — and moraine parses or evaluates none of
/// them.
///
/// Note the contrast with [`PartitionColumnDef`]: a sort key names its
/// column inside the expression string, not by field id, so a column
/// rename does not carry into the spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKeyDef {
    /// The sort expression, verbatim.
    pub expression: String,
    /// The SQL dialect the expression is written in.
    pub dialect: String,
    /// The sort direction, verbatim.
    pub sort_direction: String,
    /// The null placement, verbatim.
    pub null_order: String,
}

/// A table's sort spec: its sort keys in order. A table has at most one
/// live spec; setting a new one ends the old. Unlike a partition spec, no
/// data file records the sort spec it was written under — a sort spec is a
/// write-time instruction, not file provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    /// The spec's id.
    pub id: SortId,
    /// The sort keys, in `sort_key_index` order.
    pub keys: Vec<SortKeyDef>,
}

/// A catalog object a tag can be attached to. Schemas, tables, and views
/// share one id space, so the variant selects which entity the verb
/// validates against and how the change is classified, not how the tag is
/// keyed. Column tags are carried on the column itself and are not
/// reachable here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagTarget {
    /// One schema.
    Schema(SchemaId),
    /// One table.
    Table(TableId),
    /// One view.
    View(ViewId),
}

impl TagTarget {
    /// The tagged object's id — the key the tag container is stored under.
    #[must_use]
    pub const fn object_id(self) -> u64 {
        match self {
            Self::Schema(id) => id.get(),
            Self::Table(id) => id.get(),
            Self::View(id) => id.get(),
        }
    }
}

/// A live data file, as read from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileInfo {
    /// The file's id.
    pub id: DataFileId,
    /// Object-store path.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format.
    pub file_format: String,
    /// Number of rows.
    pub record_count: u64,
    /// Total size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// First row id of the file's dense per-table row-id range; `None`
    /// when the file's rows carry explicit per-row ids instead
    /// (compaction outputs).
    pub row_id_start: Option<u64>,
    /// Encryption key material, verbatim.
    pub encryption_key: Option<String>,
    /// The partition spec the file was written under, if any. Files
    /// written under different specs coexist, each naming its own.
    pub partition_id: Option<PartitionId>,
    /// The file's value per key of that spec, in key order.
    pub partition_values: Vec<String>,
    /// The newest snapshot among the file's rows, when they were inserted
    /// across more than one — a flush carries rows from every snapshot it
    /// drains, so its record is backdated to the earliest and bounded
    /// here by the latest. `None` when every row arrived at the file's
    /// begin snapshot.
    pub partial_max: Option<SnapshotId>,
}

/// Where one stable row id may physically live at head.
///
/// A row id can yield more than one candidate: an update can leave an
/// expired copy in an otherwise-current file while writing the visible copy
/// elsewhere. Choosing between them stays DuckLake's delete and snapshot
/// processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileRowCandidate {
    /// The stable row id located.
    pub row_id: u64,
    /// The file holding it, or `None` for a live inlined row and for one
    /// located nowhere at all.
    pub data_file_id: Option<DataFileId>,
}

/// A live delete file, as read from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFileInfo {
    /// The delete file's id.
    pub id: DeleteFileId,
    /// The data file it targets.
    pub data_file_id: DataFileId,
    /// Object-store path.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format.
    pub format: String,
    /// Number of deleted rows.
    pub delete_count: u64,
    /// Total size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// Encryption key material, verbatim.
    pub encryption_key: Option<String>,
}

/// A table's statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStats {
    /// Total rows across the table's live data files.
    pub record_count: u64,
    /// Total bytes across the table's live data files.
    pub file_size_bytes: u64,
    /// The next row id to allocate; advances with every registration and
    /// never regresses.
    pub next_row_id: u64,
}

/// A column's table-level statistics. Min/max are opaque strings, stored
/// verbatim — moraine never interprets or merges them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnStats {
    /// Whether the column contains NULLs.
    pub contains_null: Option<bool>,
    /// Whether the column contains NaN.
    pub contains_nan: Option<bool>,
    /// Minimum value, verbatim.
    pub min_value: Option<String>,
    /// Maximum value, verbatim.
    pub max_value: Option<String>,
    /// Extra statistics, verbatim.
    pub extra_stats: Option<String>,
}

/// A live schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    /// The schema's id.
    pub id: SchemaId,
    /// The schema's name, unique among live schemas.
    pub name: String,
}

/// A live table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// The table's id.
    pub id: TableId,
    /// The schema the table belongs to.
    pub schema_id: SchemaId,
    /// The table's name, unique among live tables of its schema.
    pub name: String,
}

/// A live view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewInfo {
    /// The view's id.
    pub id: ViewId,
    /// The schema the view belongs to.
    pub schema_id: SchemaId,
    /// The view's name, unique among the schema's live tables and views.
    pub name: String,
    /// SQL dialect of the definition.
    pub dialect: String,
    /// The view's defining SQL.
    pub sql: String,
}

/// One parameter of a macro implementation. An absent default stores
/// `default_value: None` with `default_value_type` `"unknown"`, matching
/// the row DuckLake writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroParameterDef {
    /// Parameter name.
    pub name: String,
    /// DuckLake type string; `"unknown"` for an untyped parameter.
    pub parameter_type: String,
    /// Default value rendered as a string, if the parameter has one.
    pub default_value: Option<String>,
    /// DuckLake type string of the default; `"unknown"` when absent.
    pub default_value_type: String,
}

/// One implementation (arity overload) of a macro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroImplementationDef {
    /// SQL dialect of the body (DuckLake writes `"duckdb"`).
    pub dialect: String,
    /// The macro body: an expression (scalar) or a SELECT (table).
    pub sql: String,
    /// `"scalar"` or `"table"`; every implementation of one macro must
    /// carry the same value.
    pub macro_type: String,
    /// Parameters in positional order.
    pub parameters: Vec<MacroParameterDef>,
}

/// A macro with its implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroInfo {
    /// The macro's id.
    pub id: MacroId,
    /// The schema the macro belongs to.
    pub schema_id: SchemaId,
    /// The macro's name, unique among the schema's live macros (macros
    /// have their own namespace, separate from tables and views).
    pub name: String,
    /// Implementations in `impl_id` order.
    pub implementations: Vec<MacroImplementationDef>,
}

/// One `ducklake_name_mapping` row: how one physical column of an
/// externally written file resolves to a table field. `column_id` is a
/// 0-based ordinal local to the mapping; `parent_column`, when present,
/// references a smaller ordinal (parents precede children in preorder).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameMappingDef {
    /// The row's ordinal within its mapping.
    pub column_id: u64,
    /// The physical column name in the file (or hive path).
    pub source_name: String,
    /// The table field the column resolves to.
    pub target_field_id: u64,
    /// The parent row's ordinal for nested columns; `None` for roots.
    pub parent_column: Option<u64>,
    /// Whether the value comes from the file's hive path, not its body.
    pub is_partition: bool,
}

/// A column mapping for externally written Parquet: immutable once
/// created, referenced by data files via their `mapping_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingInfo {
    /// The mapping's id.
    pub id: MappingId,
    /// The table the mapping belongs to.
    pub table_id: TableId,
    /// The mapping strategy, stored verbatim (DuckLake writes
    /// `"map_by_name"`).
    pub map_type: String,
    /// The mapping's rows in `column_id` order.
    pub name_mappings: Vec<NameMappingDef>,
}

/// One writer-supplied index entry for a row of a registered data file.
/// The ordinal is the row's 0-based position in the file; the commit maps
/// it to a row id (`row_id_start + ordinal`).
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexEntry {
    /// The index this entry belongs to.
    pub index: IndexId,
    /// The row's 0-based position within the file.
    pub ordinal: u64,
    /// The indexed column values, positionally matching the index's
    /// columns; a `None` is SQL NULL (stored as a collision-exempt
    /// multi-shaped entry so `IS NULL` finds the row).
    pub values: Vec<Option<crate::store::index_encoding::IndexKeyValue>>,
}

/// One writer-supplied entry removal for a registered delete file: the
/// killed row's id and the values it was indexed under. Against a
/// dense-range target the id must lie inside the target's row-id range;
/// against a per-row-id target it is the file's embedded id, supplied
/// verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexRemoval {
    /// The index the removal belongs to.
    pub index: IndexId,
    /// The killed row's id.
    pub row_id: u64,
    /// The indexed column values the dead row held, positionally
    /// matching the index's columns; a `None` is SQL NULL (the row's
    /// multi-shaped NULL entry is removed).
    pub values: Vec<Option<crate::store::index_encoding::IndexKeyValue>>,
}

/// The build lifecycle of an equality index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Fully built: serving lookups and enforcing uniqueness.
    Ready,
    /// A staged backfill is in progress; lookups fail typed and a unique
    /// violation poisons the build rather than failing the writer.
    Building,
    /// SQL additions committed ahead of bounded non-unique index repair;
    /// lookups refuse until the repair flips the index ready.
    Maintaining,
    /// A duplicate was discovered during a staged build; the definition is
    /// terminally poisoned and will be dropped by its driver.
    Poisoned,
}

/// When SQL writes add entries to an index.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexMaintenance {
    /// Entries land atomically with the data snapshot.
    #[default]
    Synchronous,
    /// Non-unique additions land in bounded commits after the data snapshot.
    Deferred,
}

/// The sort order of one indexed column: its direction and NULL placement.
/// Defaults to ascending, NULLS LAST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnOrder {
    /// Ascending or descending.
    pub direction: crate::store::index_encoding::Direction,
    /// NULL placement relative to non-null values.
    pub nulls: crate::store::index_encoding::NullOrder,
}

impl Default for ColumnOrder {
    fn default() -> Self {
        Self {
            direction: crate::store::index_encoding::Direction::Ascending,
            nulls: crate::store::index_encoding::NullOrder::Last,
        }
    }
}

/// A live equality index, as read from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    /// The index's id.
    pub id: IndexId,
    /// The table the index covers.
    pub table_id: TableId,
    /// The index name, unique among the table's live indexes.
    pub name: String,
    /// Indexed columns by field id, in declared order.
    pub columns: Vec<ColumnId>,
    /// Per-column sort direction, parallel to `columns`.
    pub directions: Vec<crate::store::index_encoding::Direction>,
    /// Per-column NULL placement, parallel to `columns`.
    pub nulls: Vec<crate::store::index_encoding::NullOrder>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Whether SQL additions are maintained synchronously or after commit.
    pub maintenance: IndexMaintenance,
    /// The build lifecycle state.
    pub state: IndexState,
    /// A staged build's watermark: the highest row id covered so far, or
    /// `None` before its first step. Always `None` on a single-commit index.
    pub build_cursor: Option<u64>,
    /// The data file currently covered through [`Self::build_position_cursor`].
    /// `None` while the build is still covering inline rows or was written by
    /// a binary that used only the row-id cursor.
    pub build_file_cursor: Option<DataFileId>,
    /// The physical row position durably covered in
    /// [`Self::build_file_cursor`].
    pub build_position_cursor: Option<u64>,
}

/// The definition of an equality index to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// The index name, unique among the table's live indexes.
    pub name: String,
    /// Indexed columns by field id, in declared order.
    pub columns: Vec<ColumnId>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
}

/// One writer-supplied index entry: a row and its indexed column values,
/// in the index's column order. A `None` in any position is SQL NULL; a
/// row with any NULL indexed value is stored as a collision-exempt
/// multi-shaped entry, so `IS NULL` finds it and a unique index still admits
/// unlimited NULL rows.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// The row this entry points at.
    pub row_id: u64,
    /// The indexed column values, positionally matching the index's
    /// columns.
    pub values: Vec<Option<crate::store::index_encoding::IndexKeyValue>>,
}

impl IndexEntry {
    /// What this entry will weigh on a batch, key and value together,
    /// before escaping; escaping can make the staged entry up to twice
    /// this, never smaller.
    pub(crate) fn nominal_bytes(&self) -> u64 {
        crate::store::key::index_entry_bytes(&self.values)
    }
}

/// How much one step of a staged index build may commit. A step ends at
/// whichever bound it reaches first, and always carries at least one
/// entry. `entries` bounds memory; `bytes` bounds the single object-store
/// request a batch becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildStep {
    /// Entries per step.
    pub entries: usize,
    /// Staged key and value bytes per step, summed over each entry's key
    /// and value before either is encoded. Framing escapes `0x00` and
    /// `0x01`, so the committed batch can be up to twice this.
    pub bytes: u64,
}

/// Entries per step when [`BuildStep`] is left to its default.
const DEFAULT_STEP_ENTRIES: usize = 1_000_000;

/// Staged bytes per step when [`BuildStep`] is left to its default. Eight
/// mebibytes clears `object_store`'s 30-second default request timeout at
/// a little over 270 KiB/s, so a step lands on links far slower than a
/// build has any right to expect.
const DEFAULT_STEP_BYTES: u64 = 8 * 1024 * 1024;

impl Default for BuildStep {
    fn default() -> Self {
        Self {
            entries: DEFAULT_STEP_ENTRIES,
            bytes: DEFAULT_STEP_BYTES,
        }
    }
}

/// A column definition: the input to table creation and column addition.
///
/// A nested type is a tree, not a type string to parse: the parent carries
/// DuckLake's marker (`"STRUCT"`, `"LIST"`, `"MAP"`) as its
/// [`column_type`](Self::column_type) and its fields as
/// [`children`](Self::children).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnDef {
    /// Column name, unique among its **siblings**: DuckLake scopes a nested
    /// field's name to its parent, so two structs may each hold an `x`.
    pub name: String,
    /// Column type, as a DuckLake type string (e.g. `"BIGINT"`), or a
    /// nested-type marker when [`children`](Self::children) is non-empty.
    pub column_type: String,
    /// Whether NULL values are allowed.
    pub nulls_allowed: bool,
    /// Default value expression, if any.
    pub default_value: Option<String>,
    /// The type's fields, in declaration order; empty for a scalar. A
    /// `LIST`'s single child is conventionally named `element`, as DuckLake
    /// names it.
    pub children: Vec<ColumnDef>,
}

/// A live column: its definition plus identity and position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// The column's field id.
    pub id: ColumnId,
    /// Column name.
    pub name: String,
    /// Column type, as a DuckLake type string.
    pub column_type: String,
    /// Whether NULL values are allowed.
    pub nulls_allowed: bool,
    /// Default value expression, if any.
    pub default_value: Option<String>,
    /// Ordinal position in the table (0-based).
    pub position: u64,
    /// The parent column's field id for a nested child column (a `STRUCT`
    /// field, `LIST` element, or `MAP` key/value), or `None` for a
    /// top-level column.
    pub parent_column: Option<ColumnId>,
}

/// A change to one column. `None` fields leave the attribute untouched;
/// `default_value` uses a nested `Option` so "clear the default"
/// (`Some(None)`) is distinct from "leave it" (`None`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnAlteration {
    /// New column type, if changing.
    pub column_type: Option<String>,
    /// New nullability, if changing.
    pub nulls_allowed: Option<bool>,
    /// New default value: `Some(Some(expr))` sets, `Some(None)` clears.
    pub default_value: Option<Option<String>>,
}

/// Identity and metadata of a committed snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// The snapshot's id.
    pub id: SnapshotId,
    /// When the snapshot was committed.
    pub time: Timestamp,
    /// Catalog schema version: advances only when a commit changes the
    /// catalog's shape, so clients can key schema caches on it.
    pub schema_version: u64,
}

/// One tag on a catalog object: a key/value row, begin/end-versioned
/// like any temporal row. Ended entries stay readable for time travel
/// until garbage-collected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagEntry {
    /// Snapshot at which this tag value became visible.
    pub begin_snapshot: u64,
    /// Snapshot at which it was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// Tag key (e.g. `comment`).
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// One `ducklake_files_scheduled_for_deletion` row: a path awaiting
/// physical deletion, decoupled from the expiry that scheduled it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledDeletion {
    /// The data or delete file id the path belonged to (the schedule's
    /// row identity).
    pub data_file_id: u64,
    /// Object-store path, relative iff `path_is_relative`.
    pub path: String,
    /// Whether `path` is relative to the table's data prefix.
    pub path_is_relative: bool,
    /// When the file was scheduled.
    pub schedule_start: Timestamp,
}

/// A chunk of rows to inline: one Arrow IPC record-batch body over the
/// table's user columns, decoded against the schema-only stream recorded
/// for its `schema_version`. Row ids are allocated by the commit from the
/// table's row-id counter, never caller-provided — the same allocation a
/// data-file registration performs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineChunk {
    /// The catalog schema version the body's columns match.
    pub schema_version: u64,
    /// The Arrow IPC schema-only stream for `schema_version`. Written once
    /// per version; a chunk for a version already recorded may repeat it
    /// (the record is overwritten with identical bytes).
    pub arrow_schema: Vec<u8>,
    /// The Arrow IPC record-batch body: the batch message and buffers, with
    /// no schema message.
    pub arrow_body: Vec<u8>,
    /// How many rows the body carries.
    pub row_count: u64,
}

/// A data file a flush wrote, carrying rows that were inlined before it.
/// Unlike an ordinary registration, the rows keep the ids they were
/// inlined under and the record is backdated, so a pre-flush time-travel
/// read finds them in the file rather than in the drained chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushedDataFile {
    /// The written file. Registering it adds only its bytes to the table's
    /// statistics; its rows were counted when inlined.
    pub file: DataFile,
    /// The first row id the file carries — preserved from the inlined
    /// rows, never reallocated.
    pub row_id_start: u64,
    /// The snapshot the file record is backdated to: the earliest
    /// `begin_snapshot` among the rows it carries.
    pub begin_snapshot: SnapshotId,
    /// The latest `begin_snapshot` among those rows, when the file
    /// collects rows from more than one snapshot. A reader needs both
    /// bounds: the record is live from the earliest, and rows newer than
    /// the snapshot being read must be filtered out per row against the
    /// file's own snapshot column. `None` when every row shares the
    /// backdated snapshot.
    pub partial_max: Option<SnapshotId>,
}

/// One inlined row, with the Arrow IPC bytes a caller needs to decode it.
/// Rows of one chunk share one `chunk_body`, and rows of one schema
/// version share one `arrow_schema`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentRow {
    /// The row's dense id.
    pub row_id: u64,
    /// The commit that inserted it.
    pub begin_snapshot: SnapshotId,
    /// The schema version its body decodes against.
    pub schema_version: u64,
    /// The row's offset within its chunk.
    pub offset_in_chunk: u64,
    /// The owning chunk's Arrow IPC record-batch body.
    pub chunk_body: std::sync::Arc<Vec<u8>>,
    /// The Arrow IPC schema-only stream of `schema_version`.
    pub arrow_schema: std::sync::Arc<Vec<u8>>,
}

/// An option scope: global, or one schema or table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionScope {
    /// Catalog-wide.
    Global,
    /// One schema.
    Schema(SchemaId),
    /// One table.
    Table(TableId),
}

impl OptionScope {
    /// Returns scope key components as (`scope_type`, `id`).
    pub(crate) fn key_components(self) -> (u64, u64) {
        match self {
            Self::Global => (0, 0),
            Self::Schema(id) => (1, id.get()),
            Self::Table(id) => (2, id.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// The micros conversion is total in both directions, pre-epoch
        /// included.
        #[test]
        fn timestamps_round_trip_through_their_micros(micros in any::<i64>()) {
            prop_assert_eq!(Timestamp::from_micros(micros).as_micros(), micros);
        }

        /// Timestamps order by their micros.
        #[test]
        fn timestamps_order_by_their_micros(left in any::<i64>(), right in any::<i64>()) {
            prop_assert_eq!(
                Timestamp::from_micros(left).cmp(&Timestamp::from_micros(right)),
                left.cmp(&right)
            );
        }
    }

    #[test]
    fn timestamp_now_is_after_the_epoch() {
        assert!(Timestamp::now() > Timestamp::UNIX_EPOCH);
    }

    #[test]
    fn ids_round_trip_and_display() {
        let id = TableId::new(7);
        assert_eq!(id.get(), 7);
        assert_eq!(id.to_string(), "7");
        assert_eq!(SnapshotId::new(0).get(), 0);
        assert_eq!(ColumnId::new(3).get(), 3);
        assert_eq!(SchemaId::new(4).to_string(), "4");
        assert_eq!(DataFileId::new(9).get(), 9);
        assert_eq!(DeleteFileId::new(8).to_string(), "8");
        assert_eq!(ViewId::new(5).get(), 5);
    }
}
