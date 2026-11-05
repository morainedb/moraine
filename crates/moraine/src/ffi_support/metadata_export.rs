//! Admission for selected descriptor copies and their conversion scratch.

use crate::{
    CatalogSnapshot, ColumnInfo, MemoryReservation, Result, SchemaInfo, TableInfo, ViewInfo,
};

/// The descriptors selected by one snapshot export.
#[derive(Clone, Copy)]
pub enum MetadataExport {
    /// All live schemas.
    Schemas,
    /// Live tables belonging to a schema.
    TablesIn(u64),
    /// Live columns belonging to a table.
    ColumnsOf(u64),
    /// Live views belonging to a schema.
    ViewsIn(u64),
}

impl MetadataExport {
    /// Reserves selected Rust descriptors, copied strings, sorting scratch,
    /// and caller-supplied output and conversion row sizes before allocation.
    /// The caller retains this reservation until its exported array is freed.
    ///
    /// # Errors
    /// Returns an error when the working-memory allowance is exhausted.
    pub fn reserve_export(
        self,
        snapshot: &CatalogSnapshot,
        descriptor_bytes: usize,
        conversion_bytes: usize,
    ) -> Result<MemoryReservation> {
        let mut count = 0_u64;
        let mut strings = 0_u64;
        let mut include = |values: &[&str]| {
            count = count.saturating_add(1);
            for value in values {
                // A Rust string and its NUL-terminated replacement can coexist.
                strings = strings
                    .saturating_add((value.len() as u64).saturating_add(1).saturating_mul(2));
            }
        };
        let (info_bytes, sort_bytes) = match self {
            Self::Schemas => {
                for schema in snapshot.schemas.values() {
                    include(&[&schema.schema_name]);
                }
                (size_of::<SchemaInfo>(), 0)
            }
            Self::TablesIn(schema) => {
                for table in snapshot
                    .tables
                    .values()
                    .filter(|table| table.schema_id == schema)
                {
                    include(&[&table.table_name]);
                }
                (size_of::<TableInfo>(), 0)
            }
            Self::ColumnsOf(table) => {
                if let Some(columns) = snapshot.columns.get(&table) {
                    for column in columns.values() {
                        include(&[
                            &column.column_name,
                            &column.column_type,
                            column.default_value.as_deref().unwrap_or_default(),
                        ]);
                    }
                }
                (size_of::<ColumnInfo>(), size_of::<ColumnInfo>())
            }
            Self::ViewsIn(schema) => {
                for view in snapshot
                    .views
                    .values()
                    .filter(|view| view.schema_id == schema)
                {
                    include(&[&view.view_name, &view.dialect, &view.sql]);
                }
                (size_of::<ViewInfo>(), 0)
            }
        };
        if count == 0 {
            return MemoryReservation::reserve(0);
        }
        // Filtered and fallible collects may grow geometrically from four slots.
        // Include a second output buffer for shrinking the array into a box.
        let row_bytes = (info_bytes as u64)
            .saturating_add(conversion_bytes as u64)
            .saturating_add((descriptor_bytes as u64).saturating_mul(2));
        let bytes = count
            .max(4)
            .saturating_mul(2)
            .saturating_mul(row_bytes)
            .saturating_add(count.saturating_mul(sort_bytes as u64))
            .saturating_add(strings);
        MemoryReservation::reserve(bytes)
    }
}
