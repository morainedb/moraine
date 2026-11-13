//! Bounded file descriptors at one row-lookup read point.

use super::{CatalogSnapshot, DataStore, Placements, ReadOnlyCatalog, Result, TableId};
use crate::{
    MemoryReservation,
    store::{
        handle::ReadHandle,
        key::{EntityKey, Key},
        read::{self, EntityCursor, EntityRecordKind, Versions},
    },
};

const PAGE_FILES: usize = 128;
const PAGE_BYTES: u64 = 1024 * 1024;

impl ReadOnlyCatalog {
    pub(in crate::catalog::handle) async fn locate_file_pages(
        &self,
        handle: ReadHandle<'_>,
        store: &DataStore,
        data_prefix: &str,
        snapshot: &CatalogSnapshot,
        table: TableId,
        row_ids: &[u64],
    ) -> Result<Placements> {
        let mut cursor = EntityCursor::open(
            handle,
            EntityRecordKind::File,
            Versions::Live,
            Some(table.get()),
            crate::read_policy::CacheAdmission::Retain,
        )
        .await?;
        let mut placements = Placements::new();
        loop {
            let mut page = snapshot.clone();
            let mut memory = Vec::new();
            let mut bytes = 0_u64;
            for _ in 0..PAGE_FILES {
                let Some((record, reservation)) = cursor
                    .next()
                    .await?
                    .map(crate::memory::Reserved::into_parts)
                else {
                    break;
                };
                let read::EntityRecord::File(file) = &record else {
                    continue;
                };
                let stats_key = Key::current(EntityKey::FileColumnStats {
                    table_id: table.get(),
                    data_file_id: file.data_file_id,
                    column_id: crate::data_file::ROW_ID_FIELD_ID,
                });
                if let Some(stats) = handle.get(stats_key.encode()).await? {
                    let allowance = MemoryReservation::reserve(
                        (stats.len() as u64).saturating_mul(4).saturating_add(256),
                    )?;
                    bytes = bytes.saturating_add(allowance.bytes());
                    memory.push(allowance);
                    page.put_record(read::decode_current_record(stats_key, &stats)?);
                }
                bytes = bytes.saturating_add(reservation.bytes());
                memory.push(reservation);
                page.put_record(record);
                if bytes >= PAGE_BYTES {
                    break;
                }
            }
            if page.data_files.is_empty() {
                break;
            }
            for (row, files) in self
                .locate_files(store, data_prefix, &page, table, row_ids)
                .await?
            {
                placements.entry(row).or_default().extend(files);
            }
        }
        Ok(placements)
    }
}
