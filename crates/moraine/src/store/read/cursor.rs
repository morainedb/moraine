//! Incremental entity reads at a caller-owned read point.

use super::{
    EntityRecord, EntityRecordKind, Versions, decode_current_record, decode_history_record,
};
use crate::{
    Error, MemoryReservation, Result,
    store::{
        handle::{OwnedReadHandle, ReadHandle, ScanIterator, ScanOrder, ScanShape},
        key::{
            EntityKey, Key, current_entity_kind_prefix, current_gc_file_prefix,
            current_table_prefix, history_entity_kind_prefix, history_entity_prefix,
            history_table_prefix,
        },
    },
};

/// One kind's current records followed by its ended records, in key order.
pub(crate) struct EntityCursor<'a> {
    handle: ReadHandle<'a>,
    state: CursorState,
}

struct CursorState {
    iterator: ScanIterator,
    history: Option<Vec<u8>>,
    historical: bool,
    shape: ScanShape,
    visited: usize,
    latest_only: bool,
    previous_history_entity: Option<EntityKey>,
}

impl<'a> EntityCursor<'a> {
    pub(crate) async fn open(
        handle: ReadHandle<'a>,
        kind: EntityRecordKind,
        versions: Versions,
        table: Option<u64>,
    ) -> Result<Self> {
        Ok(Self {
            handle,
            state: CursorState::open(handle, kind, versions, table).await?,
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        self.state.next(self.handle).await
    }
}

/// A cursor that owns the read point and its checkpoint lease.
pub(crate) struct OwnedEntityCursor {
    handle: OwnedReadHandle,
    state: CursorState,
}

impl OwnedEntityCursor {
    pub(crate) async fn open(
        handle: OwnedReadHandle,
        kind: EntityRecordKind,
        versions: Versions,
        table: Option<u64>,
    ) -> Result<Self> {
        let state = CursorState::open(handle.borrow(), kind, versions, table).await?;
        Ok(Self { handle, state })
    }

    /// Emits current parents and only the newest ended version of absent
    /// parents.
    pub(crate) async fn open_latest(
        handle: OwnedReadHandle,
        kind: EntityRecordKind,
    ) -> Result<Self> {
        let mut cursor = Self::open(handle, kind, Versions::LiveAndEnded, None).await?;
        cursor.state.latest_only = true;
        Ok(cursor)
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        self.state.next(self.handle.borrow()).await
    }
}

/// An append-only snapshot scan at one retained read point.
pub(crate) struct OwnedSnapshotCursor {
    handle: OwnedReadHandle,
    iterator: ScanIterator,
}

impl OwnedSnapshotCursor {
    pub(crate) async fn open(handle: OwnedReadHandle) -> Result<Self> {
        let iterator = handle
            .borrow()
            .scan_prefix(
                crate::store::key::subspace_prefix(crate::store::key::Subspace::Snapshot),
                ..,
                ScanShape::Streaming,
            )
            .await?;
        Ok(Self { handle, iterator })
    }

    pub(crate) async fn next(
        &mut self,
    ) -> Result<Option<(crate::store::proto::SnapshotValue, MemoryReservation)>> {
        self.handle.borrow().validate()?;
        let Some(entry) = self.iterator.next().await? else {
            return Ok(None);
        };
        self.handle.borrow().validate()?;
        let memory = MemoryReservation::reserve(
            (entry.key.len() as u64)
                .saturating_add(entry.value.len() as u64)
                .saturating_mul(2)
                .saturating_add(super::RECORD_BYTES),
        )?;
        if !matches!(Key::decode(&entry.key)?, Key::Snapshot { .. }) {
            return Err(Error::Corruption(
                "non-snapshot key in snapshot cursor".into(),
            ));
        }
        Ok(Some((
            crate::store::value::decode_value(&entry.value)?,
            memory,
        )))
    }
}

impl CursorState {
    async fn open(
        handle: ReadHandle<'_>,
        kind: EntityRecordKind,
        versions: Versions,
        table: Option<u64>,
    ) -> Result<Self> {
        let (current, history) = if let Some(table) = table {
            let scoped = kind.table_scoped().ok_or_else(|| {
                Error::Corruption(format!("{kind:?} does not support a table-scoped read"))
            })?;
            (
                current_table_prefix(scoped, table),
                Some(history_table_prefix(scoped, table)),
            )
        } else if let Some(entity) = kind.entity_kind() {
            (
                current_entity_kind_prefix(entity),
                Some(history_entity_kind_prefix(entity)),
            )
        } else {
            (current_gc_file_prefix(), None)
        };
        let history = history.filter(|_| versions == Versions::LiveAndEnded && kind.is_versioned());
        let shape = if table.is_some() {
            ScanShape::Metadata
        } else {
            ScanShape::Streaming
        };
        let iterator = handle.scan_prefix(current, .., shape).await?;
        Ok(Self {
            iterator,
            history,
            historical: false,
            shape,
            visited: 0,
            latest_only: false,
            previous_history_entity: None,
        })
    }

    /// Decodes one record, retaining its estimated working allowance.
    async fn next(
        &mut self,
        handle: ReadHandle<'_>,
    ) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        loop {
            handle.validate()?;
            if let Some(mut entry) = self.iterator.next().await? {
                handle.validate()?;
                self.visited = self.visited.wrapping_add(1);
                if self.visited.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
                if self.latest_only && self.historical {
                    let Key::History(history) = Key::decode(&entry.key)? else {
                        return Err(Error::Corruption(
                            "non-history key in latest-parent cursor".into(),
                        ));
                    };
                    if self.previous_history_entity == Some(history.entity) {
                        continue;
                    }
                    self.previous_history_entity = Some(history.entity);
                    if handle
                        .get(Key::current(history.entity).encode())
                        .await?
                        .is_some()
                    {
                        continue;
                    }
                    let mut latest = handle
                        .scan_prefix_ordered(
                            history_entity_prefix(history.entity),
                            ..,
                            ScanShape::Seek,
                            ScanOrder::Descending,
                        )
                        .await?;
                    entry = latest.next().await?.ok_or_else(|| {
                        Error::Corruption("history parent disappeared within a fixed read".into())
                    })?;
                    handle.validate()?;
                }
                let memory = MemoryReservation::reserve(
                    (entry.key.len() as u64)
                        .saturating_add(entry.value.len() as u64)
                        .saturating_mul(2)
                        .saturating_add(super::RECORD_BYTES),
                )?;
                let key = Key::decode(&entry.key)?;
                let record = if self.historical {
                    decode_history_record(key, &entry.value)?
                } else {
                    decode_current_record(key, &entry.value)?
                };
                return Ok(Some((record, memory)));
            }
            let Some(history) = self.history.take() else {
                return Ok(None);
            };
            self.iterator = handle.scan_prefix(history, .., self.shape).await?;
            self.historical = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::{
        IsolationLevel,
        config::{FlushOptions, FlushType},
    };

    use super::*;
    use crate::store::{
        key::EntityKey, open::StoreBuilder, proto::DataFileValue, value::encode_value,
    };

    #[tokio::test]
    async fn a_cursor_decodes_only_records_the_consumer_requests() {
        let (database, _) = StoreBuilder::new("incremental", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let transaction = database.begin(IsolationLevel::Snapshot).unwrap();
        transaction
            .put(
                Key::current(EntityKey::File {
                    table_id: 1,
                    data_file_id: 1,
                })
                .encode(),
                encode_value(&DataFileValue {
                    table_id: 1,
                    data_file_id: 1,
                    ..Default::default()
                }),
            )
            .unwrap();
        transaction
            .put(
                Key::current(EntityKey::File {
                    table_id: 1,
                    data_file_id: 2,
                })
                .encode(),
                [0xff],
            )
            .unwrap();
        let mut cursor = EntityCursor::open(
            ReadHandle::Tx(&transaction),
            EntityRecordKind::File,
            Versions::Live,
            Some(1),
        )
        .await
        .unwrap();
        let (record, memory) = cursor.next().await.unwrap().unwrap();
        assert!(matches!(record, EntityRecord::File(file) if file.data_file_id == 1));
        assert!(memory.bytes() > 0);
        assert!(matches!(cursor.next().await, Err(Error::Corruption(_))));
        drop(cursor);
        transaction.rollback();
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn repeated_table_scans_reuse_foyer_blocks() {
        let (database, counters) = StoreBuilder::new("cached-metadata", Arc::new(InMemory::new()))
            .cache_puts(false)
            .open_writer()
            .await
            .unwrap();
        for id in 0..256 {
            database
                .put(
                    Key::current(EntityKey::File {
                        table_id: 1,
                        data_file_id: id,
                    })
                    .encode(),
                    encode_value(&DataFileValue {
                        table_id: 1,
                        data_file_id: id,
                        path: format!("{id}.parquet"),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap();
        }
        database
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        for warm in [false, true] {
            let before = counters.tally();
            let transaction = database.begin(IsolationLevel::Snapshot).unwrap();
            let mut cursor = EntityCursor::open(
                ReadHandle::Tx(&transaction),
                EntityRecordKind::File,
                Versions::Live,
                Some(1),
            )
            .await
            .unwrap();
            let mut count = 0;
            while cursor.next().await.unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, 256);
            drop(cursor);
            transaction.rollback();
            let reads = counters.tally().since(before);
            if warm {
                assert!(reads.block_hits > 0, "{reads:?}");
                assert_eq!(reads.block_misses, 0, "{reads:?}");
            } else {
                assert!(reads.block_misses > 0, "{reads:?}");
            }
        }
        database.close().await.unwrap();
    }
}
