//! Bounded schema-version projection, including legacy rows and file floors.

use std::{ops::Bound, sync::Arc};

use crate::{
    Error, MemoryReservation, Result,
    ffi_support::SchemaVersionRow,
    store::{
        handle::OwnedReadHandle,
        read::{
            EntityRecord, EntityRecordKind, OwnedEntityCursor, OwnedSnapshotCursor, Versions,
            schema_versions::{self, SchemaVersionCursor},
        },
    },
};

enum Phase {
    Stored,
    Authored,
    Legacy,
    Floors,
    Done,
}

struct LegacyRows {
    snapshot: u64,
    version: u64,
    tables: std::vec::IntoIter<u64>,
    _memory: MemoryReservation,
}

pub(super) struct Cursor {
    pub(super) overlay: Option<Arc<crate::transaction::staged::paging::Overlay>>,
    authored_after: Option<(u64, u64)>,
    handle: OwnedReadHandle,
    stored: SchemaVersionCursor,
    snapshots: Option<OwnedSnapshotCursor>,
    legacy: Option<LegacyRows>,
    files: Option<FileFloors>,
    phase: Phase,
    missing_legacy: bool,
    current_version: u64,
}

impl Cursor {
    pub(super) async fn open(handle: OwnedReadHandle) -> Result<Self> {
        let stored = SchemaVersionCursor::open(handle.clone()).await?;
        Ok(Self {
            overlay: None,
            authored_after: None,
            handle,
            stored,
            snapshots: None,
            legacy: None,
            files: None,
            phase: Phase::Stored,
            missing_legacy: false,
            current_version: 0,
        })
    }

    pub(super) async fn next(&mut self) -> Result<Option<(SchemaVersionRow, MemoryReservation)>> {
        loop {
            match self.phase {
                Phase::Stored => {
                    if let Some((table_id, begin_snapshot, schema_version, memory)) =
                        self.stored.next().await?
                    {
                        if self
                            .overlay
                            .as_ref()
                            .and_then(|overlay| overlay.schema_changes())
                            .is_some_and(|changes| {
                                changes.records.contains_key(&(table_id, begin_snapshot))
                            })
                        {
                            continue;
                        }
                        return Ok(Some((
                            SchemaVersionRow {
                                begin_snapshot,
                                schema_version,
                                table_id,
                            },
                            memory,
                        )));
                    }
                    self.phase = Phase::Authored;
                }
                Phase::Authored => {
                    if let Some(changes) = self
                        .overlay
                        .as_ref()
                        .and_then(|overlay| overlay.schema_changes())
                    {
                        let lower = self
                            .authored_after
                            .map_or(Bound::Unbounded, Bound::Excluded);
                        for (&(table_id, begin_snapshot), &version) in
                            changes.records.range((lower, Bound::Unbounded))
                        {
                            self.authored_after = Some((table_id, begin_snapshot));
                            if let Some(schema_version) = version {
                                return Self::admit(SchemaVersionRow {
                                    begin_snapshot,
                                    schema_version,
                                    table_id,
                                });
                            }
                        }
                    }
                    self.snapshots = Some(OwnedSnapshotCursor::open(self.handle.clone()).await?);
                    self.phase = Phase::Legacy;
                }
                Phase::Legacy => {
                    if let Some(row) = self.next_legacy().await? {
                        return Self::admit(row);
                    }
                    self.snapshots = None;
                    self.files =
                        Some(FileFloors::open(self.handle.clone(), self.overlay.clone()).await?);
                    self.phase = Phase::Floors;
                }
                Phase::Floors => {
                    let files = self.files.as_mut().ok_or_else(|| {
                        Error::Corruption("schema-version floor cursor is missing".into())
                    })?;
                    let Some((table_id, begin_snapshot)) = files.next().await? else {
                        self.files = None;
                        self.phase = Phase::Done;
                        continue;
                    };
                    if let Some(row) = self.floor(table_id, begin_snapshot).await? {
                        return Self::admit(row);
                    }
                }
                Phase::Done => return Ok(None),
            }
        }
    }

    fn admit(row: SchemaVersionRow) -> Result<Option<(SchemaVersionRow, MemoryReservation)>> {
        Ok(Some((
            row,
            MemoryReservation::reserve(size_of::<SchemaVersionRow>() as u64)?,
        )))
    }

    async fn next_legacy(&mut self) -> Result<Option<SchemaVersionRow>> {
        loop {
            if let Some(legacy) = &mut self.legacy {
                for table_id in legacy.tables.by_ref() {
                    let authored = self
                        .overlay
                        .as_ref()
                        .and_then(|overlay| overlay.schema_changes())
                        .and_then(|changes| changes.records.get(&(table_id, legacy.snapshot)));
                    let exists = match authored {
                        Some(value) => value.is_some(),
                        None => {
                            schema_versions::exists(self.handle.borrow(), table_id, legacy.snapshot)
                                .await?
                        }
                    };
                    if !exists {
                        self.missing_legacy = true;
                        return Ok(Some(SchemaVersionRow {
                            table_id,
                            begin_snapshot: legacy.snapshot,
                            schema_version: legacy.version,
                        }));
                    }
                }
            }
            self.legacy = None;
            let snapshots = self.snapshots.as_mut().ok_or_else(|| {
                Error::Corruption("schema-version snapshot cursor is missing".into())
            })?;
            let Some((mut snapshot, memory)) = snapshots.next().await? else {
                return Ok(None);
            };
            if self
                .overlay
                .as_ref()
                .is_some_and(|overlay| !overlay.includes_snapshot(snapshot.snapshot_id))
            {
                continue;
            }
            self.current_version = snapshot.schema_version;
            snapshot.schema_changed_table_ids.sort_unstable();
            snapshot.schema_changed_table_ids.dedup();
            self.legacy = Some(LegacyRows {
                snapshot: snapshot.snapshot_id,
                version: snapshot.schema_version,
                tables: snapshot.schema_changed_table_ids.into_iter(),
                _memory: memory,
            });
        }
    }

    async fn first_stored(&self, table_id: u64) -> Result<Option<(u64, u64)>> {
        let Some(changes) = self
            .overlay
            .as_ref()
            .and_then(|overlay| overlay.schema_changes())
        else {
            return schema_versions::first(self.handle.borrow(), table_id).await;
        };
        let mut first = changes
            .records
            .range((table_id, 0)..=(table_id, u64::MAX))
            .find_map(|(&(_, begin), &version)| version.map(|version| (begin, version)));
        let mut cursor = SchemaVersionCursor::open_table(self.handle.clone(), table_id).await?;
        while let Some((table, begin, version, _memory)) = cursor.next().await? {
            if !changes.records.contains_key(&(table, begin)) {
                if first.is_none_or(|(authored, _)| begin < authored) {
                    first = Some((begin, version));
                }
                break;
            }
        }
        Ok(first)
    }

    async fn floor(&self, table_id: u64, begin_snapshot: u64) -> Result<Option<SchemaVersionRow>> {
        let mut first = self.first_stored(table_id).await?;
        if first.is_some_and(|(begin, _)| begin <= begin_snapshot) {
            return Ok(None);
        }
        // Legacy catalogs can carry a schema row only inside a snapshot.
        if self.missing_legacy {
            let mut snapshots = OwnedSnapshotCursor::open(self.handle.clone()).await?;
            while let Some((snapshot, _memory)) = snapshots.next().await? {
                if self
                    .overlay
                    .as_ref()
                    .is_some_and(|overlay| !overlay.includes_snapshot(snapshot.snapshot_id))
                {
                    continue;
                }
                if snapshot.schema_changed_table_ids.contains(&table_id) {
                    if first.is_none_or(|(begin, _)| snapshot.snapshot_id < begin) {
                        first = Some((snapshot.snapshot_id, snapshot.schema_version));
                    }
                    break;
                }
            }
        }
        if first.is_some_and(|(begin, _)| begin <= begin_snapshot) {
            return Ok(None);
        }
        Ok(Some(SchemaVersionRow {
            table_id,
            begin_snapshot,
            schema_version: first.map_or(self.current_version, |(_, version)| version),
        }))
    }
}

struct FileFloors {
    overlay: Option<Arc<crate::transaction::staged::paging::Overlay>>,
    authored: std::iter::Peekable<std::collections::btree_map::IntoIter<u64, u64>>,
    cursors: [OwnedEntityCursor; 2],
    pending: [Option<(u64, u64)>; 2],
    ended: [bool; 2],
}

impl FileFloors {
    async fn open(
        handle: OwnedReadHandle,
        overlay: Option<Arc<crate::transaction::staged::paging::Overlay>>,
    ) -> Result<Self> {
        let authored = overlay
            .as_ref()
            .and_then(|overlay| overlay.schema_changes())
            .map(crate::transaction::staged::paging::schema_versions::Changes::file_floors)
            .unwrap_or_default()
            .into_iter()
            .peekable();
        Ok(Self {
            overlay,
            authored,
            cursors: [
                OwnedEntityCursor::open(
                    handle.clone(),
                    EntityRecordKind::File,
                    Versions::Live,
                    None,
                )
                .await?,
                OwnedEntityCursor::open_ended(handle, EntityRecordKind::File).await?,
            ],
            pending: [None, None],
            ended: [false, false],
        })
    }

    async fn fill(&mut self, index: usize) -> Result<()> {
        if self.pending[index].is_some() || self.ended[index] {
            return Ok(());
        }
        while let Some((record, _memory)) = self.cursors[index].next().await? {
            let record = match &self.overlay {
                Some(overlay) => overlay.apply(record)?,
                None => Some(record),
            };
            let Some(record) = record else {
                continue;
            };
            let EntityRecord::File(file) = record else {
                return Err(Error::Corruption(
                    "file floor cursor returned a different entity".into(),
                ));
            };
            self.pending[index] = Some((file.table_id, file.begin_snapshot));
            return Ok(());
        }
        self.ended[index] = true;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<(u64, u64)>> {
        self.fill(0).await?;
        self.fill(1).await?;
        let Some(table_id) = self
            .pending
            .iter()
            .flatten()
            .map(|(table, _)| *table)
            .chain(self.authored.peek().map(|(table, _)| *table))
            .min()
        else {
            return Ok(None);
        };
        let mut oldest = u64::MAX;
        if self
            .authored
            .peek()
            .is_some_and(|(table, _)| *table == table_id)
            && let Some((_, begin)) = self.authored.next()
        {
            oldest = begin;
        }
        for index in 0..2 {
            while let Some((table, begin)) = self.pending[index] {
                if table != table_id {
                    break;
                }
                oldest = oldest.min(begin);
                self.pending[index] = None;
                self.fill(index).await?;
            }
        }
        Ok(Some((table_id, oldest)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        Catalog, CatalogOptions,
        ffi_support::{self, metadata_cursor::MetadataCursor, staged::TableKind},
        store::{
            key::{EntityKey, Key},
            proto::{DataFileValue, SchemaVersionValue, SnapshotValue},
            value::encode_value,
        },
    };

    #[tokio::test]
    async fn stored_pages_do_not_materialize_files_or_later_rows() {
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        for table_id in 0..130 {
            write
                .put(
                    Key::SchemaVersion {
                        table_id,
                        begin_snapshot: 1,
                    }
                    .encode(),
                    encode_value(&SchemaVersionValue { schema_version: 1 }),
                )
                .unwrap();
        }
        write
            .put(
                Key::current(EntityKey::File {
                    table_id: 1,
                    data_file_id: 1,
                })
                .encode(),
                [0xff],
            )
            .unwrap();
        catalog.commit_write_tx(write).await.unwrap();
        let mut cursor = MetadataCursor::open(&catalog, TableKind::SchemaVersions, None, None)
            .await
            .unwrap();
        assert_eq!(cursor.schema_versions().await.unwrap().records.len(), 64);
        assert_eq!(cursor.schema_versions().await.unwrap().records.len(), 64);
        assert!(matches!(
            cursor.schema_versions().await,
            Err(crate::Error::Corruption(_))
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn pages_merge_stored_legacy_and_file_floor_rows() {
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        for snapshot_id in 1..=70 {
            let ids = match snapshot_id {
                2 => vec![2],
                5 => vec![1, 1, 4],
                _ => Vec::new(),
            };
            write
                .put(
                    Key::Snapshot { snapshot_id }.encode(),
                    encode_value(&SnapshotValue {
                        snapshot_id,
                        schema_version: snapshot_id,
                        schema_changed_table_ids: ids,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        for table_id in std::iter::once(1).chain(100..300) {
            write
                .put(
                    Key::SchemaVersion {
                        table_id,
                        begin_snapshot: 5,
                    }
                    .encode(),
                    encode_value(&SchemaVersionValue { schema_version: 99 }),
                )
                .unwrap();
        }
        for (table_id, begin_snapshot, end_snapshot) in
            [(1, 1, None), (1, 0, Some(1)), (2, 1, None), (3, 5, None)]
        {
            let entity = EntityKey::File {
                table_id,
                data_file_id: begin_snapshot,
            };
            let key =
                end_snapshot.map_or_else(|| Key::current(entity), |end| Key::history(entity, end));
            write
                .put(
                    key.encode(),
                    encode_value(&DataFileValue {
                        table_id,
                        data_file_id: begin_snapshot,
                        begin_snapshot,
                        end_snapshot,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        catalog.commit_write_tx(write).await.unwrap();
        let expected = ffi_support::dump_schema_versions(&catalog).await.unwrap();
        let mut cursor = MetadataCursor::open(&catalog, TableKind::SchemaVersions, None, None)
            .await
            .unwrap();
        let mut actual = Vec::new();
        loop {
            let page = cursor.schema_versions().await.unwrap();
            assert!(page.records.len() <= 64);
            if page.records.is_empty() {
                break;
            }
            actual.extend(page.records);
        }
        actual.sort_by_key(|row| (row.table_id, row.begin_snapshot));
        assert_eq!(actual, expected);
        assert_eq!(
            actual
                .iter()
                .filter(|row| row.table_id == 1)
                .map(|row| (row.begin_snapshot, row.schema_version))
                .collect::<Vec<_>>(),
            [(0, 99), (5, 99)]
        );
        catalog.close().await.unwrap();
    }
}
