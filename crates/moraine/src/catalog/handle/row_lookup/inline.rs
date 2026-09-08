//! Requested inline IDs selected before chunk bodies or row objects are
//! materialized.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use super::{InlineDirectory, Intervals};
use crate::{
    catalog::{
        ReadOnlyCatalog, RecentRow, TableId, handle::inline_scan::InlineRowSource,
        inline::InlineRow, projection,
    },
    error::Result,
    store::{
        handle::{ReadHandle, ReadSession},
        inline::{self as store_inline, InlineChunkLocator, stream::InlineTombstones},
        key::InlineOperation,
        proto::{HeadValue, InlineChunkValue},
    },
    transaction::commit,
};

type ScannedChunks = Vec<(InlineOperation, InlineChunkValue)>;

impl ReadOnlyCatalog {
    async fn inline_lookup_directory(
        &self,
        session: &ReadSession,
        table: TableId,
        head: HeadValue,
        use_cache: bool,
    ) -> Result<(Arc<InlineDirectory>, Option<ScannedChunks>)> {
        let handle = session.handle();
        let cached = self
            .row_lookups
            .inline
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&table)
            .filter(|directory| use_cache && directory.head == head)
            .cloned();
        if let Some(directory) = cached {
            return Ok((directory, None));
        }
        let mut scanned = None;
        let locators =
            if use_cache && projection::inline_directory_complete(&self.projections, table.get()) {
                store_inline::scan_inline_chunk_locators(handle, table.get()).await?
            } else {
                let chunks = store_inline::scan_inline_chunks(handle, table.get()).await?;
                self.verify_inline_directory(handle, table, &chunks).await?;
                let locators = chunks
                    .iter()
                    .map(|(operation, chunk)| InlineChunkLocator::from_chunk(*operation, chunk))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect();
                if !handle.is_isolated() || !use_cache {
                    scanned = Some(chunks);
                }
                locators
            };
        let directory = Arc::new(InlineDirectory {
            head,
            ranges: Intervals::new(
                locators
                    .into_iter()
                    .map(|locator| (locator.row_id_start(), locator.row_id_end(), locator)),
            ),
        });
        Ok((directory, scanned))
    }

    async fn requested_inline_rows(
        &self,
        session: &ReadSession,
        table: TableId,
        requested: &[u64],
        head: HeadValue,
        use_cache: bool,
    ) -> Result<(InlineRowSource, Vec<InlineRow>, Arc<InlineDirectory>)> {
        let (directory, scanned) = self
            .inline_lookup_directory(session, table, head, use_cache)
            .await?;
        let mut selected = Vec::new();
        let mut locators = Vec::new();
        let mut chunks = BTreeMap::new();
        for row in requested.iter().copied().collect::<BTreeSet<_>>() {
            let mut matches = Vec::new();
            directory
                .ranges
                .visit(row, |locator| matches.push(*locator));
            if matches.is_empty() {
                continue;
            }
            let deletion =
                latest_deletion(session.handle(), table, row, directory.head.snapshot_id).await?;
            for locator in matches {
                let InlineOperation::Insert { begin_snapshot, .. } = locator.operation() else {
                    continue;
                };
                if begin_snapshot > directory.head.snapshot_id
                    || deletion.is_some_and(|end| begin_snapshot < end)
                {
                    continue;
                }
                let chunk = *chunks.entry(locator.operation()).or_insert_with(|| {
                    let index = locators.len();
                    locators.push(locator);
                    index
                });
                selected.push(InlineRow {
                    row_id: row,
                    begin_snapshot,
                    end_snapshot: None,
                    chunk,
                    offset_in_chunk: row - locator.row_id_start(),
                });
            }
        }
        selected.sort_unstable_by_key(|row| (row.row_id, row.begin_snapshot));
        let source = if let Some(scanned) = scanned {
            let mut referenced: Vec<_> = scanned
                .into_iter()
                .filter_map(|(operation, body)| {
                    chunks
                        .get(&operation)
                        .map(|index| (*index, (operation, body)))
                })
                .collect();
            referenced.sort_unstable_by_key(|(index, _)| *index);
            InlineRowSource::Chunks(referenced.into_iter().map(|(_, chunk)| chunk).collect())
        } else {
            InlineRowSource::Locators(locators)
        };
        Ok((source, selected, directory))
    }

    /// Retries a moving manifest before falling back to the original
    /// scanned-body path.
    async fn lookup_inline<T>(
        &self,
        session: &ReadSession,
        table: TableId,
        requested: &[u64],
        finish: impl AsyncFn(InlineRowSource, Vec<InlineRow>) -> Result<T>,
    ) -> Result<T> {
        let handle = session.handle();
        let mut attempt = 0;
        loop {
            let head = commit::read_head_value(handle).await?;
            let use_cache = attempt < 3;
            let outcome = async {
                let (source, rows, directory) = self
                    .requested_inline_rows(session, table, requested, head, use_cache)
                    .await?;
                finish(source, rows).await.map(|result| (result, directory))
            }
            .await;
            if handle.is_isolated() || !use_cache || commit::read_head_value(handle).await? == head
            {
                return outcome.map(|(result, directory)| {
                    if use_cache {
                        super::install(&self.row_lookups.inline, table, directory);
                    }
                    result
                });
            }
            attempt += 1;
        }
    }

    pub(in crate::catalog::handle) async fn requested_inline_row_ids(
        &self,
        table: TableId,
        requested: &[u64],
    ) -> Result<HashSet<u64>> {
        if requested.is_empty() {
            return Ok(HashSet::new());
        }
        let session = self.begin_read().await?;
        let outcome = self
            .lookup_inline(&session, table, requested, async |_, rows| {
                Ok(rows.into_iter().map(|row| row.row_id).collect())
            })
            .await;
        session.finish();
        outcome
    }

    pub(in crate::catalog::handle) async fn scan_recent_row(
        &self,
        session: &ReadSession,
        table: TableId,
        row: u64,
    ) -> Result<Option<RecentRow>> {
        self.lookup_inline(session, table, &[row], async |source, rows| {
            let (rows, chunks) = source.resolve_chunks(session.handle(), table, rows).await?;
            Ok(self
                .recent_rows_from_chunks(session.handle(), table, rows, chunks)
                .await?
                .into_iter()
                .next())
        })
        .await
    }
}

async fn latest_deletion(
    handle: ReadHandle<'_>,
    table: TableId,
    row: u64,
    head: u64,
) -> Result<Option<u64>> {
    InlineTombstones::open(handle, table.get(), row, row)
        .await?
        .latest_at(row, head)
        .await
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::Arc};

    use object_store::memory::InMemory;

    use crate::{
        Catalog, CatalogOptions, ColumnDef, InlineChunk, TableId,
        store::{
            handle::ReadHandle,
            key::{InlineKey, InlineOperation, Key, SysKey},
            value,
        },
        transaction::commit,
    };

    async fn fixture() -> (Arc<InMemory>, TableId) {
        let store = Arc::new(InMemory::new());
        let catalog = Catalog::open(store.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let table = Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").unwrap().id;
                let id = tx.create_table(
                    schema,
                    "inline_lookup",
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        ..Default::default()
                    }],
                )?;
                table.set(Some(id));
                for body in [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()] {
                    tx.inline_insert(
                        id,
                        &InlineChunk {
                            schema_version: 0,
                            row_count: 2,
                            arrow_schema: b"schema".to_vec(),
                            arrow_body: body,
                        },
                        &[],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let table = table.get().unwrap();
        catalog
            .commit(|tx| tx.inline_delete(table, 2, &[]))
            .await
            .unwrap();
        catalog.close().await.unwrap();
        (store, table)
    }

    #[tokio::test]
    async fn manifest_reader_caches_only_a_stable_inline_directory() {
        let (store, table) = fixture().await;
        let reader = Catalog::open_read_only(store, CatalogOptions::default())
            .await
            .unwrap();
        for _ in 0..2 {
            let row = reader.recent_row(table, 3).await.unwrap().unwrap();
            assert_eq!(row.offset_in_chunk, 1);
            assert_eq!(row.chunk_body.as_slice(), b"second");
            assert!(reader.recent_row(table, 2).await.unwrap().is_none());
            assert!(reader.recent_row(table, 999).await.unwrap().is_none());
        }
        assert!(
            reader
                .row_lookups
                .inline
                .read()
                .unwrap()
                .contains_key(&table)
        );
        reader.close().await.unwrap();
    }
    #[tokio::test]
    async fn manifest_lookup_retries_a_chunk_removed_by_a_maintenance_batch() {
        let (store, table) = fixture().await;
        let options = CatalogOptions {
            reader_poll_interval: std::time::Duration::from_millis(10),
            ..Default::default()
        };
        let reader = Catalog::open_read_only(store.clone(), options.clone())
            .await
            .unwrap();
        assert!(reader.recent_row(table, 3).await.unwrap().is_some());
        let writer = Catalog::open(store, options).await.unwrap();
        let session = reader.begin_read().await.unwrap();
        let attempts = Cell::new(0);
        let rows = reader
            .lookup_inline(&session, table, &[3], async |source, rows| {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    let tx = writer.begin_write_tx().await.unwrap();
                    let mut head = commit::read_head_value(ReadHandle::Tx(&tx)).await.unwrap();
                    head.batch_seq += 1;
                    tx.delete(
                        Key::Inline(InlineKey::Live(InlineOperation::Insert {
                            table_id: table.get(),
                            schema_version: 0,
                            begin_snapshot: 1,
                            chunk_seq: 1,
                        }))
                        .encode(),
                    )
                    .unwrap();
                    tx.delete(
                        Key::Inline(InlineKey::ChunkLocator {
                            table_id: table.get(),
                            row_id_end: 3,
                            schema_version: 0,
                            begin_snapshot: 1,
                            chunk_seq: 1,
                        })
                        .encode(),
                    )
                    .unwrap();
                    tx.put(Key::Sys(SysKey::Head).encode(), value::encode_value(&head))
                        .unwrap();
                    tx.commit().await.unwrap();
                    writer.close().await.unwrap();
                    tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        while commit::read_head_value(session.handle()).await.unwrap() != head {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .unwrap();
                }
                let (rows, _) = source.resolve_chunks(session.handle(), table, rows).await?;
                Ok(rows)
            })
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(attempts.get(), 2);
        session.finish();
        reader.close().await.unwrap();
    }
}
