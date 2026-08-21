//! The inline read seam: materializes DuckLake's four inline scan variants
//! over the `inline/*` keyspace and re-exports
//! [`InlineScanKind`](crate::catalog::inline::InlineScanKind) from the
//! otherwise-private `catalog`. Each function opens a fresh read-only
//! transaction, scans, and rolls back.

use bytes::Bytes;

#[doc(hidden)]
pub use crate::catalog::inline::InlineScanKind;
use crate::{
    catalog::ReadOnlyCatalog,
    error::{Error, Result},
    store::{inline as store_inline, key::InlineOperation},
};

/// One inlined row selected by [`scan_inline`], referencing its chunk's
/// body by index into the scan's deduplicated
/// [`chunk_bodies`](InlineScanRecord::chunk_bodies).
#[doc(hidden)]
pub struct InlineRowRecord {
    /// The row's dense id.
    pub row_id: u64,
    /// The schema version the owning chunk was written under.
    pub schema_version: u64,
    /// The commit snapshot that inserted this row.
    pub begin_snapshot: u64,
    /// The commit snapshot that tombstoned this row, if any.
    pub end_snapshot: Option<u64>,
    /// The owning chunk: an index into the scan's `chunk_bodies`.
    pub chunk_index: u64,
    /// The row's index within its chunk (`0..row_count`).
    pub offset_in_chunk: u64,
}

/// A selected scan: the rows plus the chunk bodies they reference.
#[doc(hidden)]
pub struct InlineScanRecord {
    /// The selected rows, in the scan variant's order.
    pub rows: Vec<InlineRowRecord>,
    /// The referenced chunks' full Arrow IPC record-batch bodies, each
    /// appearing once, indexed by [`InlineRowRecord::chunk_index`].
    pub chunk_bodies: Vec<Bytes>,
}

/// Materializes `table_id`'s inlined rows and selects `kind`'s variant at
/// `snapshot` (windowed from `start` for the incremental variants) — the
/// read model behind `moraine_inline_scan`. Served from the chunk-range
/// directory once it is known complete, hauling only the chunk bodies the
/// selected rows reference. `schema_version`, when set, keeps only rows
/// whose chunk was written under that version, so a caller serving one
/// version's projection never hauls another version's bodies.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn scan_inline(
    catalog: &ReadOnlyCatalog,
    table_id: u64,
    kind: InlineScanKind,
    snapshot: u64,
    start: u64,
    schema_version: Option<u64>,
) -> Result<InlineScanRecord> {
    let (selected, chunks) = catalog
        .select_inline_rows(table_id, kind, snapshot, start, schema_version)
        .await?;

    // Rows arrive with `chunk` dense-indexed in first-reference order over
    // exactly the referenced chunks, which is the record's shape already.
    let chunk_bodies: Vec<Bytes> = chunks.iter().map(|(_, chunk)| chunk.body.clone()).collect();
    let rows = selected
        .into_iter()
        .map(|row| {
            let (operation, _) = &chunks[row.chunk];
            let InlineOperation::Insert { schema_version, .. } = operation else {
                return Err(Error::Corruption(format!(
                    "inline row {} references a non-insert chunk key: {operation:?}",
                    row.row_id
                )));
            };

            Ok(InlineRowRecord {
                row_id: row.row_id,
                schema_version: *schema_version,
                begin_snapshot: row.begin_snapshot,
                end_snapshot: row.end_snapshot,
                chunk_index: row.chunk as u64,
                offset_in_chunk: row.offset_in_chunk,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(InlineScanRecord { rows, chunk_bodies })
}

/// Every `(schema_version, arrow_schema)` recorded for `table_id`, in
/// schema-version order — the read model behind `moraine_inline_schemas`.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_schemas(catalog: &ReadOnlyCatalog, table_id: u64) -> Result<Vec<(u64, Bytes)>> {
    let read = catalog.begin_catalog_read().await?;
    let head = read.head_value();
    // A base-table scan asks once per registered schema version and every ask
    // returns the same whole list, so the scan is paid once per head.
    if let Some(schemas) =
        crate::catalog::projection::inline_schemas_at(catalog.projections(), table_id, &head)
    {
        read.finish().await;
        return Ok(schemas.as_ref().clone());
    }

    let schemas = store_inline::scan_inline_schemas(read.handle(), read.overlay(), table_id).await;
    read.finish().await;
    let schemas: Vec<(u64, Bytes)> = schemas?
        .into_iter()
        .map(|(schema_version, value)| (schema_version, value.arrow_schema))
        .collect();
    crate::catalog::projection::install_inline_schemas(
        catalog.projections(),
        table_id,
        head,
        std::sync::Arc::new(schemas.clone()),
    );

    Ok(schemas)
}

/// Every `(table_id, schema_version)` with a recorded inline schema,
/// across every table — feeds the `ducklake_inlined_data_tables`
/// projection behind `moraine_inline_registered_tables`.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_registered_tables(catalog: &ReadOnlyCatalog) -> Result<Vec<(u64, u64)>> {
    let read = catalog.begin_catalog_read().await?;
    let schemas = store_inline::scan_all_inline_schemas(read.handle(), read.overlay()).await;
    read.finish().await;
    Ok(schemas?
        .into_iter()
        .map(|(table_id, schema_version, _)| (table_id, schema_version))
        .collect())
}

/// Whether `table_id`'s `ducklake_inlined_delete_<table_id>` exists: it
/// does not until the first `inline/file_delete` is staged, and stays
/// existing after a flush clears the records. The record fallback covers
/// stores written before the marker.
///
/// # Errors
///
/// Returns an error if the underlying store read fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_file_delete_table_exists(
    catalog: &ReadOnlyCatalog,
    table_id: u64,
) -> Result<bool> {
    let read = catalog.begin_catalog_read().await?;
    let marked =
        store_inline::read_inline_file_delete_table(read.handle(), read.overlay(), table_id).await;
    let exists = match marked {
        Ok(true) => Ok(true),
        Ok(false) => {
            store_inline::scan_inline_file_deletes(read.handle(), read.overlay(), table_id)
                .await
                .map(|records| !records.is_empty())
        }
        Err(error) => Err(error),
    };
    read.finish().await;
    exists
}

/// Every `inline/file_delete` record for `table_id` as
/// `(data_file_id, row_id, begin_snapshot)` in key order — the rows behind
/// the `ducklake_inlined_delete_<t>` projection.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_file_deletes(
    catalog: &ReadOnlyCatalog,
    table_id: u64,
) -> Result<Vec<(u64, u64, u64)>> {
    let read = catalog.begin_catalog_read().await?;
    let file_deletes =
        store_inline::scan_inline_file_deletes(read.handle(), read.overlay(), table_id).await;
    read.finish().await;
    Ok(file_deletes?
        .into_iter()
        .map(|(data_file_id, row_id, value)| (data_file_id, row_id, value.begin_snapshot))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        catalog::CatalogOptions,
        transaction::staged::{Cell, RowOperation, TableKind},
    };

    fn snapshot_row(id: u64) -> Vec<Cell> {
        vec![
            Cell::U64(id),
            Cell::I64(1),
            Cell::U64(0),
            Cell::U64(1),
            Cell::U64(0),
        ]
    }

    fn snapshot_changes_row(id: u64) -> Vec<Cell> {
        vec![
            Cell::U64(id),
            Cell::Str("inlined_insert:1".to_string()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ]
    }

    async fn open() -> crate::catalog::Catalog {
        crate::catalog::Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap()
    }

    /// Inline rows committed to a slot with no folder running are served by
    /// the inline dump through the tail overlay. Against a folded-only read
    /// the tail is invisible and this scan returns nothing.
    #[tokio::test]
    async fn scan_inline_serves_unfolded_rows_on_a_slot_backed_attach() {
        let catalog = open().await;

        let mut tx = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        tx.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: b"schema".to_vec(),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk-a".to_vec(),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        // No folder has run: the rows live only in the unfolded tail.
        let record = scan_inline(&catalog, 1, InlineScanKind::Table, 1, 0, None)
            .await
            .unwrap();
        let mut ids: Vec<u64> = record.rows.iter().map(|r| r.row_id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![0, 1],
            "unfolded inline rows are served through the overlay"
        );
        assert_eq!(record.chunk_bodies, vec![Bytes::from_static(b"chunk-a")]);

        let schemas = inline_schemas(&catalog, 1).await.unwrap();
        assert_eq!(schemas, vec![(0, Bytes::from_static(b"schema"))]);
    }

    /// A base-table scan asks once per registered schema version; the walk
    /// those asks share is paid once.
    #[tokio::test]
    async fn per_version_scans_share_one_walk_of_the_chunks() {
        let catalog = open().await;
        let mut tx = catalog.begin_staged(None, String::new()).await.unwrap();
        for version in 0..3u64 {
            tx.stage(RowOperation::InlineSchema {
                table_id: 1,
                schema_version: version,
                arrow_schema: b"schema".to_vec(),
            });
        }
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 1,
            arrow_body: b"chunk-a".to_vec(),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        let read = catalog.begin_dump().await.unwrap();
        let head = read.head_value();
        read.finish().await;
        assert!(
            crate::catalog::projection::inline_chunks_at(catalog.projections(), 1, &head).is_none(),
            "nothing is remembered before the first ask"
        );

        for version in 0..3u64 {
            scan_inline(&catalog, 1, InlineScanKind::Table, 1, 0, Some(version))
                .await
                .unwrap();
        }

        let read = catalog.begin_dump().await.unwrap();
        let later_head = read.head_value();
        read.finish().await;
        assert_eq!(
            (later_head.snapshot_id, later_head.batch_seq),
            (head.snapshot_id, head.batch_seq),
            "a read moves no head, so every ask of one statement shares one"
        );
        assert!(
            crate::catalog::projection::inline_chunks_at(catalog.projections(), 1, &later_head)
                .is_some(),
            "the walk the first ask paid for is there for the rest"
        );
    }

    /// Two chunks (rows 0-1, row 2) staged in one commit, one tombstone
    /// on row 1: `scan_inline` with `Table` at the tombstone's snapshot
    /// returns rows 0 and 2 with their chunk bodies attached, and
    /// `inline_schemas`/`inline_registered_tables` see the recorded
    /// schema.
    #[tokio::test]
    async fn scan_inline_materializes_rows_with_chunk_bodies() {
        let catalog = open().await;
        let mut tx = catalog.begin_staged(None, String::new()).await.unwrap();

        tx.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: b"schema".to_vec(),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk-a".to_vec(),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 2,
            row_count: 1,
            arrow_body: b"chunk-b".to_vec(),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        let mut inline_delete = catalog.begin_staged(None, String::new()).await.unwrap();
        inline_delete.stage(RowOperation::InlineInlineDelete {
            table_id: 1,
            row_id: 1,
            end_snapshot: 2,
        });
        inline_delete.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2),
        });
        inline_delete.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2),
        });
        inline_delete.commit().await.unwrap();

        let record = scan_inline(&catalog, 1, InlineScanKind::Table, 2, 0, None)
            .await
            .unwrap();
        let mut by_id: Vec<(u64, Bytes, u64)> = record
            .rows
            .iter()
            .map(|r| {
                (
                    r.row_id,
                    record.chunk_bodies[usize::try_from(r.chunk_index).unwrap()].clone(),
                    r.offset_in_chunk,
                )
            })
            .collect();
        by_id.sort_by_key(|(id, ..)| *id);
        assert_eq!(
            by_id,
            vec![
                (0, Bytes::from_static(b"chunk-a"), 0),
                (2, Bytes::from_static(b"chunk-b"), 0)
            ]
        );
        // Each referenced chunk's body crosses once.
        assert_eq!(record.chunk_bodies.len(), 2);

        let schemas = inline_schemas(&catalog, 1).await.unwrap();
        assert_eq!(schemas, vec![(0, Bytes::from_static(b"schema"))]);

        let registered = inline_registered_tables(&catalog).await.unwrap();
        assert_eq!(registered, vec![(1, 0)]);
    }

    /// The first `scan_inline` walk verifies the chunk directory and
    /// remembers it; the next serves from it, hauling only referenced
    /// bodies. The proof is divergence: a chunk planted without a locator
    /// is visible only to a body scan.
    #[tokio::test]
    async fn scan_inline_serves_from_the_directory_once_verified() {
        use crate::store::{
            key::{InlineKey, InlineOperation, Key},
            proto, value,
        };

        let catalog = open().await;
        let mut tx = catalog.begin_staged(None, String::new()).await.unwrap();
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk-a".to_vec(),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        let record = scan_inline(&catalog, 1, InlineScanKind::Table, 1, 0, None)
            .await
            .unwrap();
        assert_eq!(record.rows.len(), 2);
        assert!(
            crate::catalog::projection::inline_directory_complete(catalog.projections(), 1),
            "the first walk must verify and remember a complete directory"
        );

        // The divergence: a chunk with no locator, behind the memo's back.
        catalog
            .with_folder_writer(async |db| {
                let tx = db
                    .begin(slatedb::IsolationLevel::Snapshot)
                    .await
                    .map_err(crate::error::Error::from)?;
                tx.put(
                    Key::Inline(InlineKey::Live(InlineOperation::Insert {
                        table_id: 1,
                        schema_version: 0,
                        begin_snapshot: 1,
                        chunk_seq: 5,
                    }))
                    .encode(),
                    value::encode_value(&proto::InlineChunkValue {
                        body: b"rogue".to_vec().into(),
                        row_id_start: 10,
                        row_count: 2,
                        data_file_id: None,
                    }),
                )
                .unwrap();
                crate::transaction::commit::commit_durably(db, tx)
                    .await
                    .unwrap();
                Ok(())
            })
            .await
            .unwrap();

        let record = scan_inline(&catalog, 1, InlineScanKind::Table, 1, 0, None)
            .await
            .unwrap();
        let row_ids: Vec<u64> = record.rows.iter().map(|row| row.row_id).collect();
        assert_eq!(
            row_ids,
            [0, 1],
            "a directory-served scan must not see a chunk only a body scan finds"
        );
        assert_eq!(record.chunk_bodies, vec![Bytes::from_static(b"chunk-a")]);
    }

    /// A version-filtered scan returns one version's rows and hauls one
    /// version's bodies — the other version's chunks are not in the record
    /// at all, so a caller serving one version's projection never touches
    /// the rest of the table.
    #[tokio::test]
    async fn scan_inline_filtered_to_a_version_hauls_only_its_bodies() {
        let catalog = open().await;
        let mut tx = catalog.begin_staged(None, String::new()).await.unwrap();

        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk-v0".to_vec(),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 1,
            begin_snapshot: 1,
            row_id_start: 2,
            row_count: 1,
            arrow_body: b"chunk-v1".to_vec(),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        // Once eagerly (verifying the directory), once served from it: the
        // filter must narrow both paths the same way.
        for pass in ["chunk scan", "directory"] {
            let record = scan_inline(&catalog, 1, InlineScanKind::Table, 1, 0, Some(1))
                .await
                .unwrap();
            let rows: Vec<(u64, u64, u64)> = record
                .rows
                .iter()
                .map(|row| (row.row_id, row.schema_version, row.chunk_index))
                .collect();
            assert_eq!(rows, vec![(2, 1, 0)], "one version-1 row ({pass})");
            assert_eq!(
                record.chunk_bodies,
                vec![Bytes::from_static(b"chunk-v1")],
                "the other version's body must not be hauled ({pass})"
            );
        }
    }

    /// Staged `inline/file_delete` rows read back as
    /// `(data_file_id, row_id, begin_snapshot)` in key order — the rows
    /// behind the `ducklake_inlined_delete_<t>` projection.
    #[tokio::test]
    async fn inline_file_deletes_read_back_in_key_order() {
        let catalog = open().await;
        let mut tx = catalog.begin_staged(None, String::new()).await.unwrap();

        tx.stage(RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 7,
            row_id: 2,
            begin_snapshot: 6,
        });
        tx.stage(RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 7,
            row_id: 0,
            begin_snapshot: 6,
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(6),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(6),
        });
        tx.commit().await.unwrap();

        let rows = inline_file_deletes(&catalog, 1).await.unwrap();
        assert_eq!(rows, vec![(7, 0, 6), (7, 2, 6)]);

        assert!(inline_file_delete_table_exists(&catalog, 1).await.unwrap());
        assert_eq!(inline_file_deletes(&catalog, 9).await.unwrap(), vec![]);
    }
}
