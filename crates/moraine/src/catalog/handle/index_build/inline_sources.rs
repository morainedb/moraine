//! Inline source traversal with one decoded chunk retained.

use arrow::datatypes::SchemaRef;

use super::{ColumnId, Error, InlineBuildCursorValue, Result, StepBuffer, backfill};
use crate::{
    catalog::TableId,
    data_file,
    store::{
        handle::ReadHandle,
        inline::{
            self as store_inline,
            stream::{InlineChunks, InlineTombstones},
        },
        key::InlineOperation,
    },
};

pub(super) async fn stream_inline_sources(
    source: backfill::BackfillSource<'_>,
    columns: &[ColumnId],
    legacy_cursor: Option<u64>,
    buffer: &mut StepBuffer<'_>,
) -> Result<()> {
    let backfill::BackfillSource {
        snapshot,
        table,
        handle,
    } = source;
    let positions = snapshot.column_positions(table, columns)?;
    let initial_cursor = buffer.inline_cursor;
    let mut chunks = InlineChunks::open(handle, table.get(), initial_cursor.as_ref()).await?;
    let mut schema = None;
    while let Some((operation, chunk)) = chunks.next().await? {
        let InlineOperation::Insert {
            schema_version,
            begin_snapshot,
            chunk_seq,
            ..
        } = operation
        else {
            return Err(Error::Corruption("non-insert inline build source".into()));
        };
        let current_schema = cached_schema(handle, table, schema_version, &mut schema).await?;
        let start = initial_cursor
            .as_ref()
            .filter(|cursor| {
                (
                    cursor.schema_version,
                    cursor.begin_snapshot,
                    cursor.chunk_seq,
                ) == (schema_version, begin_snapshot, chunk_seq)
            })
            .map_or(0, |cursor| cursor.next_position);
        let columns = snapshot
            .inline_read_columns(handle, table, begin_snapshot)
            .await?;
        let mut rows = data_file::InlineRows::new(
            current_schema,
            &chunk.body,
            &positions,
            chunk.row_id_start,
            start,
            &columns,
        )?;
        if u64::try_from(rows.row_count()).ok() != Some(chunk.row_count) {
            return Err(Error::Corruption(
                "inline chunk row count disagrees with its body".into(),
            ));
        }
        buffer.peak_inline_body_bytes = buffer.peak_inline_body_bytes.max(chunk.body.len());
        buffer.peak_inline_decoded_bytes =
            buffer.peak_inline_decoded_bytes.max(rows.decoded_bytes());
        let end = chunk
            .row_id_start
            .checked_add(chunk.row_count.saturating_sub(1))
            .ok_or_else(|| Error::Corruption("inline chunk row range overflow".into()))?;
        let mut tombstones =
            InlineTombstones::open(handle, table.get(), chunk.row_id_start, end).await?;
        while let Some(entry) = rows.next()? {
            let next_position = entry.ordinal + 1;
            let dead = tombstones
                .latest(entry.row_id)
                .await?
                .is_some_and(|end| begin_snapshot < end);
            if !dead && legacy_cursor.is_none_or(|cursor| entry.row_id > cursor) {
                buffer.push_values(entry.row_id, &entry.values).await?;
            }
            buffer.inline_cursor = Some(InlineBuildCursorValue {
                schema_version,
                begin_snapshot,
                chunk_seq,
                next_position,
                chunk_finished: next_position == chunk.row_count,
                complete: false,
                covered_snapshot: 0,
            });
        }
    }
    Ok(())
}

async fn cached_schema(
    handle: ReadHandle<'_>,
    table: TableId,
    version: u64,
    cached: &mut Option<(u64, SchemaRef)>,
) -> Result<SchemaRef> {
    if let Some((current, schema)) = cached
        && *current == version
    {
        return Ok(schema.clone());
    }
    let bytes = store_inline::read_inline_schema(handle, table.get(), version)
        .await?
        .ok_or_else(|| {
            Error::Corruption(format!(
                "no inline schema for table {table} version {version}"
            ))
        })?;
    let schema = data_file::decode_inline_schema(bytes)?;
    *cached = Some((version, schema.clone()));
    Ok(schema)
}
