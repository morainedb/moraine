//! Incremental reads for inline backfill sources.

use std::ops::Bound;

use slatedb::DbIterator;

use super::{
    Error, InlineChunkValue, InlineFileDeleteValue, InlineInlineDeleteValue, InlineKey,
    InlineOperation, InlineOperationKind, Key, ReadHandle, Result, ScanShape,
    inline_live_table_prefix, inline_row_tombstone_table_prefix, value,
};
use crate::store::proto::InlineBuildCursorValue;

pub(crate) struct InlineChunks(DbIterator);

impl InlineChunks {
    pub(crate) async fn open(
        handle: ReadHandle<'_>,
        table_id: u64,
        cursor: Option<&InlineBuildCursorValue>,
    ) -> Result<Self> {
        let prefix = inline_live_table_prefix(InlineOperationKind::Insert, table_id);
        let start = cursor.map_or(Bound::Unbounded, |cursor| {
            let key = Key::Inline(InlineKey::Live(InlineOperation::Insert {
                table_id,
                schema_version: cursor.schema_version,
                begin_snapshot: cursor.begin_snapshot,
                chunk_seq: cursor.chunk_seq,
            }))
            .encode();
            let suffix = key[prefix.len()..].to_vec();
            if cursor.chunk_finished {
                Bound::Excluded(suffix)
            } else {
                Bound::Included(suffix)
            }
        });
        Ok(Self(
            handle
                .scan_prefix(prefix, (start, Bound::Unbounded), ScanShape::Streaming)
                .await?,
        ))
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(InlineOperation, InlineChunkValue)>> {
        let Some(entry) = self.0.next().await? else {
            return Ok(None);
        };
        match Key::decode(&entry.key)? {
            Key::Inline(InlineKey::Live(operation @ InlineOperation::Insert { .. })) => {
                Ok(Some((operation, value::decode_owned(entry.value)?)))
            }
            key => Err(Error::Corruption(format!(
                "non-insert key in inline chunk scan: {key:?}"
            ))),
        }
    }
}

/// Tombstone events in row order, retaining only the next event.
pub(crate) struct InlineTombstones {
    iterator: DbIterator,
    next: Option<(u64, u64)>,
}

impl InlineTombstones {
    pub(crate) async fn open(
        handle: ReadHandle<'_>,
        table_id: u64,
        start: u64,
        end: u64,
    ) -> Result<Self> {
        let prefix = inline_row_tombstone_table_prefix(table_id);
        let key = |row_id, end_snapshot| {
            Key::Inline(InlineKey::RowTombstone {
                table_id,
                row_id,
                end_snapshot,
            })
            .encode()[prefix.len()..]
                .to_vec()
        };
        let iterator = handle
            .scan_prefix(
                &prefix,
                key(start, 0)..=key(end, u64::MAX),
                ScanShape::Streaming,
            )
            .await?;
        let mut tombstones = Self {
            iterator,
            next: None,
        };
        tombstones.advance().await?;
        Ok(tombstones)
    }

    async fn advance(&mut self) -> Result<()> {
        self.next = match self.iterator.next().await? {
            None => None,
            Some(entry) => match Key::decode(&entry.key)? {
                Key::Inline(InlineKey::RowTombstone {
                    row_id,
                    end_snapshot,
                    ..
                }) => {
                    let decoded: InlineInlineDeleteValue = value::decode_owned(entry.value)?;
                    if decoded.end_snapshot != end_snapshot {
                        return Err(Error::Corruption(
                            "inline tombstone key and value disagree".into(),
                        ));
                    }
                    Some((row_id, end_snapshot))
                }
                key => {
                    return Err(Error::Corruption(format!(
                        "non-tombstone key in inline scan: {key:?}"
                    )));
                }
            },
        };
        Ok(())
    }

    /// Resolves increasing row ids against the latest deletion event.
    pub(crate) async fn latest(&mut self, row_id: u64) -> Result<Option<u64>> {
        let mut latest = None;
        while let Some((row, end)) = self.next {
            if row > row_id {
                break;
            }
            if row == row_id {
                latest = Some(end);
            }
            self.advance().await?;
        }
        Ok(latest)
    }
}

/// Inline file-deletion positions for one physical data file.
pub(crate) async fn file_delete_positions(
    handle: ReadHandle<'_>,
    table_id: u64,
    data_file_id: u64,
) -> Result<std::collections::HashSet<u64>> {
    let prefix = inline_live_table_prefix(InlineOperationKind::FileDelete, table_id);
    let key = |row_id| {
        Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
            table_id,
            data_file_id,
            row_id,
        }))
        .encode()[prefix.len()..]
            .to_vec()
    };
    let mut iterator = handle
        .scan_prefix(&prefix, key(0)..=key(u64::MAX), ScanShape::Streaming)
        .await?;
    let mut positions = std::collections::HashSet::new();
    while let Some(entry) = iterator.next().await? {
        match Key::decode(&entry.key)? {
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete { row_id, .. })) => {
                let _: InlineFileDeleteValue = value::decode_owned(entry.value)?;
                positions.insert(row_id);
            }
            key => {
                return Err(Error::Corruption(format!(
                    "non-file-delete key in inline scan: {key:?}"
                )));
            }
        }
    }
    Ok(positions)
}
