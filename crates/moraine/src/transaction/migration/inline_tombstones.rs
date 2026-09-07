//! Rewrites row-level inline tombstones into deletion-event keys.

use std::ops::Bound;

use futures::{FutureExt, future::BoxFuture};
use slatedb::DbTransaction;

use super::{MigrationUnit, StepOutcome, StepProgress};
use crate::{
    error::{Error, Result},
    store::{
        StagedBytes,
        handle::{ReadHandle, ScanShape},
        key::{InlineKey, InlineOperation, InlineOperationKind, Key, inline_live_kind_prefix},
        proto::InlineInlineDeleteValue,
        value,
    },
};

const TOMBSTONES_PER_STEP: usize = 256;

/// Moves the tombstones written by formats 1 through 8 into format 9.
pub(super) const MIGRATION: MigrationUnit = MigrationUnit {
    name: "version-inline-tombstones",
    from_format: 8,
    to_format: 9,
    step,
};

fn step<'a>(tx: &'a DbTransaction, cursor: &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>> {
    async move {
        let prefix = inline_live_kind_prefix(InlineOperationKind::InlineDelete);
        let lower = if cursor.is_empty() {
            Bound::Unbounded
        } else {
            if !matches!(
                Key::decode(cursor)?,
                Key::Inline(InlineKey::Live(InlineOperation::InlineDelete { .. }))
            ) {
                return Err(Error::Corruption(
                    "inline tombstone migration cursor names another key kind".to_owned(),
                ));
            }
            let suffix = cursor.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                Error::Corruption(
                    "inline tombstone migration cursor has the wrong prefix".to_owned(),
                )
            })?;
            Bound::Excluded(suffix.to_vec())
        };
        let mut records = ReadHandle::Tx(tx)
            .scan_prefix(
                prefix,
                (lower, Bound::<Vec<u8>>::Unbounded),
                ScanShape::Bulk,
            )
            .await?;
        let mut staged = StagedBytes::default();
        let mut last = None;
        for _ in 0..TOMBSTONES_PER_STEP {
            let Some(record) = records.next().await? else {
                break;
            };
            let Key::Inline(InlineKey::Live(InlineOperation::InlineDelete { table_id, row_id })) =
                Key::decode(&record.key)?
            else {
                return Err(Error::Corruption(
                    "inline tombstone migration encountered another key kind".to_owned(),
                ));
            };
            let tombstone: InlineInlineDeleteValue = value::decode_owned(record.value.clone())?;
            let target = Key::Inline(InlineKey::RowTombstone {
                table_id,
                row_id,
                end_snapshot: tombstone.end_snapshot,
            })
            .encode();
            staged.add(target.len(), record.value.len());
            tx.put(target, record.value).map_err(Error::from)?;
            staged.add(record.key.len(), 0);
            tx.delete(record.key.clone()).map_err(Error::from)?;
            last = Some(record.key.to_vec());
        }
        Ok(last.map(|cursor| StepProgress { cursor, staged }))
    }
    .boxed()
}
