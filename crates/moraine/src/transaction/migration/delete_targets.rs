//! Bounded current delete-file target backfill.

use std::ops::Bound;

use futures::{FutureExt, future::BoxFuture};

use super::{MigrationUnit, StepOutcome, StepProgress};
use crate::{
    Error, MemoryReservation, Result,
    store::{
        StagedBytes,
        handle::ScanShape,
        key::{CurrentKey, EntityKey, EntityKind, Key, current_entity_kind_prefix},
    },
    transaction::{file_targets, session::CommitSession},
};

pub(super) const MIGRATION: MigrationUnit = MigrationUnit {
    name: "index-delete-file-targets",
    from_format: 11,
    to_format: 12,
    step,
};

fn step<'a>(session: &'a CommitSession, cursor: &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>> {
    async move {
        if !cursor.is_empty()
            && !matches!(
                Key::decode(cursor)?,
                Key::Current(CurrentKey::Entity(EntityKey::DeleteFile { .. }))
            )
        {
            return Err(Error::Corruption(
                "invalid delete-target migration cursor".into(),
            ));
        }
        let prefix = current_entity_kind_prefix(EntityKind::DeleteFile);
        let lower = cursor
            .strip_prefix(prefix.as_slice())
            .map_or(Bound::Unbounded, |suffix| Bound::Excluded(suffix.to_vec()));
        let mut rows = session
            .handle()
            .scan_prefix(
                prefix,
                (lower, Bound::<Vec<u8>>::Unbounded),
                ScanShape::bounded().reuse_only(),
            )
            .await?;
        let mut staged = StagedBytes::default();
        let mut last = None;
        for _ in 0..256 {
            let Some(row) = rows.next().await? else { break };
            let Key::Current(CurrentKey::Entity(entity)) = Key::decode(&row.key)? else {
                return Err(Error::Corruption(
                    "invalid delete-target migration source".into(),
                ));
            };
            let _memory = MemoryReservation::reserve(
                (row.value.len() as u64)
                    .saturating_mul(4)
                    .saturating_add(256),
            )?;
            if let Some(key) = file_targets::entry(entity, &row.value)? {
                let key = key.encode();
                staged.add(key.len(), 0);
                session.put(key, [])?;
            }
            last = Some(row.key.to_vec());
            if staged.0 >= 1024 * 1024 {
                break;
            }
        }
        Ok(last.map(|cursor| StepProgress { cursor, staged }))
    }
    .boxed()
}
