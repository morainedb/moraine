//! Bounded exact-name backfill for native and commit-log catalogs.

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
    transaction::session::CommitSession,
};

pub(super) const NATIVE: MigrationUnit = MigrationUnit {
    name: "index-current-names",
    from_format: 9,
    to_format: 11,
    step,
};
pub(super) const COMMIT_LOG: MigrationUnit = MigrationUnit {
    name: "index-current-names",
    from_format: 10,
    to_format: 11,
    step,
};

fn step<'a>(session: &'a CommitSession, cursor: &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>> {
    async move {
        if !cursor.is_empty()
            && !matches!(
                Key::decode(cursor)?,
                Key::Current(CurrentKey::Entity(
                    EntityKey::Schema { .. } | EntityKey::Table { .. }
                ))
            )
        {
            return Err(Error::Corruption("invalid name migration cursor".into()));
        }
        let mut staged = StagedBytes::default();
        let mut last = None;
        let mut visited = 0;
        for kind in [EntityKind::Schema, EntityKind::Table] {
            let prefix = current_entity_kind_prefix(kind);
            if !cursor.is_empty() && cursor > prefix.as_slice() && !cursor.starts_with(&prefix) {
                continue;
            }
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
            while let Some(row) = rows.next().await? {
                let Key::Current(CurrentKey::Entity(entity)) = Key::decode(&row.key)? else {
                    return Err(Error::Corruption(
                        "name migration encountered a non-entity key".into(),
                    ));
                };
                let _memory = MemoryReservation::reserve(
                    (row.value.len() as u64)
                        .saturating_mul(4)
                        .saturating_add(256),
                )?;
                if let Some(name) = crate::transaction::names::entry(entity, &row.value)? {
                    let key = Key::Name(name).encode();
                    staged.add(key.len(), 0);
                    session.put(key, [])?;
                }
                last = Some(row.key.to_vec());
                visited += 1;
                if visited == 256 || staged.0 >= 1024 * 1024 {
                    break;
                }
            }
            if visited == 256 || staged.0 >= 1024 * 1024 {
                break;
            }
        }
        Ok(last.map(|cursor| StepProgress { cursor, staged }))
    }
    .boxed()
}
