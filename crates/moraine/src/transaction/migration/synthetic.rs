//! Synthetic migration units, installed into the driver's registry by a
//! fault-injection build so tests can drive the shipped planner.

use futures::{FutureExt, future::BoxFuture};
use slatedb::DbTransaction;

use crate::{
    error::{Error, Result},
    store::{
        StagedBytes,
        handle::ReadHandle,
        key::{EntityKey, Key},
        proto,
        read::{EntityRecord, scan_current_entities},
        value,
    },
    transaction::{
        commit::{FORMAT_VERSION, MAX_FORMAT_VERSION},
        migration::{MigrationUnit, StepOutcome, StepProgress},
    },
};

/// The option scope kind the rewritten records start under
/// (`OptionScope::Schema`).
pub(crate) const SOURCE_SCOPE: u64 = 1;
/// The option scope kind the rewrite moves them to (`OptionScope::Table`).
pub(crate) const TARGET_SCOPE: u64 = 2;

/// Walks the option records under [`SOURCE_SCOPE`] in key order and moves
/// each to [`TARGET_SCOPE`], one record per batch, writing the new key
/// before deleting the old.
fn move_scope_step<'a>(
    tx: &'a DbTransaction,
    cursor: &'a [u8],
) -> BoxFuture<'a, Result<StepOutcome>> {
    async move {
        let start = decode_cursor(cursor)?;

        let Some((scope_id, value)) = next_source_record(tx, start).await? else {
            return Ok(None);
        };

        let mut staged = StagedBytes::default();
        let target = Key::current(EntityKey::Option {
            scope_kind: TARGET_SCOPE,
            scope_id,
        })
        .encode();
        let encoded = value::encode_value(&value);
        staged.add(target.len(), encoded.len());
        tx.put(target, encoded).map_err(Error::from)?;

        let source = Key::current(EntityKey::Option {
            scope_kind: SOURCE_SCOPE,
            scope_id,
        })
        .encode();
        staged.add(source.len(), 0);
        tx.delete(source).map_err(Error::from)?;

        Ok(Some(StepProgress {
            cursor: scope_id.to_be_bytes().to_vec(),
            staged,
        }))
    }
    .boxed()
}

/// The scope id a cursor names, or `None` at the start of the walk.
fn decode_cursor(cursor: &[u8]) -> Result<Option<u64>> {
    if cursor.is_empty() {
        return Ok(None);
    }
    let bytes: [u8; 8] = cursor
        .try_into()
        .map_err(|_| Error::Corruption("migration cursor is not a scope id".to_string()))?;
    Ok(Some(u64::from_be_bytes(bytes)))
}

/// The first record still under [`SOURCE_SCOPE`] past `start`.
async fn next_source_record(
    tx: &DbTransaction,
    start: Option<u64>,
) -> Result<Option<(u64, proto::OptionScopeValue)>> {
    Ok(scan_current_entities(ReadHandle::Tx(tx))
        .await?
        .into_iter()
        .filter_map(|record| match record {
            EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            } if scope_kind == SOURCE_SCOPE => Some((scope_id, value)),
            _ => None,
        })
        .filter(|(scope_id, _)| start.is_none_or(|start| *scope_id > start))
        .min_by_key(|(scope_id, _)| *scope_id))
}

/// The rewriting unit. Its target is the newest format this binary reads,
/// so a store it migrates still attaches.
const MOVE_SCOPE: MigrationUnit = MigrationUnit {
    name: "move-option-scope",
    from_format: FORMAT_VERSION,
    to_format: MAX_FORMAT_VERSION,
    step: move_scope_step,
};

/// A step that walks nothing.
fn no_work_step<'a>(
    _tx: &'a DbTransaction,
    _cursor: &'a [u8],
) -> BoxFuture<'a, Result<StepOutcome>> {
    async move { Ok(None) }.boxed()
}

/// A second link for a multi-version jump. Its target is past the newest
/// format this binary reads, so a store carried through the chain will not
/// attach.
const SECOND_LINK: MigrationUnit = MigrationUnit {
    name: "second-link",
    from_format: MAX_FORMAT_VERSION,
    to_format: MAX_FORMAT_VERSION + 1,
    step: no_work_step,
};

/// The registry [`SyntheticMigration::MoveOptionScope`] installs.
///
/// [`SyntheticMigration::MoveOptionScope`]: crate::SyntheticMigration::MoveOptionScope
pub(crate) const REWRITE: &[&MigrationUnit] = &[&MOVE_SCOPE];

/// The registry [`SyntheticMigration::MoveOptionScopeThenLink`] installs.
///
/// [`SyntheticMigration::MoveOptionScopeThenLink`]: crate::SyntheticMigration::MoveOptionScopeThenLink
pub(crate) const REWRITE_THEN_LINK: &[&MigrationUnit] = &[&MOVE_SCOPE, &SECOND_LINK];
