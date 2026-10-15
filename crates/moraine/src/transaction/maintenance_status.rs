//! Durable, bounded maintenance-pass status.

use std::sync::Arc;

use crate::{
    Catalog, MaintenanceStatusPass, MaintenanceStatusStep, ReadOnlyCatalog, Timestamp,
    error::{Error, Result},
    store::{
        key::{Key, SysKey},
        proto::{MaintenanceStatusPassValue, MaintenanceStatusStepValue},
        read, value,
    },
    transaction::commit::{self, MAX_COMMIT_ATTEMPTS, StagedWrite},
};

const RETAINED_PASSES: usize = 16;

/// Reads the durable history, newest first.
pub(crate) async fn read(catalog: &ReadOnlyCatalog) -> Result<Vec<MaintenanceStatusPass>> {
    let session = catalog.begin_read().await?;
    let stored = read::read_maintenance_status(session.handle()).await;
    session.finish();

    Ok(stored?
        .unwrap_or_default()
        .passes
        .into_iter()
        .rev()
        .map(decode_pass)
        .collect())
}

/// Appends one pass and durably retains the newest bounded window, as a
/// head-preserving commit stamped with the head it was recorded against.
pub(crate) async fn record(catalog: &Catalog, mut pass: MaintenanceStatusPass) -> Result<()> {
    let store = catalog.store();
    let durability = store.commit_durability();

    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(commit::retry_backoff(attempt)).await;
        }

        let session = store.begin_commit_session().await?;
        let base = commit::head_view_for(session.handle(), catalog.projections()).await?;
        let status = read::read_maintenance_status(session.handle()).await?;
        pass.head_snapshot_id = base.snapshot.snapshot_id;
        pass.head_batch_seq = base.batch_seq;
        let mut status = status.unwrap_or_default();
        status.passes.push(encode_pass(pass.clone()));
        if status.passes.len() > RETAINED_PASSES {
            let remove = status.passes.len() - RETAINED_PASSES;
            status.passes.drain(..remove);
        }

        let head = base.snapshot.snapshot_id;
        let mut writes: Vec<StagedWrite> = vec![(
            Key::Sys(SysKey::MaintenanceStatus).encode(),
            Some(value::encode_value(&status)),
        )];
        writes.push(commit::head_stamp(head, base.batch_seq));
        let staged = commit::stage_writes_on(&session, &writes)?;

        let landed = commit::commit_batch(
            Arc::new(session),
            commit::HeadTransition {
                before: head,
                after: head,
            },
            &writes,
            staged,
            commit::HeadViewUpdate::Rebuild(base),
            commit::Landing {
                projections: catalog.projections(),
                log: store.commit_log(),
            },
            &durability,
        )
        .await?;
        if matches!(landed, commit::Landed::Committed(_)) {
            return Ok(());
        }
    }

    Err(Error::RetryBudgetExhausted(format!(
        "maintenance status did not settle in {MAX_COMMIT_ATTEMPTS} attempts"
    )))
}

fn encode_pass(pass: MaintenanceStatusPass) -> MaintenanceStatusPassValue {
    MaintenanceStatusPassValue {
        started_at_micros: pass.started_at.as_micros(),
        trigger: pass.trigger,
        head_snapshot_id: pass.head_snapshot_id,
        head_batch_seq: pass.head_batch_seq,
        work_pending: pass.work_pending,
        steps: pass
            .steps
            .into_iter()
            .map(|step| MaintenanceStatusStepValue {
                step: step.step,
                status: step.status,
                detail: step.detail,
            })
            .collect(),
    }
}

fn decode_pass(pass: MaintenanceStatusPassValue) -> MaintenanceStatusPass {
    let mut decoded = MaintenanceStatusPass::new(
        Timestamp::from_micros(pass.started_at_micros),
        pass.trigger,
        pass.steps
            .into_iter()
            .map(|step| MaintenanceStatusStep::new(step.step, step.status, step.detail))
            .collect(),
    );
    decoded.head_snapshot_id = pass.head_snapshot_id;
    decoded.head_batch_seq = pass.head_batch_seq;
    decoded.work_pending = pass.work_pending;
    decoded
}
