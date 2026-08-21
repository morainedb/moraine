//! Durable, bounded maintenance-pass status.

use slatedb::ErrorKind;

use crate::{
    Catalog, MaintenanceStatusPass, MaintenanceStatusStep, ReadOnlyCatalog, Timestamp,
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{Key, SysKey},
        proto::{MaintenanceStatusPassValue, MaintenanceStatusStepValue},
        read, value,
    },
    transaction::commit::{self, FORMAT_WITH_MAINTENANCE_STATUS, MAX_COMMIT_ATTEMPTS, StagedWrite},
};

const RETAINED_PASSES: usize = 16;

/// Reads the durable history, newest first.
pub(crate) async fn read(catalog: &ReadOnlyCatalog) -> Result<Vec<MaintenanceStatusPass>> {
    // Through the dump read: the pass is written under the folder role, and
    // the shared reader only refreshes on its poll interval, so a status just
    // recorded through this handle is invisible to it until then.
    let dump = catalog.begin_dump().await?;
    let stored = read::read_maintenance_status(dump.handle()).await;
    dump.finish().await;

    Ok(stored?
        .unwrap_or_default()
        .passes
        .into_iter()
        .rev()
        .map(decode_pass)
        .collect())
}

/// Appends one pass and durably retains the newest bounded window.
pub(crate) async fn record(catalog: &Catalog, pass: MaintenanceStatusPass) -> Result<()> {
    let encoded = encode_pass(pass);

    // The status record is derived-state upkeep, never replayed into a view,
    // so it is written under the folder role — the single direct writer of a
    // slot-backed store. Every attempt shares the one fenced session: a
    // transaction cannot outlive the writer it began against.
    catalog
        .with_writer(async |db| {
            for attempt in 0..MAX_COMMIT_ATTEMPTS {
                if attempt > 0 {
                    tokio::time::sleep(commit::retry_backoff(attempt)).await;
                }

                let tx = db
                    .begin(slatedb::IsolationLevel::Snapshot)
                    .await
                    .map_err(Error::from)?;
                let (status, stamp) = futures::try_join!(
                    read::read_maintenance_status(ReadHandle::Tx(&tx)),
                    commit::format_stamp_to(
                        &tx,
                        catalog.projections(),
                        FORMAT_WITH_MAINTENANCE_STATUS
                    ),
                )?;
                let mut status = status.unwrap_or_default();
                status.passes.push(encoded.clone());
                if status.passes.len() > RETAINED_PASSES {
                    let remove = status.passes.len() - RETAINED_PASSES;
                    status.passes.drain(..remove);
                }

                let mut writes: Vec<StagedWrite> = vec![(
                    Key::Sys(SysKey::MaintenanceStatus).encode(),
                    Some(value::encode_value(&status)),
                )];
                writes.extend(stamp);
                let staged = commit::stage_writes(&tx, &writes)?;

                match commit::commit_durable(db, tx, "maintenance status", staged).await {
                    Ok(_) => return Ok(()),
                    Err(error) if error.kind() == ErrorKind::Transaction => {}
                    Err(error) => return Err(Error::from(error)),
                }
            }

            Err(Error::RetryBudgetExhausted(format!(
                "maintenance status did not settle in {MAX_COMMIT_ATTEMPTS} attempts"
            )))
        })
        .await
}

fn encode_pass(pass: MaintenanceStatusPass) -> MaintenanceStatusPassValue {
    MaintenanceStatusPassValue {
        started_at_micros: pass.started_at.as_micros(),
        trigger: pass.trigger,
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
    MaintenanceStatusPass::new(
        Timestamp::from_micros(pass.started_at_micros),
        pass.trigger,
        pass.steps
            .into_iter()
            .map(|step| MaintenanceStatusStep::new(step.step, step.status, step.detail))
            .collect(),
    )
}
