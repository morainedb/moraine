//! Enables durable orphan-deletion reservations without rewriting file records.

use futures::{FutureExt, future::BoxFuture};

use super::{MigrationUnit, StepOutcome};
use crate::{Error, Result, transaction::session::CommitSession};

pub(super) const MIGRATION: MigrationUnit = MigrationUnit {
    name: "enable-orphan-deletion-reservations",
    from_format: 12,
    to_format: 13,
    step,
};

fn step<'a>(_session: &'a CommitSession, cursor: &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>> {
    async move {
        if !cursor.is_empty() {
            return Err(Error::Corruption(
                "orphan-deletion migration has no cursor".into(),
            ));
        }
        Ok(None)
    }
    .boxed()
}
