//! Admission for authored rows, before translation allocates a write batch.

use super::{Cell, RowOperation};
use crate::{Error, Result};

const DEFAULT_LIMIT: usize = 256 * 1024 * 1024;

pub(super) struct StagingMemory {
    limit: usize,
    payload: usize,
    failure: Option<String>,
}

impl Default for StagingMemory {
    fn default() -> Self {
        match std::env::var("MORAINE_STAGING_MEMORY_BYTES") {
            Ok(value) => match value.parse::<usize>() {
                Ok(limit) if limit > 0 => Self::with_limit(limit),
                _ => Self {
                    failure: Some(
                        "MORAINE_STAGING_MEMORY_BYTES must be a positive byte count".into(),
                    ),
                    ..Self::with_limit(DEFAULT_LIMIT)
                },
            },
            Err(std::env::VarError::NotPresent) => Self::with_limit(DEFAULT_LIMIT),
            Err(_) => Self {
                failure: Some("MORAINE_STAGING_MEMORY_BYTES must be UTF-8".into()),
                ..Self::with_limit(DEFAULT_LIMIT)
            },
        }
    }
}

impl StagingMemory {
    pub(super) fn with_limit(limit: usize) -> Self {
        Self {
            limit,
            payload: 0,
            failure: None,
        }
    }

    pub(super) fn check(&self) -> Result<()> {
        match &self.failure {
            Some(message) => Err(Error::Constraint(message.clone())),
            None => Ok(()),
        }
    }

    pub(super) fn push(&mut self, ops: &mut Vec<RowOperation>, op: RowOperation) -> Result<()> {
        self.check()?;
        let payload = self.payload.saturating_add(payload_bytes(&op));
        let capacity = if ops.len() == ops.capacity() {
            ops.capacity().saturating_mul(2).max(4)
        } else {
            ops.capacity()
        };
        let bytes = capacity
            .saturating_mul(size_of::<RowOperation>())
            .saturating_add(payload);
        if bytes > self.limit {
            self.failure = Some(format!(
                "staging memory exceeds {} bytes; split the transaction or raise MORAINE_STAGING_MEMORY_BYTES",
                self.limit
            ));
        } else if ops.try_reserve_exact(capacity - ops.len()).is_err() {
            self.failure = Some("staging memory allocation failed".into());
        }
        if self.failure.is_some() {
            *ops = Vec::new();
            self.payload = 0;
            return self.check();
        }
        ops.push(op);
        self.payload = payload;
        Ok(())
    }
}

fn payload_bytes(op: &RowOperation) -> usize {
    match op {
        RowOperation::Insert { cells, .. }
        | RowOperation::Delete { cells, .. }
        | RowOperation::UpdateSetEnd { cells, .. }
        | RowOperation::UpdateSetBegin { cells, .. } => cells.iter().fold(
            cells.capacity().saturating_mul(size_of::<Cell>()),
            |bytes, cell| {
                bytes.saturating_add(match cell {
                    Cell::Str(value) => value.capacity(),
                    _ => 0,
                })
            },
        ),
        RowOperation::InlineSchema { arrow_schema, .. } => arrow_schema.capacity(),
        RowOperation::InlineInsert { arrow_body, .. } => arrow_body.len(),
        _ => 0,
    }
}
