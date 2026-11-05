//! Admission for WAL rows retained by read-only SlateDB memtables.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{
    ValueDeletable,
    wal::{WalError, WalFileRange, WalIterator, WalReader, WalRows},
};

use crate::MemoryReservation;

pub(crate) struct AdmittedReader {
    inner: Arc<dyn WalReader>,
    memory: Arc<MemoryReservation>,
}

impl AdmittedReader {
    pub(crate) fn new(inner: Arc<dyn WalReader>, memory: MemoryReservation) -> Self {
        Self {
            inner,
            memory: Arc::new(memory),
        }
    }
}

#[async_trait]
impl WalReader for AdmittedReader {
    async fn iterator(&self, range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        Ok(Box::new(AdmittedIterator {
            inner: self.inner.iterator(range).await?,
            memory: self.memory.clone(),
        }))
    }

    async fn last_wal_file_id(&self, after: u64) -> Result<u64, WalError> {
        self.inner.last_wal_file_id(after).await
    }
}

struct AdmittedIterator {
    inner: Box<dyn WalIterator>,
    memory: Arc<MemoryReservation>,
}

#[async_trait]
impl WalIterator for AdmittedIterator {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        let Some(mut batch) = self.inner.next().await? else {
            return Ok(None);
        };
        let payload = batch
            .rows
            .iter()
            .try_fold(0usize, |bytes, row| {
                bytes
                    .checked_add(row.key.len())
                    .and_then(|bytes| bytes.checked_add(row.value.len()))
            })
            .ok_or_else(|| {
                WalError::InternalError(Arc::new(crate::Error::Constraint(
                    "WAL replay batch size overflow".into(),
                )))
            })?;
        let bytes = (payload as u64)
            .saturating_mul(2)
            .saturating_add((batch.rows.len() as u64).saturating_mul(320));
        let memory = self
            .memory
            .independent(bytes)
            .map_err(|error| WalError::InternalError(Arc::new(error)))?;
        // Compact slices so a small surviving row cannot pin an uncharged WAL block.
        let mut packed = Vec::with_capacity(payload);
        for row in &batch.rows {
            packed.extend_from_slice(&row.key);
            if let Some(value) = row.value.as_bytes() {
                packed.extend_from_slice(&value);
            }
        }
        let packed = memory
            .into_bytes(Bytes::from(packed))
            .map_err(|error| WalError::InternalError(Arc::new(error)))?;
        let mut offset = 0;
        for row in &mut batch.rows {
            let end = offset + row.key.len();
            row.key = packed.slice(offset..end);
            offset = end;
            match &mut row.value {
                ValueDeletable::Value(value) | ValueDeletable::Merge(value) => {
                    let end = offset + value.len();
                    *value = packed.slice(offset..end);
                    offset = end;
                }
                ValueDeletable::Tombstone => {}
            }
        }
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests;
