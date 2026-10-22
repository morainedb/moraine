//! A bounded resident queue whose overflow lives in an anonymous temporary
//! file.

use std::{collections::VecDeque, io::SeekFrom};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::publication::Position;
use crate::{Error, Result};

const RESIDENT_FILES: usize = 1024;

#[derive(Default)]
pub(super) struct RetryQueue {
    resident: VecDeque<Position>,
    spill: Option<tokio::fs::File>,
    read: u64,
    written: u64,
}

fn storage_error(error: &std::io::Error) -> Error {
    Error::Configuration(format!("row summary retry spool: {error}"))
}

impl RetryQueue {
    pub(super) fn is_empty(&self) -> bool {
        self.resident.is_empty() && self.read == self.written
    }

    #[cfg(test)]
    pub(super) fn resident_len(&self) -> usize {
        self.resident.len()
    }

    pub(super) async fn push(&mut self, position: Position) -> Result<()> {
        if self.spill.is_none() && self.resident.len() < RESIDENT_FILES {
            self.resident.push_back(position);
            return Ok(());
        }
        if self.spill.is_none() {
            self.spill = Some(tokio::fs::File::from_std(
                tempfile::tempfile().map_err(|error| storage_error(&error))?,
            ));
        }
        if let Some(file) = &mut self.spill {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&position.0.to_le_bytes());
            bytes[8..].copy_from_slice(&position.1.to_le_bytes());
            file.seek(SeekFrom::Start(self.written))
                .await
                .map_err(|error| storage_error(&error))?;
            file.write_all(&bytes)
                .await
                .map_err(|error| storage_error(&error))?;
            // Advance only after the write: cancellation can safely overwrite
            // an unfinished record at this offset.
            self.written += 16;
        }
        Ok(())
    }

    pub(super) async fn peek(&mut self, count: usize) -> Result<Vec<Position>> {
        let mut selected: Vec<_> = self.resident.iter().take(count).copied().collect();
        if let Some(file) = &mut self.spill {
            file.seek(SeekFrom::Start(self.read))
                .await
                .map_err(|error| storage_error(&error))?;
            let available = (self.written - self.read) / 16;
            for _ in 0..available.min((count - selected.len()) as u64) {
                let mut bytes = [0; 16];
                file.read_exact(&mut bytes)
                    .await
                    .map_err(|error| storage_error(&error))?;
                let mut table = [0; 8];
                let mut data_file = [0; 8];
                table.copy_from_slice(&bytes[..8]);
                data_file.copy_from_slice(&bytes[8..]);
                selected.push((u64::from_le_bytes(table), u64::from_le_bytes(data_file)));
            }
        }
        Ok(selected)
    }

    pub(super) fn acknowledge(&mut self, count: usize) {
        let resident = count.min(self.resident.len());
        self.resident.drain(..resident);
        self.read += ((count - resident) as u64) * 16;
    }
}
