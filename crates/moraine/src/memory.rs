//! Admission for live working allocations, released by their owners.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::{
    Error, Result,
    store::cache::retention::{Budget, Charge},
};

fn working_budget() -> Result<Arc<Budget>> {
    static BUDGET: OnceLock<std::result::Result<Arc<Budget>, String>> = OnceLock::new();
    BUDGET
        .get_or_init(|| {
            let capacity = match std::env::var("MORAINE_WORKING_MEMORY_BYTES") {
                Ok(value) => value
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| {
                        "MORAINE_WORKING_MEMORY_BYTES must be a positive byte count".to_owned()
                    })?,
                Err(std::env::VarError::NotPresent) => 1024 * 1024 * 1024,
                Err(_) => return Err("MORAINE_WORKING_MEMORY_BYTES must be UTF-8".into()),
            };
            Ok(Budget::new(capacity))
        })
        .clone()
        .map_err(Error::Configuration)
}

/// Process-wide admission for live Moraine buffers. Drop releases the
/// allowance. This accounts for the bytes requested by owners, not allocator
/// overhead or RSS.
#[derive(Debug)]
pub struct MemoryReservation {
    charge: Charge,
}

impl MemoryReservation {
    /// Reserves bytes under `MORAINE_WORKING_MEMORY_BYTES` (default 1 GiB).
    ///
    /// ```
    /// let reservation = moraine::MemoryReservation::reserve(3)?;
    /// let bytes = reservation.into_bytes(bytes::Bytes::from_static(b"abc"))?;
    /// let held = bytes.slice(1..);
    /// drop(bytes); // The slice keeps the reservation alive.
    /// assert_eq!(&held[..], b"bc");
    /// # Ok::<(), moraine::Error>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error for invalid configuration or exhausted working memory.
    pub fn reserve(bytes: u64) -> Result<Self> {
        Self::from_budget(bytes, working_budget()?)
    }

    pub(crate) fn from_budget(bytes: u64, budget: Arc<Budget>) -> Result<Self> {
        let mut reservation = Self {
            charge: Charge::new(budget),
        };
        reservation.resize(bytes)?;
        Ok(reservation)
    }

    pub(crate) fn independent(&self, bytes: u64) -> Result<Self> {
        Self::from_budget(bytes, self.charge.budget())
    }

    /// Changes the reservation, leaving it unchanged if growth is refused.
    ///
    /// # Errors
    /// Returns an error when the requested allowance is unavailable.
    pub fn resize(&mut self, bytes: u64) -> Result<()> {
        if self.charge.resize(bytes) {
            Ok(())
        } else {
            Err(Error::Constraint(format!(
                "working memory cannot admit {bytes} bytes; release held results, split the operation, or raise MORAINE_WORKING_MEMORY_BYTES"
            )))
        }
    }

    /// Bytes currently reserved by this owner.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.charge.bytes()
    }

    /// Transfers admission to a byte buffer, including every clone and slice.
    ///
    /// # Errors
    /// Returns an error if the buffer requires more bytes than can be reserved.
    pub fn into_bytes(mut self, bytes: Bytes) -> Result<Bytes> {
        self.resize(self.bytes().max(bytes.len() as u64))?;
        Ok(Self::shared_bytes(Arc::new(self), bytes))
    }

    pub(crate) fn shared_bytes(reservation: Arc<Self>, bytes: Bytes) -> Bytes {
        struct Owner {
            bytes: Bytes,
            _reservation: Arc<MemoryReservation>,
        }
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        Bytes::from_owner(Owner {
            bytes,
            _reservation: reservation,
        })
    }
}

/// Reserved live bytes and the process-wide admission allowance.
///
/// # Errors
/// Returns an error for invalid working-memory configuration.
pub fn working_memory_usage() -> Result<(u64, u64)> {
    let budget = working_budget()?;
    Ok((budget.used(), budget.capacity()))
}

/// Reads a cache record incrementally, admitting vector growth before
/// allocation.
pub(crate) fn read_bytes(reader: &mut impl std::io::Read) -> std::io::Result<Bytes> {
    let mut memory = MemoryReservation::reserve(0).map_err(std::io::Error::other)?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let needed = bytes.len().saturating_add(count);
        if needed > bytes.capacity() {
            let capacity = needed.max(bytes.capacity().saturating_mul(2));
            memory
                .resize((capacity as u64).saturating_mul(2))
                .map_err(std::io::Error::other)?;
            bytes.reserve_exact(capacity - bytes.len());
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    memory
        .into_bytes(bytes.into())
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::cache::retention::Budget;

    #[test]
    fn byte_slices_retain_admission_until_the_last_owner_drops() {
        let budget = Budget::new(16);
        let reservation = MemoryReservation::from_budget(16, budget.clone()).unwrap();
        let bytes = reservation
            .into_bytes(bytes::Bytes::from(vec![1; 16]))
            .unwrap();
        let held = bytes.slice(0..1);
        drop(bytes);
        assert_eq!(budget.used(), 16);
        assert!(MemoryReservation::from_budget(1, budget.clone()).is_err());
        drop(held);
        assert_eq!(budget.used(), 0);
        assert!(MemoryReservation::from_budget(16, budget).is_ok());
    }

    #[test]
    fn refused_growth_keeps_the_existing_reservation() {
        let budget = Budget::new(16);
        let mut reservation = MemoryReservation::from_budget(8, budget.clone()).unwrap();
        assert!(reservation.resize(17).is_err());
        assert_eq!(budget.used(), 8);
        reservation.resize(4).unwrap();
        assert_eq!(budget.used(), 4);
    }
}

#[cfg(test)]
mod arrow_tests {
    use arrow::array::{Array, Int64Array, RecordBatch};

    use super::*;

    #[test]
    fn retained_arrow_slices_keep_the_original_allocation_reserved() {
        let budget = Budget::new(1024);
        let batch =
            RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1, 2, 3])) as _)])
                .unwrap();
        let bytes = batch.get_array_memory_size() as u64;
        let reservation = MemoryReservation::from_budget(bytes, budget.clone()).unwrap();
        let batch = reservation.into_record_batch(batch).unwrap();
        let slice = batch.column(0).slice(1, 1);
        drop(batch);
        assert_eq!(budget.used(), bytes);
        assert_eq!(
            slice
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        drop(slice);
        assert_eq!(budget.used(), 0);
    }
}

impl MemoryReservation {
    /// Attaches admission to every Arrow buffer, including nested arrays and
    /// null masks. Slices and Arrow C exports retain the allowance.
    ///
    /// # Errors
    /// Returns an error if growth is refused or an array is invalid.
    pub fn into_record_batch(
        mut self,
        batch: arrow::array::RecordBatch,
    ) -> Result<arrow::array::RecordBatch> {
        use arrow::{
            array::{ArrayData, make_array},
            buffer::{BooleanBuffer, Buffer, NullBuffer},
        };
        fn retain(data: ArrayData, memory: &Arc<MemoryReservation>) -> Result<ArrayData> {
            fn buffer(buffer: &Buffer, memory: &Arc<MemoryReservation>) -> Buffer {
                struct Owner {
                    buffer: Buffer,
                    _memory: Arc<MemoryReservation>,
                }
                impl AsRef<[u8]> for Owner {
                    fn as_ref(&self) -> &[u8] {
                        self.buffer.as_slice()
                    }
                }
                Buffer::from(Bytes::from_owner(Owner {
                    buffer: buffer.clone(),
                    _memory: memory.clone(),
                }))
            }
            let buffers = data
                .buffers()
                .iter()
                .map(|value| buffer(value, memory))
                .collect();
            let children = data
                .child_data()
                .iter()
                .cloned()
                .map(|child| retain(child, memory))
                .collect::<Result<Vec<_>>>()?;
            let nulls = data.nulls().map(|nulls| {
                let bits = nulls.inner();
                NullBuffer::new(BooleanBuffer::new(
                    buffer(bits.inner(), memory),
                    bits.offset(),
                    bits.len(),
                ))
            });
            data.into_builder()
                .buffers(buffers)
                .child_data(children)
                .nulls(nulls)
                .build()
                .map_err(|error| Error::Corruption(format!("retaining Arrow memory: {error}")))
        }
        self.resize(self.bytes().max(batch.get_array_memory_size() as u64))?;
        let memory = Arc::new(self);
        let arrays = batch
            .columns()
            .iter()
            .map(|array| retain(array.to_data(), &memory).map(make_array))
            .collect::<Result<Vec<_>>>()?;
        let schema = batch.schema();
        let rows = batch.num_rows();
        drop(batch);
        arrow::array::RecordBatch::try_new_with_options(
            schema,
            arrays,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|error| Error::Corruption(format!("retaining Arrow batch: {error}")))
    }
}

pub(crate) fn positive_bytes(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| Error::Configuration(format!("{name} must be a positive byte count"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(Error::Configuration(format!("{name} must be UTF-8"))),
    }
}

pub(crate) fn store_memory_limit() -> Result<u64> {
    positive_bytes("MORAINE_STORE_MEMORY_BYTES", 1024 * 1024 * 1024)
}

/// Checks an optional host envelope before attaching to DuckDB. Set
/// `MORAINE_HOST_MEMORY_BYTES` to enable it; one quarter is left for native
/// allocations unless `MORAINE_HOST_HEADROOM_BYTES` overrides that allowance.
///
/// `other_bytes` must include cache capacity and every store's write buffers.
/// The caller must revalidate after increasing DuckDB's memory limit.
///
/// # Errors
/// Returns an error for invalid configuration or overlapping allowances.
pub fn validate_host_memory(duckdb_bytes: u64, other_bytes: u64) -> Result<()> {
    if std::env::var_os("MORAINE_HOST_MEMORY_BYTES").is_none() {
        return Ok(());
    }
    let host = positive_bytes("MORAINE_HOST_MEMORY_BYTES", 1)?;
    let headroom = positive_bytes("MORAINE_HOST_HEADROOM_BYTES", (host / 4).max(1))?;
    let (_, working) = working_memory_usage()?;
    check_host_envelope(host, headroom, duckdb_bytes, working, other_bytes)
}

fn check_host_envelope(
    host: u64,
    headroom: u64,
    duckdb: u64,
    working: u64,
    other: u64,
) -> Result<()> {
    let required = headroom
        .checked_add(duckdb)
        .and_then(|bytes| bytes.checked_add(working))
        .and_then(|bytes| bytes.checked_add(other));
    if required.is_some_and(|required| required <= host) {
        return Ok(());
    }
    Err(Error::Configuration(format!(
        "host memory admission refused: host={host}, headroom={headroom}, DuckDB={duckdb}, Moraine working={working}, cache/store={other}; lower memory_limit or Moraine allowances before attaching"
    )))
}

#[cfg(test)]
mod host_tests {
    use super::*;
    #[test]
    fn host_admission_preserves_headroom_and_refuses_overflow() {
        assert!(check_host_envelope(100, 25, 40, 20, 15).is_ok());
        assert!(check_host_envelope(100, 25, 41, 20, 15).is_err());
        assert!(check_host_envelope(u64::MAX, 1, u64::MAX, 1, 1).is_err());
    }
}
