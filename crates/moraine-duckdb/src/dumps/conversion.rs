//! Conversion and ownership of metadata rows crossing the C ABI.

use crate::error::AbiError;

/// A C row with a matching record converter and resource release operation.
///
/// # Safety
///
/// `convert` must return independently owned rows and clean up on failure.
/// `release` must free their resources without unwinding.
pub(crate) unsafe trait DumpRow: Sized {
    type Record;

    fn convert(record: Self::Record) -> Result<Self, AbiError>;

    /// Releases this row's resources.
    ///
    /// # Safety
    ///
    /// The row must come from `convert` and must not have been released.
    unsafe fn release(&mut self);
}

pub(crate) fn convert_rows<Row: DumpRow>(records: Vec<Row::Record>) -> Result<Vec<Row>, AbiError> {
    let mut pending = PendingRows(Vec::with_capacity(records.len()));
    for record in records {
        pending.0.push(Row::convert(record)?);
    }
    Ok(std::mem::take(&mut pending.0))
}

/// Owns converted rows until the complete batch can be handed to the caller.
struct PendingRows<Row: DumpRow>(Vec<Row>);

impl<Row: DumpRow> Drop for PendingRows<Row> {
    fn drop(&mut self) {
        for row in &mut self.0 {
            // SAFETY: these rows were converted successfully and are released once.
            unsafe { row.release() };
        }
    }
}

/// Defines conversion and cleanup from the same field mapping. C structs and
/// exported functions stay explicit so cbindgen can read them without
/// expansion.
macro_rules! dump_row {
    ($record:ty => $row:ty {
        values { $($value:ident),* $(,)? }
        optional { $($optional:ident => $present:ident),* $(,)? }
        strings { $($string:ident),* $(,)? }
        optional_strings { $($optional_string:ident),* $(,)? }
    }) => {
        // SAFETY: all strings are validated before transferring ownership;
        // release frees exactly those strings and cannot unwind.
        unsafe impl $crate::dumps::conversion::DumpRow for $row {
            type Record = $record;

            fn convert(record: Self::Record) -> Result<Self, $crate::error::AbiError> {
                $(let $string = $crate::abi::to_c_string(record.$string)?;)*
                $(let $optional_string = record.$optional_string
                    .map($crate::abi::to_c_string).transpose()?;)*
                Ok(Self {
                    $($value: record.$value,)*
                    $($present: record.$optional.is_some(),
                      $optional: record.$optional.unwrap_or_default(),)*
                    $($string: $string.into_raw(),)*
                    $($optional_string: $optional_string
                        .map_or(std::ptr::null_mut(), std::ffi::CString::into_raw),)*
                })
            }

            unsafe fn release(&mut self) {
                // SAFETY: caller supplies a converted row not previously released.
                $(unsafe { $crate::abi::free_c_string(self.$string) };)*
                // SAFETY: absent strings are null; present strings belong to this row.
                $(unsafe { $crate::abi::free_c_string(self.$optional_string) };)*
            }
        }
    };
}

pub(super) use dump_row;

#[cfg(test)]
mod tests;
