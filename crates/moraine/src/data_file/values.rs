//! One Arrow array cell as a canonical index value. The borrowed form
//! leaves strings and blobs in place while their escaped bytes are
//! appended; the owned form is the same mapping, materialized.

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};

use crate::{
    error::{Error, Result},
    store::index_encoding::{BorrowedIndexKeyValue, IndexKeyValue, IntWidth},
};

fn downcast<A: 'static>(array: &dyn Array) -> Result<&A> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Corruption("scoped read: parquet column type mismatch".to_owned()))
}

/// The canonical value of `array` at `row`, or `None` for NULL.
pub(super) fn array_value(array: &dyn Array, row: usize) -> Result<Option<IndexKeyValue>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let signed = |value: i128, width| IndexKeyValue::Int { value, width };
    let unsigned = |value: u128, width| IndexKeyValue::UInt { value, width };
    let value = match array.data_type() {
        DataType::Int8 => signed(
            i128::from(downcast::<Int8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::Int16 => signed(
            i128::from(downcast::<Int16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::Int32 => signed(
            i128::from(downcast::<Int32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Int64 => signed(
            i128::from(downcast::<Int64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::UInt8 => unsigned(
            u128::from(downcast::<UInt8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::UInt16 => unsigned(
            u128::from(downcast::<UInt16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::UInt32 => unsigned(
            u128::from(downcast::<UInt32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::UInt64 => unsigned(
            u128::from(downcast::<UInt64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Float32 => IndexKeyValue::F32(downcast::<Float32Array>(array)?.value(row)),
        DataType::Float64 => IndexKeyValue::F64(downcast::<Float64Array>(array)?.value(row)),
        DataType::Boolean => IndexKeyValue::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Utf8 => IndexKeyValue::Str(downcast::<StringArray>(array)?.value(row).to_owned()),
        DataType::LargeUtf8 => {
            IndexKeyValue::Str(downcast::<LargeStringArray>(array)?.value(row).to_owned())
        }
        DataType::LargeBinary => {
            IndexKeyValue::Bytes(downcast::<LargeBinaryArray>(array)?.value(row).to_vec())
        }
        DataType::Binary => {
            IndexKeyValue::Bytes(downcast::<BinaryArray>(array)?.value(row).to_vec())
        }
        // Fixed-width blobs, e.g. a 16-byte `UUID`.
        DataType::FixedSizeBinary(_) => {
            IndexKeyValue::Bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec())
        }
        // Temporal types index by their underlying integer representation.
        DataType::Date32 => signed(
            i128::from(downcast::<Date32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Date64 => signed(
            i128::from(downcast::<Date64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        // Each timestamp width indexes by its own underlying `i64` count —
        // seconds, milli-, micro-, or nanoseconds — read from the array of
        // the matching unit. The count is the same on the inline path (whose
        // schema carries the same unit), so the two paths agree.
        DataType::Timestamp(TimeUnit::Second, _) => signed(
            i128::from(downcast::<TimestampSecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => signed(
            i128::from(downcast::<TimestampMillisecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => signed(
            i128::from(downcast::<TimestampMicrosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => signed(
            i128::from(downcast::<TimestampNanosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        other => {
            return Err(Error::Constraint(format!(
                "scoped read: column type {other:?} is not indexable"
            )));
        }
    };
    Ok(Some(value))
}

/// Borrows one Arrow scalar for immediate canonical-key construction.
pub(super) fn borrowed_array_value(
    array: &dyn Array,
    row: usize,
) -> Result<Option<BorrowedIndexKeyValue<'_>>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let signed = |value: i128, width| BorrowedIndexKeyValue::Int { value, width };
    let unsigned = |value: u128, width| BorrowedIndexKeyValue::UInt { value, width };
    let value = match array.data_type() {
        DataType::Int8 => signed(
            i128::from(downcast::<Int8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::Int16 => signed(
            i128::from(downcast::<Int16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::Int32 => signed(
            i128::from(downcast::<Int32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Int64 => signed(
            i128::from(downcast::<Int64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::UInt8 => unsigned(
            u128::from(downcast::<UInt8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::UInt16 => unsigned(
            u128::from(downcast::<UInt16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::UInt32 => unsigned(
            u128::from(downcast::<UInt32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::UInt64 => unsigned(
            u128::from(downcast::<UInt64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Float32 => {
            BorrowedIndexKeyValue::F32(downcast::<Float32Array>(array)?.value(row))
        }
        DataType::Float64 => {
            BorrowedIndexKeyValue::F64(downcast::<Float64Array>(array)?.value(row))
        }
        DataType::Boolean => {
            BorrowedIndexKeyValue::Bool(downcast::<BooleanArray>(array)?.value(row))
        }
        DataType::Utf8 => BorrowedIndexKeyValue::Str(downcast::<StringArray>(array)?.value(row)),
        DataType::LargeUtf8 => {
            BorrowedIndexKeyValue::Str(downcast::<LargeStringArray>(array)?.value(row))
        }
        DataType::LargeBinary => {
            BorrowedIndexKeyValue::Bytes(downcast::<LargeBinaryArray>(array)?.value(row))
        }
        DataType::Binary => {
            BorrowedIndexKeyValue::Bytes(downcast::<BinaryArray>(array)?.value(row))
        }
        DataType::FixedSizeBinary(_) => {
            BorrowedIndexKeyValue::Bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row))
        }
        DataType::Date32 => signed(
            i128::from(downcast::<Date32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Date64 => signed(
            i128::from(downcast::<Date64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Second, _) => signed(
            i128::from(downcast::<TimestampSecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => signed(
            i128::from(downcast::<TimestampMillisecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => signed(
            i128::from(downcast::<TimestampMicrosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => signed(
            i128::from(downcast::<TimestampNanosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        other => {
            return Err(Error::Constraint(format!(
                "scoped read: column type {other:?} is not indexable"
            )));
        }
    };
    Ok(Some(value))
}

/// Reads a row id or row position at `row` as a `u64` (`Int64`/`UInt64`).
pub(super) fn row_id_value(array: &dyn Array, row: usize) -> Result<u64> {
    if array.is_null(row) {
        return Err(Error::Corruption(
            "scoped read: row-id column holds a NULL".to_owned(),
        ));
    }
    match array.data_type() {
        DataType::Int64 => u64::try_from(downcast::<Int64Array>(array)?.value(row))
            .map_err(|_| Error::Corruption("scoped read: negative row id".to_owned())),
        DataType::UInt64 => Ok(downcast::<UInt64Array>(array)?.value(row)),
        other => Err(Error::Corruption(format!(
            "scoped read: row-id column has non-integer type {other:?}"
        ))),
    }
}
