//! Arrow IPC serialization for inlined-data chunks.
//!
//! The C++ shim converts a DuckDB `DataChunk` to the Arrow C Data
//! Interface (`ArrowArray`/`ArrowSchema`) using DuckDB's own converter,
//! then hands those structs here; this module serializes them to Arrow
//! IPC bytes with `arrow-rs` (the layer DuckDB's C++ lacks). Decoding
//! reverses it: IPC bytes become C Data Interface structs the shim feeds
//! back to DuckDB's Arrow import.
//!
//! Ownership across the boundary is one rule in each direction. On encode,
//! this crate **consumes** the structs the shim passes (reads them out and
//! releases DuckDB's buffers) — the shim must not release them afterward.
//! On decode, this crate **produces** structs it owns via their release
//! callbacks and writes them into the shim's out-pointers — the shim (via
//! DuckDB's importer) owns calling those callbacks.

use std::{collections::HashMap, io::Cursor, ptr};

use arrow::{
    array::{Array, RecordBatch, StructArray},
    buffer::Buffer,
    datatypes::{Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi},
    ipc::{
        MetadataVersion,
        reader::{StreamReader, read_record_batch},
        root_as_message,
        writer::{
            DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
        },
    },
};
use bytes::Bytes;

use crate::{
    abi::guard,
    error::{AbiError, MoraineError, codes},
    inline::MoraineInlineChunk,
};

/// An encode failure: the input came from DuckDB in-process, so a failure
/// is an internal fault, not bad stored data.
fn encode_error(message: impl std::fmt::Display) -> AbiError {
    AbiError::new(codes::INTERNAL, format!("moraine-duckdb: arrow {message}"))
}

/// A decode failure: the bytes came from the store, so a failure means
/// they are corrupt or written by an incompatible encoder.
fn decode_error(message: impl std::fmt::Display) -> AbiError {
    AbiError::new(
        codes::CORRUPTION,
        format!("moraine-duckdb: arrow {message}"),
    )
}

/// A byte buffer owned by Rust and freed via [`moraine_arrow_bytes_free`].
#[repr(C)]
pub struct MoraineArrowBytes {
    /// Pointer to the buffer, or null on failure.
    pub data: *mut u8,
    /// Length in bytes.
    pub len: usize,
    /// Capacity, retained so the buffer can be reconstructed for freeing.
    pub cap: usize,
}

impl MoraineArrowBytes {
    pub(crate) fn from_vec(mut v: Vec<u8>) -> Self {
        v.shrink_to_fit();
        let out = Self {
            data: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        std::mem::forget(v);
        out
    }

    fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }

    /// Reclaims a buffer returned by an Arrow encoder without copying it.
    ///
    /// # Safety
    ///
    /// `self` must have been minted by [`Self::from_vec`] and not consumed.
    pub(crate) unsafe fn into_vec(self) -> Vec<u8> {
        if self.data.is_null() {
            return Vec::new();
        }
        // SAFETY: caller contract preserves the original allocation fields.
        unsafe { Vec::from_raw_parts(self.data, self.len, self.cap) }
    }
}

/// Frees a buffer returned by an encode call.
///
/// # Safety
/// `bytes` must be a value returned by a `moraine_arrow_encode_*` call and
/// not previously freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_bytes_free(bytes: MoraineArrowBytes) {
    // SAFETY: caller guarantees the fields came from `from_vec` and are
    // freed once.
    drop(unsafe { bytes.into_vec() });
}

/// Runs `attempt` under [`guard`] and writes the produced C Data
/// Interface pair into the out slots.
///
/// # Safety
///
/// `out_schema`/`out_array` are valid writable slots and `err` is null or
/// a valid, writable [`MoraineError`], all for the duration of this call.
unsafe fn write_ffi_result(
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineError,
    attempt: impl FnOnce() -> Result<(FFI_ArrowArray, FFI_ArrowSchema), AbiError>,
) -> i32 {
    // SAFETY: `err` forwarded unchanged under this function's contract.
    match unsafe { guard(err, attempt) } {
        Ok((ffi_array, ffi_schema)) => {
            // SAFETY: `out_*` are valid writable slots per this function's
            // contract.
            unsafe {
                ptr::write(out_array, ffi_array);
                ptr::write(out_schema, ffi_schema);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Runs `attempt` under [`guard`] and writes the produced buffer (or the
/// empty buffer on failure) into `out`.
///
/// # Safety
///
/// `out` is a valid writable slot and `err` is null or a valid, writable
/// [`MoraineError`], both for the duration of this call.
unsafe fn write_bytes_result(
    out: *mut MoraineArrowBytes,
    err: *mut MoraineError,
    attempt: impl FnOnce() -> Result<Vec<u8>, AbiError>,
) -> i32 {
    // SAFETY: `err` forwarded unchanged under this function's contract.
    match unsafe { guard(err, attempt) } {
        Ok(buf) => {
            // SAFETY: `out` is a valid writable slot per this function's
            // contract.
            unsafe { ptr::write(out, MoraineArrowBytes::from_vec(buf)) };
            codes::OK
        }
        Err(code) => {
            // SAFETY: as above.
            unsafe { ptr::write(out, MoraineArrowBytes::empty()) };
            code
        }
    }
}

/// Serializes just the Arrow schema (an IPC stream with the schema and no
/// batches), stored once per inline schema version so an empty scan can
/// reconstruct the column layout.
///
/// # Safety
/// `schema` is an exported `ArrowSchema` consumed by this call; `out`/`err`
/// are valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_encode_schema(
    schema: *mut FFI_ArrowSchema,
    out: *mut MoraineArrowBytes,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<u8>, AbiError> {
        // SAFETY: caller cedes ownership of the exported schema.
        let schema = unsafe { consume_schema(schema) }?;
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| encode_error(format!("writer: {e}")))?;
            writer
                .finish()
                .map_err(|e| encode_error(format!("finish: {e}")))?;
        }
        Ok(buf)
    };
    // SAFETY: out/err validity is this function's own safety contract.
    unsafe { write_bytes_result(out, err, attempt) }
}

/// Serializes one inlined chunk to a record-batch **body** only — the IPC
/// record-batch message and its buffers, without a schema message. The
/// schema is stored once per version by [`moraine_arrow_encode_schema`] and
/// supplied back at decode by [`moraine_arrow_decode_body`], so the WAL
/// append for a tiny commit never re-serializes the schema. The layout is a
/// little-endian `u32` message length, the record-batch flatbuffer message,
/// then the arrow data buffers.
///
/// Dictionary-encoded columns are rejected: the body carries no dictionary
/// messages. Inlined user columns are not dictionary-encoded in practice.
///
/// # Safety
/// `schema`/`array` are exported structs consumed by this call; `out`/`err`
/// are valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_encode_chunk(
    schema: *mut FFI_ArrowSchema,
    array: *mut FFI_ArrowArray,
    out: *mut MoraineArrowBytes,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<u8>, AbiError> {
        if schema.is_null() || array.is_null() {
            return Err(AbiError::invalid_argument("null schema or array"));
        }
        // SAFETY: caller cedes ownership of both exported structs.
        let (owned_schema, owned_array) = unsafe { (ptr::read(schema), ptr::read(array)) };
        // SAFETY: `owned_array` is a valid exported array matching `owned_schema`.
        let data = unsafe { from_ffi(owned_array, &owned_schema) }
            .map_err(|e| encode_error(format!("array import: {e}")))?;
        drop(owned_schema);
        let batch = RecordBatch::from(StructArray::from(data));

        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let options = IpcWriteOptions::default();
        let mut context = IpcWriteContext::default();
        let (dictionaries, encoded) = generator
            .encode(&batch, &mut tracker, &options, &mut context)
            .map_err(|e| encode_error(format!("encode batch: {e}")))?;
        if !dictionaries.is_empty() {
            return Err(AbiError::invalid_argument(
                "dictionary-encoded inline columns are not supported",
            ));
        }

        let mut buf = Vec::with_capacity(4 + encoded.ipc_message.len() + encoded.arrow_data.len());
        let message_len = u32::try_from(encoded.ipc_message.len())
            .map_err(|_| encode_error("inline chunk message too large"))?;
        buf.extend_from_slice(&message_len.to_le_bytes());
        buf.extend_from_slice(&encoded.ipc_message);
        buf.extend_from_slice(&encoded.arrow_data);
        Ok(buf)
    };
    // SAFETY: out/err validity is this function's own safety contract.
    unsafe { write_bytes_result(out, err, attempt) }
}

/// Decodes a chunk body from [`moraine_arrow_encode_chunk`] against the
/// schema stored for its version (`schema_ipc` is that version's schema-only
/// IPC stream). Produces exported C Data Interface structs the shim feeds to
/// DuckDB's Arrow import; the caller (via DuckDB's importer) owns releasing
/// them.
///
/// # Safety
/// `schema_ipc`/`body` point to `schema_ipc_len`/`body_len` readable bytes;
/// `out_schema`/`out_array` are writable slots the caller releases; `err` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_decode_body(
    schema_ipc: *const u8,
    schema_ipc_len: usize,
    body: *const u8,
    body_len: usize,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(FFI_ArrowArray, FFI_ArrowSchema), AbiError> {
        if schema_ipc.is_null() || body.is_null() {
            return Err(AbiError::invalid_argument("null schema or body"));
        }
        // SAFETY: caller guarantees `schema_ipc_len` bytes are readable at
        // `schema_ipc`.
        let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ipc, schema_ipc_len) };
        // SAFETY: caller guarantees `body_len` bytes are readable at `body`.
        let body = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(body, body_len) });
        decode_inline_body(schema_bytes, body)
    };
    // SAFETY: out/err validity is this function's own safety contract.
    unsafe { write_ffi_result(out_schema, out_array, err, attempt) }
}

fn decode_inline_body(
    schema_bytes: &[u8],
    body: Bytes,
) -> Result<(FFI_ArrowArray, FFI_ArrowSchema), AbiError> {
    let schema = StreamReader::try_new(Cursor::new(schema_bytes), None)
        .map_err(|e| decode_error(format!("schema reader: {e}")))?
        .schema();

    if body.len() < 4 {
        return Err(decode_error("inline chunk body truncated"));
    }
    let len_bytes: [u8; 4] = body[0..4]
        .try_into()
        .map_err(|_| decode_error("inline chunk length prefix"))?;
    let message_len = u32::from_le_bytes(len_bytes) as usize;
    let message_end = 4 + message_len;
    if message_end > body.len() {
        return Err(decode_error("inline chunk body truncated"));
    }
    let message = root_as_message(&body[4..message_end])
        .map_err(|e| decode_error(format!("message parse: {e}")))?;
    let record_batch = message
        .header_as_record_batch()
        .ok_or_else(|| decode_error("inline chunk body is not a record batch"))?;
    let version: MetadataVersion = message.version();
    let buffer = Buffer::from(body.slice(message_end..));

    let batch = read_record_batch(
        &buffer,
        record_batch,
        schema,
        &HashMap::new(),
        None,
        &version,
    )
    .map_err(|e| decode_error(format!("read batch: {e}")))?;
    drop(body);
    to_ffi(&StructArray::from(batch).to_data())
        .map_err(|e| decode_error(format!("array export: {e}")))
}

/// Decodes and consumes one chunk returned by
/// [`crate::inline::moraine_inline_scan`]. Its store-backed allocation becomes
/// Arrow's data buffer instead of being copied across the ABI first.
///
/// # Safety
///
/// `schema_ipc` points to `schema_ipc_len` readable bytes. `chunk` points to
/// one unconsumed [`MoraineInlineChunk`]. `out_schema`/`out_array` are writable
/// slots the caller releases; `err` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_decode_inline_chunk(
    schema_ipc: *const u8,
    schema_ipc_len: usize,
    chunk: *mut MoraineInlineChunk,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(FFI_ArrowArray, FFI_ArrowSchema), AbiError> {
        if schema_ipc.is_null() || chunk.is_null() {
            return Err(AbiError::invalid_argument("null schema or inline chunk"));
        }
        // SAFETY: caller guarantees the schema slice is readable.
        let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ipc, schema_ipc_len) };
        // SAFETY: caller guarantees `chunk` is one live, unconsumed value.
        let body = unsafe { (&mut *chunk).take_bytes() }?;
        decode_inline_body(schema_bytes, body)
    };
    // SAFETY: out/err validity is this function's own safety contract.
    unsafe { write_ffi_result(out_schema, out_array, err, attempt) }
}

/// Decodes a self-contained IPC stream (from [`moraine_arrow_encode_chunk`],
/// or a schema-only stream from [`moraine_arrow_encode_schema`], which
/// yields an empty batch) into exported C Data Interface structs the shim
/// feeds to DuckDB's Arrow import.
///
/// # Safety
/// `body` points to `body_len` readable bytes; `out_schema`/`out_array` are
/// writable slots the caller (DuckDB) will release; `err` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_decode_stream(
    body: *const u8,
    body_len: usize,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(FFI_ArrowArray, FFI_ArrowSchema), AbiError> {
        if body.is_null() {
            return Err(AbiError::invalid_argument("null body"));
        }
        // SAFETY: caller guarantees `body_len` readable bytes at `body`.
        let bytes = unsafe { std::slice::from_raw_parts(body, body_len) };
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|e| decode_error(format!("reader: {e}")))?;
        let schema = reader.schema();
        let batch = match reader.next() {
            Some(b) => b.map_err(|e| decode_error(format!("read batch: {e}")))?,
            None => RecordBatch::new_empty(schema),
        };
        to_ffi(&StructArray::from(batch).to_data())
            .map_err(|e| decode_error(format!("array export: {e}")))
    };
    // SAFETY: out/err validity is this function's own safety contract.
    unsafe { write_ffi_result(out_schema, out_array, err, attempt) }
}

/// Reads and takes ownership of an exported schema struct.
///
/// # Safety
/// `schema` points to a valid exported `ArrowSchema` whose ownership the
/// caller cedes to this call.
unsafe fn consume_schema(schema: *mut FFI_ArrowSchema) -> Result<SchemaRef, AbiError> {
    if schema.is_null() {
        return Err(AbiError::invalid_argument("null schema"));
    }
    // SAFETY: caller cedes ownership of the exported struct.
    let owned = unsafe { ptr::read(schema) };
    let schema = Schema::try_from(&owned).map_err(|e| encode_error(format!("schema import: {e}")));
    drop(owned);
    Ok(SchemaRef::new(schema?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, ListArray, StringArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };

    use super::*;

    #[test]
    fn encoded_buffer_moves_back_into_rust_without_copying() {
        let encoded = MoraineArrowBytes::from_vec(vec![1, 2, 3, 4]);
        let data = encoded.data;
        // SAFETY: `encoded` was just minted above and is consumed once.
        let reclaimed = unsafe { encoded.into_vec() };

        assert_eq!(reclaimed.as_ptr().cast_mut(), data);
        assert_eq!(reclaimed, vec![1, 2, 3, 4]);
    }

    /// Encodes a struct array's schema and body separately, then decodes the
    /// body against that stored schema — the exact FFI round trip the C++ shim
    /// drives (schema once per version, body per chunk). The returned structs
    /// are Rust-owned and release on drop.
    fn encode_then_decode(source: &StructArray) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let mut err = MoraineError::default();

        // Schema-only stream (stored once per version as `inline/schema`).
        let batch = RecordBatch::from(source.clone());
        let mut schema_ffi =
            FFI_ArrowSchema::try_from(batch.schema().as_ref()).expect("export schema");
        let mut schema_bytes = MoraineArrowBytes::empty();
        // SAFETY: `schema_ffi` is a valid exported schema consumed by the call;
        // out-pointers are valid. Forget our copy after to avoid a double free.
        let code = unsafe {
            moraine_arrow_encode_schema(&raw mut schema_ffi, &raw mut schema_bytes, &raw mut err)
        };
        std::mem::forget(schema_ffi);
        assert_eq!(code, 0, "encode schema failed");

        // Body-only chunk.
        let (mut in_array, mut in_schema) = to_ffi(&source.to_data()).expect("export source");
        let mut body_bytes = MoraineArrowBytes::empty();
        // SAFETY: the exported structs and out-pointers are valid; encode
        // consumes the structs, so we `forget` our copies below.
        let code = unsafe {
            moraine_arrow_encode_chunk(
                &raw mut in_schema,
                &raw mut in_array,
                &raw mut body_bytes,
                &raw mut err,
            )
        };
        std::mem::forget(in_array);
        std::mem::forget(in_schema);
        assert_eq!(code, 0, "encode chunk failed");
        // SAFETY: the encoder minted this buffer and this call consumes it.
        let body = unsafe { body_bytes.into_vec() };
        let mut chunk = MoraineInlineChunk::from_bytes(Bytes::from(body));

        let mut out_schema = FFI_ArrowSchema::empty();
        let mut out_array = FFI_ArrowArray::empty();
        // SAFETY: the schema buffer and owned chunk are valid; the
        // out-pointers are writable slots this call fills with owned structs.
        let code = unsafe {
            moraine_arrow_decode_inline_chunk(
                schema_bytes.data,
                schema_bytes.len,
                &raw mut chunk,
                &raw mut out_schema,
                &raw mut out_array,
                &raw mut err,
            )
        };
        assert_eq!(code, 0, "decode body failed");
        // SAFETY: the schema buffer came from the encode call above and is
        // freed once. The body now belongs to `out_array`.
        unsafe { moraine_arrow_bytes_free(schema_bytes) };
        (out_array, out_schema)
    }

    fn round_trip(batch: &RecordBatch) -> RecordBatch {
        let (out_array, out_schema) = encode_then_decode(&StructArray::from(batch.clone()));
        // SAFETY: the decoded structs are valid and owned; `from_ffi` consumes
        // them, taking over their release.
        let data = unsafe { from_ffi(out_array, &out_schema) }.expect("import decoded");
        RecordBatch::from(StructArray::from(data))
    }

    #[test]
    fn scalar_columns_round_trip_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap();
        assert_eq!(round_trip(&batch), batch);
    }

    #[test]
    fn decoded_children_report_null_count() {
        // DuckDB's arrow importer skips a column's validity buffer unless the
        // C-Data-Interface array reports `null_count != 0`. arrow-rs's own
        // `from_ffi` ignores `null_count` (it always reads the buffer), so a
        // dropped child `null_count` is invisible to the other round-trip
        // tests but silently strips every null on the DuckDB side.
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let source = StructArray::from(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                    Arc::new(StringArray::from(vec![Some("a"), None, None])),
                ],
            )
            .unwrap(),
        );
        let (out_array, _out_schema) = encode_then_decode(&source);
        assert_eq!(out_array.child(0).null_count(), 1, "int column null_count");
        assert_eq!(
            out_array.child(1).null_count(),
            2,
            "string column null_count"
        );
    }

    #[test]
    fn nested_list_column_round_trips() {
        let values = Int64Array::from(vec![10, 20, 30, 40]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 4].into());
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        let list = ListArray::new(field, offsets, Arc::new(values), None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        assert_eq!(round_trip(&batch), batch);
    }
}
