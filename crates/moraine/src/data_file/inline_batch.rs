//! Inline-insert chunks: the Arrow IPC decode and the entries derived from
//! it. Inline chunks carry no row-id column; ids are dense from the chunk's
//! recorded start.

use std::collections::HashSet;

use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::{
        reader::{StreamReader, read_record_batch},
        root_as_message,
    },
};
use bytes::Bytes;

use crate::{
    data_file::{
        IndexProjection, ScopedIndexEntry, ScopedReadEntry, corrupt,
        entries::{record_batch_entries, record_batch_index_entries},
        selection::Ordinals,
    },
    error::{Error, Result},
};

/// Decodes one schema-only inline IPC stream.
pub(crate) fn decode_inline_schema(schema_ipc: Bytes) -> Result<SchemaRef> {
    #[cfg(test)]
    record_inline_schema_decode(&schema_ipc);

    Ok(
        StreamReader::try_new(std::io::Cursor::new(schema_ipc), None)
            .map_err(corrupt("inline schema"))?
            .schema(),
    )
}

/// Decodes an inline-insert Arrow body — `[u32-le message length][record-
/// batch message][arrow data buffers]` — against its already-decoded table
/// schema without copying the data region.
fn decode_inline_batch(schema: SchemaRef, body: &Bytes) -> Result<RecordBatch> {
    if body.len() < 4 {
        return Err(Error::Corruption("inline body truncated".to_owned()));
    }
    let message_len = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| Error::Corruption("inline body length prefix".to_owned()))?,
    ) as usize;
    let message_end = 4 + message_len;
    if message_end > body.len() {
        return Err(Error::Corruption("inline body truncated".to_owned()));
    }
    let message = root_as_message(&body[4..message_end]).map_err(corrupt("inline message"))?;
    let record_batch = message
        .header_as_record_batch()
        .ok_or_else(|| Error::Corruption("inline body is not a record batch".to_owned()))?;
    let version = message.version();
    let buffer = Buffer::from(body.slice(message_end..));
    read_record_batch(
        &buffer,
        record_batch,
        schema,
        &std::collections::HashMap::new(),
        None,
        &version,
    )
    .map_err(corrupt("inline batch"))
}

/// Derives entries from an inline-insert chunk: the columns at `positions`
/// per row, with dense `row_id_start + ordinal` ids.
pub(crate) fn inline_batch_entries(
    schema: SchemaRef,
    body: &Bytes,
    positions: &[usize],
    row_id_start: u64,
) -> Result<Vec<ScopedReadEntry>> {
    #[cfg(test)]
    record_inline_batch_decode(row_id_start);

    let batch = decode_inline_batch(schema, body)?;
    record_batch_entries(&batch, positions, None, row_id_start, Ordinals::Dense, 0)
}

/// Decodes one inline Arrow chunk once and constructs every requested index
/// key directly from its arrays.
pub(crate) fn inline_batch_index_entries(
    schema: SchemaRef,
    body: &Bytes,
    projections: &[IndexProjection],
    row_id_start: u64,
    included_row_ids: Option<&HashSet<u64>>,
) -> Result<Vec<ScopedIndexEntry>> {
    #[cfg(test)]
    record_inline_batch_decode(row_id_start);

    let batch = decode_inline_batch(schema, body)?;
    record_batch_index_entries(
        &batch,
        projections,
        None,
        row_id_start,
        Ordinals::Dense,
        0,
        None,
        included_row_ids,
    )
}

#[cfg(test)]
fn inline_batch_decodes() -> &'static std::sync::Mutex<std::collections::HashMap<u64, usize>> {
    static COUNTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u64, usize>>> =
        std::sync::OnceLock::new();
    COUNTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn record_inline_batch_decode(row_id_start: u64) {
    let mut counts = inline_batch_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts.entry(row_id_start).or_default() += 1;
}

#[cfg(test)]
pub(crate) fn inline_batch_decode_count(row_id_start: u64) -> usize {
    inline_batch_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&row_id_start)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
fn inline_schema_decodes() -> &'static std::sync::Mutex<std::collections::HashMap<u64, usize>> {
    static COUNTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u64, usize>>> =
        std::sync::OnceLock::new();
    COUNTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn inline_schema_fingerprint(schema_ipc: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schema_ipc.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
fn record_inline_schema_decode(schema_ipc: &[u8]) {
    let mut counts = inline_schema_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts
        .entry(inline_schema_fingerprint(schema_ipc))
        .or_default() += 1;
}

/// How often the named schema IPC bytes were decoded in this test process.
#[cfg(test)]
pub(crate) fn inline_schema_decode_count(schema_ipc: &[u8]) -> usize {
    inline_schema_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&inline_schema_fingerprint(schema_ipc))
        .copied()
        .unwrap_or_default()
}
