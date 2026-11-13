//! Incremental schema-version records and probes at one fixed read point.

use crate::{
    Error, MemoryReservation, Result,
    store::{
        handle::{OwnedReadHandle, ReadHandle, ScanIterator, ScanShape},
        key::{Key, Subspace, schema_version_table_prefix, subspace_prefix},
        proto::SchemaVersionValue,
        value,
    },
};

pub(crate) struct SchemaVersionCursor {
    handle: OwnedReadHandle,
    iterator: ScanIterator,
}

impl SchemaVersionCursor {
    pub(crate) async fn open(handle: OwnedReadHandle) -> Result<Self> {
        let iterator = handle
            .borrow()
            .scan_prefix(
                subspace_prefix(Subspace::SchemaVersion),
                ..,
                ScanShape::bounded().reuse_only(),
            )
            .await?;
        Ok(Self { handle, iterator })
    }

    pub(crate) async fn open_table(handle: OwnedReadHandle, table_id: u64) -> Result<Self> {
        let iterator = handle
            .borrow()
            .scan_prefix(
                schema_version_table_prefix(table_id),
                ..,
                ScanShape::seek().retain(),
            )
            .await?;
        Ok(Self { handle, iterator })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(u64, u64, u64, MemoryReservation)>> {
        self.handle.borrow().validate()?;
        let Some(entry) = self.iterator.next().await? else {
            return Ok(None);
        };
        self.handle.borrow().validate()?;
        let memory = MemoryReservation::reserve(
            (entry.key.len() as u64)
                .saturating_add(entry.value.len() as u64)
                .saturating_mul(2)
                .saturating_add(super::RECORD_BYTES),
        )?;
        let Key::SchemaVersion {
            table_id,
            begin_snapshot,
        } = Key::decode(&entry.key)?
        else {
            return Err(Error::Corruption(
                "non-schema-version key in schema-version cursor".into(),
            ));
        };
        let value: SchemaVersionValue = value::decode_value(&entry.value)?;
        Ok(Some((
            table_id,
            begin_snapshot,
            value.schema_version,
            memory,
        )))
    }
}

pub(crate) async fn exists(
    handle: ReadHandle<'_>,
    table_id: u64,
    begin_snapshot: u64,
) -> Result<bool> {
    Ok(handle
        .get(
            Key::SchemaVersion {
                table_id,
                begin_snapshot,
            }
            .encode(),
        )
        .await?
        .is_some())
}

pub(crate) async fn first(handle: ReadHandle<'_>, table_id: u64) -> Result<Option<(u64, u64)>> {
    let mut cursor = handle
        .scan_prefix(
            schema_version_table_prefix(table_id),
            ..,
            ScanShape::seek().retain(),
        )
        .await?;
    let Some(entry) = cursor.next().await? else {
        return Ok(None);
    };
    let Key::SchemaVersion { begin_snapshot, .. } = Key::decode(&entry.key)? else {
        return Err(Error::Corruption(
            "non-schema-version key in table probe".into(),
        ));
    };
    let value: SchemaVersionValue = value::decode_value(&entry.value)?;
    Ok(Some((begin_snapshot, value.schema_version)))
}
