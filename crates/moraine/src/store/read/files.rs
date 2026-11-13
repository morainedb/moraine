//! Typed current-file discovery and point metadata reads.

use std::ops::Bound;

use super::{ReadHandle, ScanShape, value};
use crate::{
    MemoryReservation, Result,
    memory::{Reserved, WorkingValue},
    store::{
        handle::ScanIterator,
        key::{EntityKey, EntityKind, Key, current_entity_kind_prefix},
        proto::{DataFileValue, SchemaValue, TableValue},
    },
};

pub(crate) struct CurrentFiles {
    iterator: ScanIterator,
}

impl CurrentFiles {
    pub(crate) async fn open(
        handle: ReadHandle<'_>,
        after: Option<(u64, u64)>,
        shape: ScanShape,
    ) -> Result<Self> {
        let prefix = current_entity_kind_prefix(EntityKind::File);
        let lower = after.map_or(Bound::Unbounded, |(table_id, data_file_id)| {
            Bound::Excluded(
                Key::current(EntityKey::File {
                    table_id,
                    data_file_id,
                })
                .encode()[prefix.len()..]
                    .to_vec(),
            )
        });
        Ok(Self {
            iterator: handle
                .scan_prefix(prefix, (lower, Bound::Unbounded), shape)
                .await?,
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<WorkingValue<DataFileValue>>> {
        let Some(entry) = self.iterator.next().await? else {
            return Ok(None);
        };
        decode_reserved::<DataFileValue>(&entry.value).map(|record| Some(record.into_shared()))
    }
}

fn decode_reserved<M: prost::Message + Default>(bytes: &[u8]) -> Result<Reserved<M>> {
    let charge = bytes
        .len()
        .saturating_mul(3)
        .saturating_add(size_of::<M>() + size_of::<MemoryReservation>() + 2 * size_of::<usize>());
    let memory = MemoryReservation::reserve(charge as u64)?;
    Ok(Reserved::new(value::decode_value(bytes)?, memory))
}

async fn current<M: prost::Message + Default>(
    handle: ReadHandle<'_>,
    entity: EntityKey,
) -> Result<Option<Reserved<M>>> {
    let Some(bytes) = handle.get(&Key::current(entity).encode()).await? else {
        return Ok(None);
    };
    decode_reserved(&bytes).map(Some)
}

pub(crate) async fn file(
    handle: ReadHandle<'_>,
    table_id: u64,
    data_file_id: u64,
) -> Result<Option<Reserved<DataFileValue>>> {
    current(
        handle,
        EntityKey::File {
            table_id,
            data_file_id,
        },
    )
    .await
}

pub(crate) async fn table(
    handle: ReadHandle<'_>,
    table_id: u64,
) -> Result<Option<Reserved<TableValue>>> {
    current(handle, EntityKey::Table { table_id }).await
}

pub(crate) async fn schema(
    handle: ReadHandle<'_>,
    schema_id: u64,
) -> Result<Option<Reserved<SchemaValue>>> {
    current(handle, EntityKey::Schema { schema_id }).await
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

        #[test]
        fn reserved_file_roundtrip(file in any::<DataFileValue>()) {
            let encoded = value::encode_value(&file);
            let decoded = decode_reserved::<DataFileValue>(&encoded).unwrap();
            prop_assert_eq!(&*decoded, &file);
            let (_, reservation) = decoded.into_parts();
            prop_assert!(reservation.bytes() >= encoded.len() as u64);
        }

        #[test]
        fn reserved_table_roundtrip(table in any::<TableValue>()) {
            let decoded = decode_reserved::<TableValue>(&value::encode_value(&table)).unwrap();
            prop_assert_eq!(&*decoded, &table);
        }

        #[test]
        fn reserved_schema_roundtrip(schema in any::<SchemaValue>()) {
            let decoded = decode_reserved::<SchemaValue>(&value::encode_value(&schema)).unwrap();
            prop_assert_eq!(&*decoded, &schema);
        }
    }
}
