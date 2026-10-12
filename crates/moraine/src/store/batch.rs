//! The writes of one committed batch, decoded for folding forward: every
//! key up front, each value on request as the kind its key dictates.

use crate::{
    error::Result,
    store::{
        key::{InlineKey, Key, SysKey},
        proto::{HeadValue, InlineChunkRangeValue, SnapshotValue},
        read::{EntityRecord, decode_current_record, decode_history_record},
        value,
    },
};

/// One write of a committed batch: a put of the value bytes, or a delete
/// when there are none.
pub(crate) struct BatchWrite<'a> {
    encoded_key: &'a [u8],
    key: Key,
    bytes: Option<&'a [u8]>,
}

/// A written value, decoded as its key's kind dictates.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DecodedValue {
    /// The `sys/head` record.
    Head(HeadValue),
    /// A snapshot record.
    Snapshot(SnapshotValue),
    /// A `current` or `history` entity record.
    Record(EntityRecord),
    /// An inline chunk locator's directory record.
    InlineChunkRange(InlineChunkRangeValue),
    /// A kind no fold follows; its bytes are left undecoded.
    Unfollowed,
}

/// Decodes every key of `writes`, failing on the first this build cannot
/// read. Values stay encoded until asked for.
pub(crate) fn decode_batch(writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<Vec<BatchWrite<'_>>> {
    writes
        .iter()
        .map(|(encoded_key, bytes)| {
            Ok(BatchWrite {
                encoded_key,
                key: Key::decode(encoded_key)?,
                bytes: bytes.as_deref(),
            })
        })
        .collect()
}

impl BatchWrite<'_> {
    pub(crate) fn key(&self) -> &Key {
        &self.key
    }

    pub(crate) fn encoded_key(&self) -> &[u8] {
        self.encoded_key
    }

    /// The written value, or `None` for a delete. Fails as `Corruption`
    /// when the bytes do not decode as the key's kind.
    pub(crate) fn value(&self) -> Result<Option<DecodedValue>> {
        let Some(bytes) = self.bytes else {
            return Ok(None);
        };

        let decoded = match &self.key {
            Key::Sys(SysKey::Head) => DecodedValue::Head(value::decode_value(bytes)?),
            Key::Snapshot { .. } => DecodedValue::Snapshot(value::decode_value(bytes)?),
            Key::Current(_) => {
                DecodedValue::Record(decode_current_record(self.key.clone(), bytes)?)
            }
            Key::History(_) => {
                DecodedValue::Record(decode_history_record(self.key.clone(), bytes)?)
            }
            Key::Inline(InlineKey::ChunkLocator { .. }) => {
                DecodedValue::InlineChunkRange(value::decode_value(bytes)?)
            }
            _ => DecodedValue::Unfollowed,
        };

        Ok(Some(decoded))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::store::{
        index_encoding::CanonicalKey,
        key::{EntityKey, IndexKey},
        proto::{SchemaValue, TableStatsValue},
        value::encode_value,
    };

    fn head(snapshot_id: u64) -> HeadValue {
        HeadValue {
            snapshot_id,
            batch_seq: snapshot_id,
        }
    }

    #[test]
    fn each_value_decodes_as_its_key_dictates() {
        let locator = Key::Inline(InlineKey::ChunkLocator {
            table_id: 7,
            row_id_end: 9,
            schema_version: 1,
            begin_snapshot: 2,
            chunk_seq: 0,
        });
        let range = InlineChunkRangeValue {
            row_id_start: 5,
            schema_version: 1,
            begin_snapshot: 2,
            chunk_seq: 0,
        };
        let stats = TableStatsValue {
            table_id: 7,
            record_count: 11,
            ..TableStatsValue::default()
        };
        let schema = SchemaValue {
            schema_id: 7,
            begin_snapshot: 1,
            end_snapshot: Some(3),
            ..SchemaValue::default()
        };
        let writes = vec![
            (
                Key::Sys(SysKey::Head).encode(),
                Some(encode_value(&head(4))),
            ),
            (
                Key::Snapshot { snapshot_id: 4 }.encode(),
                Some(encode_value(&SnapshotValue {
                    snapshot_id: 4,
                    ..SnapshotValue::default()
                })),
            ),
            (
                Key::current(EntityKey::TableStats { table_id: 7 }).encode(),
                Some(encode_value(&stats)),
            ),
            (
                Key::history(EntityKey::Schema { schema_id: 7 }, 3).encode(),
                Some(encode_value(&schema)),
            ),
            (locator.encode(), Some(encode_value(&range))),
            (
                Key::Index(IndexKey::Unique {
                    index_id: 1,
                    key: CanonicalKey::empty(),
                })
                .encode(),
                Some(vec![0, 0, 0, 0, 0, 0, 0, 1]),
            ),
            (Key::Snapshot { snapshot_id: 1 }.encode(), None),
        ];

        let decoded = decode_batch(&writes).unwrap();
        assert_eq!(decoded.len(), writes.len());
        assert!(
            decoded
                .iter()
                .zip(&writes)
                .all(|(write, (encoded, _))| write.encoded_key() == encoded.as_slice())
        );
        assert_eq!(
            decoded[0].value().unwrap(),
            Some(DecodedValue::Head(head(4)))
        );
        assert!(matches!(
            decoded[1].value().unwrap(),
            Some(DecodedValue::Snapshot(snapshot)) if snapshot.snapshot_id == 4
        ));
        assert_eq!(
            decoded[2].value().unwrap(),
            Some(DecodedValue::Record(EntityRecord::TableStats(stats)))
        );
        assert_eq!(
            decoded[3].value().unwrap(),
            Some(DecodedValue::Record(EntityRecord::Schema(schema)))
        );
        assert_eq!(
            decoded[3].key(),
            &Key::history(EntityKey::Schema { schema_id: 7 }, 3)
        );
        assert_eq!(
            decoded[4].value().unwrap(),
            Some(DecodedValue::InlineChunkRange(range))
        );
        assert_eq!(decoded[5].value().unwrap(), Some(DecodedValue::Unfollowed));
        assert_eq!(decoded[6].value().unwrap(), None);
    }

    #[test]
    fn an_undecodable_key_fails_the_batch() {
        let writes = vec![
            (
                Key::Sys(SysKey::Head).encode(),
                Some(encode_value(&head(1))),
            ),
            (vec![0xff, 0xee], Some(vec![])),
        ];
        assert!(decode_batch(&writes).is_err());
    }

    #[test]
    fn an_undecodable_value_fails_only_its_own_write() {
        let writes = vec![
            (
                Key::Snapshot { snapshot_id: 1 }.encode(),
                Some(vec![0xff; 4]),
            ),
            (
                Key::Sys(SysKey::Head).encode(),
                Some(encode_value(&head(1))),
            ),
        ];
        let decoded = decode_batch(&writes).unwrap();
        assert!(decoded[0].value().is_err());
        assert_eq!(
            decoded[1].value().unwrap(),
            Some(DecodedValue::Head(head(1)))
        );
    }

    proptest! {
        #[test]
        fn head_and_snapshot_values_roundtrip(
            head in any::<HeadValue>(),
            snapshot in any::<SnapshotValue>(),
        ) {
            let writes = vec![
                (Key::Sys(SysKey::Head).encode(), Some(encode_value(&head))),
                (Key::Snapshot { snapshot_id: snapshot.snapshot_id }.encode(),
                 Some(encode_value(&snapshot))),
            ];
            let decoded = decode_batch(&writes).unwrap();
            prop_assert_eq!(decoded[0].value().unwrap(), Some(DecodedValue::Head(head)));
            prop_assert_eq!(decoded[1].value().unwrap(), Some(DecodedValue::Snapshot(snapshot)));
        }

        /// Decoding is total: any key bytes fail or decode, and any value
        /// bytes under a decoded key fail or decode, never panic.
        #[test]
        fn decoding_arbitrary_writes_never_panics(
            key in proptest::collection::vec(any::<u8>(), 0..32),
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let writes = vec![(key, Some(bytes))];
            if let Ok(decoded) = decode_batch(&writes) {
                let _ = decoded[0].value();
            }
        }
    }
}
