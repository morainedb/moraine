//! Typed value codec: protobuf messages behind the framing header.

use prost::Message;

use crate::{
    error::{Error, Result},
    store::frame,
};

/// Encode a message behind the framing header, directly into the framed
/// buffer with no payload copy.
pub(crate) fn encode_value<M: Message>(msg: &M) -> Vec<u8> {
    let mut buf = frame::header_buf(msg.encoded_len());
    // Infallible by construction: insufficient capacity is the only error
    // `encode` returns, and a `Vec`'s `BufMut` capacity is unbounded.
    #[allow(clippy::expect_used)]
    msg.encode(&mut buf)
        .expect("prost encode into a Vec cannot fail");
    buf
}

/// Validate the framing header and decode the payload as `M`. Fails as
/// [`Error::Corruption`] on any framing or protobuf decode failure.
pub(crate) fn decode_value<M: Message + Default>(bytes: &[u8]) -> Result<M> {
    let payload = frame::unframe(bytes)?;
    M::decode(payload).map_err(|err| Error::Corruption(format!("value: {err}")))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::store::proto::*;

    #[test]
    fn decode_rejects_unframed_bytes() {
        let err = decode_value::<HeadValue>(b"not framed").unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn decode_rejects_framed_garbage_payload() {
        // A valid frame whose payload is not a valid message for the
        // requested type: field 1 wire-type 2 with a length running past
        // the end of the buffer.
        let framed = frame::frame(&[0x0a, 0xff]);
        let err = decode_value::<HeadValue>(&framed).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    proptest! {
        // Decode is total: arbitrary bytes decode or fail as
        // `Corruption`, never panic.
        #[test]
        fn decode_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let _ = decode_value::<HeadValue>(&bytes);
        }
    }

    macro_rules! codec_tests {
        // Codec roundtrips need breadth, not depth: 48 cases cover the wire
        // shapes without the default 256 generating multi-megabyte trees for
        // the deeply-nested messages. Doubly-nested ones set their own lower
        // count — generation, not assertion, is what costs.
        ($roundtrip:ident, $garbage:ident, $ty:ty) => {
            codec_tests!($roundtrip, $garbage, $ty, 48);
        };
        ($roundtrip:ident, $garbage:ident, $ty:ty, $cases:expr) => {
            proptest! {
                #![proptest_config(ProptestConfig { cases: $cases, ..ProptestConfig::default() })]
                #[test]
                fn $roundtrip(msg in any::<$ty>()) {
                    let encoded = encode_value(&msg);
                    let decoded: $ty = decode_value(&encoded).unwrap();
                    prop_assert_eq!(decoded, msg);
                }

                // Decode is total: framed garbage decodes as some
                // message or fails as `Corruption`, never panics.
                #[test]
                fn $garbage(
                    payload in proptest::collection::vec(any::<u8>(), 0..256),
                ) {
                    let _ = decode_value::<$ty>(&frame::frame(&payload));
                }
            }
        };
    }

    codec_tests!(roundtrip_schema, garbage_schema, SchemaValue);
    codec_tests!(roundtrip_table, garbage_table, TableValue);
    codec_tests!(roundtrip_view, garbage_view, ViewValue);
    codec_tests!(roundtrip_column, garbage_column, ColumnValue);
    codec_tests!(roundtrip_partition, garbage_partition, PartitionValue);
    codec_tests!(roundtrip_sort, garbage_sort, SortValue);
    codec_tests!(roundtrip_index, garbage_index, IndexValue);
    codec_tests!(roundtrip_macro, garbage_macro, MacroValue, 8);
    codec_tests!(roundtrip_mapping, garbage_mapping, MappingValue);
    codec_tests!(roundtrip_data_file, garbage_data_file, DataFileValue);
    codec_tests!(roundtrip_delete_file, garbage_delete_file, DeleteFileValue);
    codec_tests!(
        roundtrip_file_column_stats,
        garbage_file_column_stats,
        FileColumnStatsValue
    );
    codec_tests!(roundtrip_table_stats, garbage_table_stats, TableStatsValue);
    codec_tests!(
        roundtrip_table_column_stats,
        garbage_table_column_stats,
        TableColumnStatsValue
    );
    codec_tests!(roundtrip_tag, garbage_tag, TagValue);
    codec_tests!(roundtrip_tag_entry, garbage_tag_entry, TagEntry);
    codec_tests!(
        roundtrip_option_scope,
        garbage_option_scope,
        OptionScopeValue
    );
    codec_tests!(roundtrip_snapshot, garbage_snapshot, SnapshotValue);
    codec_tests!(roundtrip_gcfile, garbage_gcfile, GcFileValue);
    codec_tests!(roundtrip_format, garbage_format, FormatValue);
    codec_tests!(roundtrip_head, garbage_head, HeadValue);
    codec_tests!(roundtrip_migration, garbage_migration, MigrationValue);
    codec_tests!(
        roundtrip_maintenance_status,
        garbage_maintenance_status,
        MaintenanceStatusValue,
        16
    );
    codec_tests!(
        roundtrip_inline_schema,
        garbage_inline_schema,
        InlineSchemaValue
    );
    codec_tests!(
        roundtrip_inline_chunk,
        garbage_inline_chunk,
        InlineChunkValue
    );
    codec_tests!(
        roundtrip_inline_inline_delete,
        garbage_inline_inline_delete,
        InlineInlineDeleteValue
    );
    codec_tests!(
        roundtrip_inline_file_delete,
        garbage_inline_file_delete,
        InlineFileDeleteValue
    );
}
