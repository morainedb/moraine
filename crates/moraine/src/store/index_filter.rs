//! Equality-prefix bloom filtering for non-unique index entries.

use std::sync::Arc;

use slatedb::{BloomFilterPolicy, FilterPolicy, PrefixExtractor, PrefixTarget};

use crate::store::key::{INDEX_ENTRY_PREFIX_LEN, INDEX_MULTI_TAG, INDEX_SUBSPACE_TAG};

/// Keeps the legacy whole-key filter alongside the equality-prefix filter.
pub(crate) fn policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![
        Arc::new(BloomFilterPolicy::new(10)),
        Arc::new(
            BloomFilterPolicy::new(10)
                .with_prefix_extractor(Arc::new(EqualityPrefix))
                .with_whole_key_filtering(false),
        ),
    ]
}

/// Extracts the framed indexed value, excluding the trailing row id.
pub(crate) struct EqualityPrefix;

impl PrefixExtractor for EqualityPrefix {
    fn name(&self) -> &'static str {
        "moraine-index-equality-v1"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes = match target {
            PrefixTarget::Point(bytes) | PrefixTarget::Prefix(bytes) => bytes,
        };
        if !bytes.starts_with(&[INDEX_SUBSPACE_TAG, INDEX_MULTI_TAG]) {
            return None;
        }
        // Storekey frames the value with an unescaped zero terminator;
        // escaped zero and one bytes are preceded by one.
        let mut offset = INDEX_ENTRY_PREFIX_LEN;
        while let Some(byte) = bytes.get(offset) {
            match byte {
                0 => return Some(offset + 1),
                1 => offset += 2,
                _ => offset += 1,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use proptest::prelude::*;

    use super::*;
    use crate::store::{
        index_encoding::{IndexKeyValue, encode_key},
        key::{IndexKey, Key, index_multi_value_prefix},
    };

    proptest! {
        #[test]
        fn equality_prefix_roundtrips(index in any::<u64>(), row in any::<u64>(), value in proptest::collection::vec(any::<u8>(), 0..128)) {
            let canonical = encode_key(&[IndexKeyValue::Bytes(value)]).unwrap();
            let prefix = index_multi_value_prefix(index, &canonical);
            let key = Key::Index(IndexKey::Multi { index_id: index, key: canonical, row_id: row });
            let encoded = key.encode();
            prop_assert_eq!(Key::decode(&encoded).unwrap(), key);
            prop_assert_eq!(EqualityPrefix.prefix_len(&PrefixTarget::Point(encoded.into())), Some(prefix.len()));
            prop_assert_eq!(EqualityPrefix.prefix_len(&PrefixTarget::Prefix(prefix.clone().into())), Some(prefix.len()));
            for end in 0..prefix.len() {
                prop_assert_eq!(EqualityPrefix.prefix_len(&PrefixTarget::Prefix(Bytes::copy_from_slice(&prefix[..end]))), None);
            }
        }
    }
}
