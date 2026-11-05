//! Current delete-file target prefixes.

use super::{Key, TAG_PREFIX_LEN, prefix_of};

pub(crate) fn delete_target_prefix(table_id: u64, data_file_id: u64) -> Vec<u8> {
    prefix_of(
        &Key::DeleteTarget {
            table_id,
            data_file_id,
            delete_file_id: 0,
        },
        TAG_PREFIX_LEN + 2 * size_of::<u64>(),
    )
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn targets_roundtrip_and_isolate_files(table in any::<u64>(), file in any::<u64>(), deletion in any::<u64>()) {
            let key = Key::DeleteTarget { table_id: table, data_file_id: file, delete_file_id: deletion };
            let bytes = key.encode();
            prop_assert_eq!(Key::decode(&bytes).unwrap(), key);
            prop_assert_eq!(bytes[0], 0x0b);
            prop_assert_eq!(bytes.len(), 25);
            prop_assert!(bytes.starts_with(&delete_target_prefix(table, file)));
            prop_assert!(!bytes.starts_with(&delete_target_prefix(table.wrapping_add(1), file)));
            prop_assert!(!bytes.starts_with(&delete_target_prefix(table, file.wrapping_add(1))));
            let suffix = deletion.to_be_bytes();
            prop_assert_eq!(&bytes[17..], suffix.as_slice());
        }
    }
}
