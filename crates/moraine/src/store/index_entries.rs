//! Reading the entries one equality index holds: the row-id value a unique
//! entry carries, and scans that hand back decoded entries.

use bytes::Bytes;
use slatedb::DbIterator;

use crate::{
    error::{Error, Result},
    store::{
        handle::{ReadHandle, ScanShape},
        key::{IndexKey, IndexKind, Key, index_id_suffix, index_index_prefix, index_kind_prefix},
    },
};

/// The value of a unique entry: the holding row id, big-endian.
pub(crate) fn encode_row_id(row_id: u64) -> [u8; 8] {
    row_id.to_be_bytes()
}

/// Decodes a unique entry's value.
pub(crate) fn decode_row_id(bytes: &[u8]) -> Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::Corruption(format!(
            "index entry value is {} bytes, expected 8",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(array))
}

/// Every entry of one index of `kind`, as the store holds it, in key order.
pub(crate) async fn scan_index_entries(
    handle: ReadHandle<'_>,
    kind: IndexKind,
    index_id: u64,
    shape: ScanShape,
) -> Result<DbIterator> {
    Ok(handle
        .scan_prefix(index_index_prefix(kind, index_id), .., shape)
        .await?)
}

/// The lowest index id at or after `from` holding an entry of `kind`, or
/// `None` past the last one. The scan stops at the first live key but must
/// read ahead: a swept index leaves its whole range as tombstones until a
/// store merge drops them.
pub(crate) async fn first_index_id_from(
    handle: ReadHandle<'_>,
    kind: IndexKind,
    from: u64,
) -> Result<Option<u64>> {
    let first = handle
        .scan_prefix(
            index_kind_prefix(kind),
            index_id_suffix(kind, from)..,
            ScanShape::Bulk,
        )
        .await?
        .next()
        .await?;

    first.map(|entry| index_id_of(&entry.key)).transpose()
}

/// One entry an index holds, and the row it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredIndexEntry {
    /// The entry's encoded store key.
    pub(crate) key: Bytes,
    /// The row the entry points at: the value of a unique-shaped entry, a
    /// key component of a multi-shaped one.
    pub(crate) row_id: u64,
}

/// A walk over every entry one index holds, unique-shaped entries first.
/// Both shapes are read whatever the index's uniqueness: an entry carrying
/// a NULL takes the multi shape even on a unique index.
pub(crate) struct StoredIndexEntries<'a> {
    handle: ReadHandle<'a>,
    index_id: u64,
    kinds: std::array::IntoIter<IndexKind, 2>,
    entries: Option<DbIterator>,
}

/// Starts a walk over every entry of `index_id`.
pub(crate) fn stored_index_entries(
    handle: ReadHandle<'_>,
    index_id: u64,
) -> StoredIndexEntries<'_> {
    StoredIndexEntries {
        handle,
        index_id,
        kinds: [IndexKind::Unique, IndexKind::Multi].into_iter(),
        entries: None,
    }
}

impl StoredIndexEntries<'_> {
    /// The next entry, or `None` once both shapes are exhausted.
    pub(crate) async fn next(&mut self) -> Result<Option<StoredIndexEntry>> {
        loop {
            if let Some(entries) = &mut self.entries {
                if let Some(entry) = entries.next().await? {
                    return Ok(Some(decode_stored_entry(entry.key, &entry.value)?));
                }
                self.entries = None;
            }

            let Some(kind) = self.kinds.next() else {
                return Ok(None);
            };
            self.entries =
                Some(scan_index_entries(self.handle, kind, self.index_id, ScanShape::Bulk).await?);
        }
    }
}

fn decode_stored_entry(key: Bytes, value: &[u8]) -> Result<StoredIndexEntry> {
    let row_id = match Key::decode(&key)? {
        Key::Index(IndexKey::Unique { .. }) => decode_row_id(value)?,
        Key::Index(IndexKey::Multi { row_id, .. }) => row_id,
        other => return Err(unexpected_index_key(&other)),
    };

    Ok(StoredIndexEntry { key, row_id })
}

fn index_id_of(key: &[u8]) -> Result<u64> {
    match Key::decode(key)? {
        Key::Index(IndexKey::Unique { index_id, .. } | IndexKey::Multi { index_id, .. }) => {
            Ok(index_id)
        }
        other => Err(unexpected_index_key(&other)),
    }
}

fn unexpected_index_key(key: &Key) -> Error {
    Error::Corruption(format!("key in the index subspace decoded as {key:?}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use proptest::prelude::*;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::store::{
        index_encoding::{CanonicalKey, IndexKeyValue, encode_key},
        key::encode_index_entry,
        open::StoreBuilder,
    };

    proptest! {
        #[test]
        fn row_id_roundtrip(row_id in any::<u64>()) {
            prop_assert_eq!(decode_row_id(&encode_row_id(row_id)).unwrap(), row_id);
        }

        #[test]
        fn stored_entry_roundtrip(index_id in any::<u64>(), row_id in any::<u64>(), unique in any::<bool>()) {
            let key = Bytes::from(encode_index_entry(index_id, unique, &canon("value"), row_id));
            let value = if unique { encode_row_id(row_id).to_vec() } else { vec![] };
            let entry = decode_stored_entry(key.clone(), &value).unwrap();
            prop_assert_eq!(entry, StoredIndexEntry { key, row_id });
        }

        /// Decode is total: any length but eight fails as `Corruption`.
        #[test]
        fn decode_row_id_rejects_every_other_length(
            bytes in proptest::collection::vec(any::<u8>(), 0..32),
        ) {
            let decoded = decode_row_id(&bytes);
            prop_assert_eq!(decoded.is_ok(), bytes.len() == 8);
        }
    }

    fn canon(value: &str) -> CanonicalKey {
        encode_key(&[IndexKeyValue::Str(value.to_owned())]).unwrap()
    }

    async fn open(name: &str) -> Db {
        StoreBuilder::new(name, Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap()
            .0
    }

    /// Seeds unique entries for rows 10 and 11 and a multi-shaped one for
    /// row 12 under index 3, plus one unique entry under index 5.
    async fn seed(db: &Db) {
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            encode_index_entry(3, true, &canon("a"), 0),
            encode_row_id(10),
        )
        .unwrap();
        tx.put(
            encode_index_entry(3, true, &canon("b"), 0),
            encode_row_id(11),
        )
        .unwrap();
        tx.put(encode_index_entry(3, false, &canon("c"), 12), [])
            .unwrap();
        tx.put(
            encode_index_entry(5, true, &canon("a"), 0),
            encode_row_id(20),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn stored_entries_name_the_row_of_either_shape() {
        let db = open("index-entries/stored").await;
        seed(&db).await;

        let mut entries = stored_index_entries(ReadHandle::Writer(&db), 3);
        let mut row_ids = Vec::new();
        while let Some(entry) = entries.next().await.unwrap() {
            assert!(Key::decode(&entry.key).is_ok());
            row_ids.push(entry.row_id);
        }
        assert_eq!(row_ids, vec![10, 11, 12]);

        let mut none = stored_index_entries(ReadHandle::Writer(&db), 4);
        assert_eq!(none.next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_unique_entry_without_a_row_id_is_corruption() {
        let db = open("index-entries/corrupt").await;
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(encode_index_entry(3, true, &canon("a"), 0), [1, 2, 3])
            .unwrap();
        tx.commit().await.unwrap();

        let mut entries = stored_index_entries(ReadHandle::Writer(&db), 3);
        assert!(matches!(
            entries.next().await,
            Err(Error::Corruption(message)) if message.contains("3 bytes")
        ));
    }

    #[tokio::test]
    async fn first_index_id_skips_to_the_next_populated_index() {
        let db = open("index-entries/first").await;
        seed(&db).await;
        let handle = ReadHandle::Writer(&db);

        assert_eq!(
            first_index_id_from(handle, IndexKind::Unique, 0)
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            first_index_id_from(handle, IndexKind::Unique, 4)
                .await
                .unwrap(),
            Some(5)
        );
        assert_eq!(
            first_index_id_from(handle, IndexKind::Unique, 6)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            first_index_id_from(handle, IndexKind::Multi, 0)
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            first_index_id_from(handle, IndexKind::Multi, 4)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn scan_index_entries_stays_inside_one_index() {
        let db = open("index-entries/scan").await;
        seed(&db).await;

        let mut entries = scan_index_entries(
            ReadHandle::Writer(&db),
            IndexKind::Unique,
            3,
            ScanShape::Bulk,
        )
        .await
        .unwrap();
        let mut count = 0;
        while entries.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
