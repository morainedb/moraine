//! Exact-name prefixes for persistent namespace lookup.

use storekey::{Decode, Encode};

use super::Key;

/// Namespace discriminants, fixed in their declared order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum NameKind {
    /// A schema in the catalog namespace.
    Schema,
    /// A table in its schema namespace.
    Table,
}

/// One current entity's exact name; entity IDs preserve duplicate names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) struct NameKey {
    /// Entity namespace.
    pub(crate) kind: NameKind,
    /// Schema ID for tables, zero for schemas.
    pub(crate) parent: u64,
    /// The stored name, without case folding.
    pub(crate) name: String,
    /// The entity carrying this name.
    pub(crate) entity_id: u64,
}

/// Prefix selecting all entities carrying an exact name in one namespace.
pub(crate) fn name_prefix(kind: NameKind, parent: u64, name: &str) -> Vec<u8> {
    let mut key = Key::Name(NameKey {
        kind,
        parent,
        name: name.to_owned(),
        entity_id: 0,
    })
    .encode();
    key.truncate(key.len() - size_of::<u64>());
    key
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::store::key::Key;

    proptest! {
        #[test]
        fn names_roundtrip_and_isolate_namespaces(parent in any::<u64>(), name in any::<String>(), id in any::<u64>()) {
            for kind in [NameKind::Schema, NameKind::Table] {
                let key = Key::Name(NameKey { kind, parent, name: name.clone(), entity_id: id });
                let encoded = key.encode();
                prop_assert_eq!(Key::decode(&encoded).unwrap(), key);
                prop_assert!(encoded.starts_with(&name_prefix(kind, parent, &name)));
                prop_assert!(!encoded.starts_with(&name_prefix(kind, parent.wrapping_add(1), &name)));
                let extended = format!("{name}\0");
                prop_assert!(!encoded.starts_with(&name_prefix(kind, parent, &extended)));
                let other = if kind == NameKind::Schema { NameKind::Table } else { NameKind::Schema };
                prop_assert!(!encoded.starts_with(&name_prefix(other, parent, &name)));
            }
        }
    }
}
