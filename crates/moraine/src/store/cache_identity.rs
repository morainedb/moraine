//! Explicit object-store namespaces for memory and disk caches.

use object_store::{local::LocalFileSystem, path::Path};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The object namespace shared by cache users. Equal identities assert that
/// the same object path names the same immutable contents.
///
/// Include the backend, endpoint, bucket or filesystem root, and any wrapper
/// prefix in a stable namespace. Change it when replacing a store's contents;
/// credentials and display names alone do not identify a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheIdentity(u128);

impl CacheIdentity {
    /// Names a namespace consistently across handles and process restarts.
    ///
    /// ```
    /// use moraine::CacheIdentity;
    /// let identity = CacheIdentity::new("s3:https://storage.example/bucket/prefix");
    /// assert_eq!(
    ///     identity,
    ///     CacheIdentity::new("s3:https://storage.example/bucket/prefix")
    /// );
    /// ```
    #[must_use]
    pub fn new(namespace: &str) -> Self {
        Self::from_bytes(namespace.as_bytes())
    }

    fn from_bytes(namespace: &[u8]) -> Self {
        let version = Uuid::new_v5(&Uuid::NAMESPACE_URL, b"moraine/cache-identity/v2");
        Self(Uuid::new_v5(&version, namespace).as_u128())
    }

    /// Names a local store by its resolved filesystem root.
    ///
    /// # Errors
    /// Returns an error if the store cannot resolve a filesystem path.
    pub fn local(store: &LocalFileSystem) -> object_store::Result<Self> {
        // A fixed child resolves the root without requiring that child to exist.
        let member = store.path_to_filesystem(&Path::from(".moraine-cache-identity"))?;
        let mut namespace = b"local:".to_vec();
        namespace.extend_from_slice(member.as_os_str().as_encoded_bytes());
        Ok(Self::from_bytes(&namespace))
    }

    /// A fixed-width name suitable for a cache directory component.
    pub(crate) fn directory(self) -> String {
        Uuid::from_u128(self.0).simple().to_string()
    }
}

impl Default for CacheIdentity {
    /// Creates an isolated namespace. Clone it to share within this process.
    fn default() -> Self {
        Self(Uuid::new_v4().as_u128())
    }
}

#[cfg(test)]
mod tests {
    use foyer::Code;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn cache_identity_roundtrips(value in any::<u128>()) {
            let identity = CacheIdentity(value);
            let mut bytes = Vec::new();
            identity.encode(&mut bytes).unwrap();
            prop_assert_eq!(CacheIdentity::decode(&mut bytes.as_slice()).unwrap(), identity);
        }
    }

    #[test]
    fn stable_names_are_versioned_and_isolated_names_are_unique() {
        let name = "AmazonS3(bucket)";
        assert_eq!(CacheIdentity::new(name), CacheIdentity::new(name));
        assert_ne!(
            CacheIdentity::new(name).0,
            Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()).as_u128()
        );
        assert_ne!(CacheIdentity::default(), CacheIdentity::default());
    }
}
