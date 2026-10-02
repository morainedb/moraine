//! Comparing the store locations one lake addresses.

/// Whether two store locations name the same object store with one
/// containing the other — a lake's data root against the catalog store, or
/// against the store its write-ahead log is written to.
///
/// Compared lexically by path component, in whatever spelling the host
/// addresses its stores by (`s3://bucket/prefix`, a local directory): two
/// spellings of one bucket, symlinks, and `..` are not resolved, so a
/// `false` is "not provably nested" rather than "provably separate".
///
/// # Examples
///
/// ```
/// assert!(moraine::store_paths_overlap(
///     "s3://lake/warehouse",
///     "s3://lake/warehouse/data"
/// ));
/// // The bucket root contains everything in it.
/// assert!(moraine::store_paths_overlap("s3://lake", "s3://lake/data"));
/// // Sibling prefixes that merely share leading text do not.
/// assert!(!moraine::store_paths_overlap(
///     "s3://lake/warehouse",
///     "s3://lake/ware"
/// ));
/// // Nor do different stores.
/// assert!(!moraine::store_paths_overlap(
///     "s3://lake/data",
///     "/var/lib/lake/data"
/// ));
/// ```
#[must_use]
pub fn store_paths_overlap(one: &str, other: &str) -> bool {
    let (one, other) = (std::path::Path::new(one), std::path::Path::new(other));
    one.starts_with(other) || other.starts_with(one)
}

#[cfg(test)]
mod tests {
    use super::store_paths_overlap;

    /// One location containing the other, whichever way round it is given.
    #[test]
    fn nesting_is_symmetric() {
        for (one, other) in [
            ("s3://bucket/lake", "s3://bucket/lake/data"),
            ("s3://bucket", "s3://bucket/data"),
            ("/tmp/lake/catalog", "/tmp/lake"),
            ("/tmp/lake", "/tmp/lake/data"),
            // A trailing separator names the same location.
            ("/tmp/lake/", "/tmp/lake/data"),
        ] {
            assert!(store_paths_overlap(one, other), "{one} / {other}");
            assert!(store_paths_overlap(other, one), "{other} / {one}");
        }
    }

    /// Locations neither of which contains the other, including the ones
    /// that share leading text without sharing a component.
    #[test]
    fn separate_locations_do_not_overlap() {
        for (one, other) in [
            ("s3://bucket/lakehouse", "s3://bucket/lake"),
            ("s3://bucket/lake-catalog", "s3://bucket/lake"),
            ("/tmp/lakehouse", "/tmp/lake"),
            ("s3://bucket/catalog", "s3://bucket/data"),
            ("/tmp/catalog", "/tmp/data"),
            ("s3://catalogs/lake", "s3://data/lake"),
            ("/tmp/catalog", "s3://bucket/data"),
            ("memory://", "/tmp/data"),
        ] {
            assert!(!store_paths_overlap(one, other), "{one} / {other}");
            assert!(!store_paths_overlap(other, one), "{other} / {one}");
        }
    }
}
