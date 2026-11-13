//! Admission shared by catalog scans and Parquet range reads.

/// Both policies consult existing entries; only `Retain` admits misses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheAdmission {
    Retain,
    ReuseOnly,
}

impl CacheAdmission {
    pub(crate) fn retains(self) -> bool {
        self == Self::Retain
    }
}
