//! A published row summary: the bytes the auxiliary cache's disk tier
//! holds, written beside the data file they describe.

use std::io::Write;

use futures::StreamExt as _;
use object_store::{ObjectStoreExt as _, path::Path};
use tracing::debug;

use super::{
    DataStore,
    auxiliary_cache::{decode_membership, decode_summary, encode_summary_halves},
    row_set::{FileRowSet, PositionedRowSet},
};
use crate::error::{Error, Result};

/// What every sidecar starts with, before its variable-length path.
const HEADER_BYTES: usize = 64;

/// Distinguishes a sidecar from anything else that ends in `.rowsum`.
const MAGIC: [u8; 8] = *b"MORAROWS";

/// The only format this reader and writer speak.
const VERSION: u32 = 1;

/// The suffix a sidecar carries. Never `.parquet`, which DuckLake's orphan
/// cleanup deletes when its catalog does not know the object.
const SUFFIX: &str = ".rowsum";

/// The file a sidecar describes, as its catalog records it. A sidecar whose
/// header disagrees describes some other file and is treated as absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SidecarIdentity<'a> {
    pub(super) table_id: u64,
    pub(super) data_file_id: u64,
    pub(super) file_path: &'a str,
    pub(super) file_size: u64,
}

/// Why a sidecar did not answer. Every variant means the same thing to a
/// caller — read the data file instead — and they are separate so a
/// deployment can tell "none published yet" from "published and refused".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Rejected {
    /// Too short to hold what it claims, or malformed within.
    Malformed,
    /// A version this reader does not know.
    Version,
    /// A header naming some other data file.
    Identity,
}

/// Where `data_file`'s sidecar lives: beside it, under the same prefix.
pub(super) fn path_for(data_file: &Path) -> Result<Path> {
    Path::parse(format!("{data_file}{SUFFIX}"))
        .map_err(|error| Error::Corruption(format!("invalid sidecar path: {error}")))
}

/// The bytes describing `rows`, for the file `identity` names.
pub(super) fn encode(identity: SidecarIdentity<'_>, rows: &PositionedRowSet) -> Result<Vec<u8>> {
    let (set, order) = encode_summary_halves(rows)
        .map_err(|error| Error::Corruption(format!("row summary will not encode: {error}")))?;
    let path = identity.file_path.as_bytes();
    let path_len = u32::try_from(path.len())
        .map_err(|_| Error::Corruption("a data-file path too long to publish".to_owned()))?;

    let mut bytes = Vec::with_capacity(HEADER_BYTES + path.len() + set.len() + order.len());
    // Encoding into a `Vec` cannot fail: it grows to take what it is given.
    #[allow(clippy::expect_used)]
    {
        let write = &mut bytes;
        write.write_all(&MAGIC).expect("a Vec write cannot fail");
        write
            .write_all(&VERSION.to_le_bytes())
            .expect("a Vec write cannot fail");
        for field in [
            identity.table_id,
            identity.data_file_id,
            identity.file_size,
            rows.rows.cardinality(),
            set.len() as u64,
            order.len() as u64,
        ] {
            write
                .write_all(&field.to_le_bytes())
                .expect("a Vec write cannot fail");
        }
        write
            .write_all(&path_len.to_le_bytes())
            .expect("a Vec write cannot fail");
    }
    debug_assert_eq!(bytes.len(), HEADER_BYTES);

    bytes.extend_from_slice(path);
    bytes.extend_from_slice(&set);
    bytes.extend_from_slice(&order);
    Ok(bytes)
}

/// What a header says about where the rest of the sidecar sits, so a
/// fetcher that read only a prefix knows whether it read enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Layout {
    /// Where the membership begins.
    pub(super) set_at: usize,
    pub(super) set_len: usize,
    pub(super) order_len: usize,
}

impl Layout {
    /// The prefix a membership-only read must cover.
    pub(super) fn membership_end(&self) -> usize {
        self.set_at.saturating_add(self.set_len)
    }
}

/// Reads the header of `bytes`, which need only be long enough to hold it,
/// and refuses one that does not describe the file `identity` names.
pub(super) fn layout_of(
    identity: SidecarIdentity<'_>,
    bytes: &[u8],
) -> std::result::Result<Layout, Rejected> {
    let header = bytes.get(..HEADER_BYTES).ok_or(Rejected::Malformed)?;
    if header[..8] != MAGIC {
        return Err(Rejected::Malformed);
    }
    let field = |at: usize| -> u64 {
        let mut buffer = [0_u8; 8];
        buffer.copy_from_slice(&header[at..at + 8]);
        u64::from_le_bytes(buffer)
    };
    let short = |at: usize| -> u32 {
        let mut buffer = [0_u8; 4];
        buffer.copy_from_slice(&header[at..at + 4]);
        u32::from_le_bytes(buffer)
    };

    if short(8) != VERSION {
        return Err(Rejected::Version);
    }

    let path_len = short(60) as usize;
    let path_at = HEADER_BYTES;
    let path = bytes
        .get(path_at..path_at.saturating_add(path_len))
        .ok_or(Rejected::Malformed)?;
    if field(12) != identity.table_id
        || field(20) != identity.data_file_id
        || field(28) != identity.file_size
        || path != identity.file_path.as_bytes()
    {
        return Err(Rejected::Identity);
    }

    Ok(Layout {
        set_at: path_at.saturating_add(path_len),
        set_len: usize::try_from(field(44)).map_err(|_| Rejected::Malformed)?,
        order_len: usize::try_from(field(52)).map_err(|_| Rejected::Malformed)?,
    })
}

/// The membership `bytes` describes, given a prefix covering the header,
/// the path and the set. The order, which follows, is neither read nor
/// needed.
pub(super) fn membership(
    identity: SidecarIdentity<'_>,
    bytes: &[u8],
) -> std::result::Result<FileRowSet, Rejected> {
    let layout = layout_of(identity, bytes)?;
    let set = bytes
        .get(layout.set_at..layout.membership_end())
        .ok_or(Rejected::Malformed)?;
    decode_membership(&mut &set[..]).map_err(|_| Rejected::Malformed)
}

/// The whole summary `bytes` describes, given all of it.
pub(super) fn summary(
    identity: SidecarIdentity<'_>,
    bytes: &[u8],
) -> std::result::Result<PositionedRowSet, Rejected> {
    let layout = layout_of(identity, bytes)?;
    let end = layout.membership_end().saturating_add(layout.order_len);
    let body = bytes.get(layout.set_at..end).ok_or(Rejected::Malformed)?;
    decode_summary(&mut &body[..]).map_err(|_| Rejected::Malformed)
}

/// What a sweep of published summaries did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SidecarSweep {
    /// Published summaries found under the prefix.
    pub considered: u64,
    /// Those whose data file was gone, and which were deleted with it.
    pub reclaimed: u64,
    /// Those a delete failed for, left for the next sweep.
    pub failed: u64,
}

/// Deletes every published summary under `prefix` whose data file is no
/// longer there. A sidecar dies with its file; this is what catches the
/// ones that outlived theirs.
///
/// Membership is decided from one listing rather than a probe per object,
/// so a sidecar is reclaimed only when the listing that found it did not
/// also find the file it names.
pub(crate) async fn sweep(store: &DataStore, prefix: &Path) -> Result<SidecarSweep> {
    let mut present = std::collections::HashSet::new();
    let mut sidecars = Vec::new();

    let mut listing = store.object_store().list(Some(prefix));
    while let Some(object) = listing.next().await {
        let object =
            object.map_err(|error| Error::Corruption(format!("listing sidecars: {error}")))?;
        let path = object.location.as_ref().to_owned();
        match path.strip_suffix(SUFFIX) {
            Some(described) => sidecars.push((object.location, described.to_owned())),
            None => {
                present.insert(path);
            }
        }
    }

    let mut swept = SidecarSweep {
        considered: u64::try_from(sidecars.len()).unwrap_or(u64::MAX),
        ..SidecarSweep::default()
    };
    for (sidecar, described) in sidecars {
        if present.contains(&described) {
            continue;
        }
        match store.object_store().delete(&sidecar).await {
            Ok(()) => swept.reclaimed = swept.reclaimed.saturating_add(1),
            Err(error) => {
                debug!(path = %sidecar, %error, "a published row summary could not be reclaimed");
                swept.failed = swept.failed.saturating_add(1);
            }
        }
    }

    Ok(swept)
}
