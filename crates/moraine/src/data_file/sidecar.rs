//! A published row summary: the bytes the auxiliary cache's disk tier
//! holds, written beside the data file they describe.

use std::io::Write;

use object_store::path::Path;

use super::{
    auxiliary_cache::{decode_summary, encode_summary_halves},
    row_set::PositionedRowSet,
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
pub(crate) struct SidecarIdentity<'a> {
    pub(crate) table_id: u64,
    pub(crate) data_file_id: u64,
    pub(crate) file_path: &'a str,
    pub(crate) file_size: u64,
}

/// Why a sidecar did not answer. Every variant means the same thing to a
/// caller — read the data file instead — and they are separate so a
/// deployment can tell "none published yet" from "published and refused".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejected {
    /// Too short to hold what it claims, or malformed within.
    Malformed,
    /// A version this reader does not know.
    Version,
    /// A header naming some other data file.
    Identity,
}

/// Where `data_file`'s sidecar lives: beside it, under the same prefix.
pub(crate) fn path_for(data_file: &Path) -> Result<Path> {
    Path::parse(format!("{data_file}{SUFFIX}"))
        .map_err(|error| Error::Corruption(format!("invalid sidecar path: {error}")))
}

/// The bytes describing `rows`, for the file `identity` names.
pub(crate) fn encode(identity: SidecarIdentity<'_>, rows: &PositionedRowSet) -> Result<Vec<u8>> {
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
pub(crate) struct Layout {
    /// Where the membership begins.
    pub(crate) set_at: usize,
    pub(crate) set_len: usize,
    pub(crate) order_len: usize,
}

impl Layout {
    /// The prefix a membership-only read must cover.
    pub(crate) fn membership_end(&self) -> usize {
        self.set_at.saturating_add(self.set_len)
    }
}

/// Reads the header of `bytes`, which need only be long enough to hold it,
/// and refuses one that does not describe the file `identity` names.
pub(crate) fn layout_of(
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

/// The whole summary `bytes` describes, given all of it.
pub(crate) fn summary(
    identity: SidecarIdentity<'_>,
    bytes: &[u8],
) -> std::result::Result<PositionedRowSet, Rejected> {
    let layout = layout_of(identity, bytes)?;
    let end = layout.membership_end().saturating_add(layout.order_len);
    let body = bytes.get(layout.set_at..end).ok_or(Rejected::Malformed)?;
    decode_summary(&mut &body[..]).map_err(|_| Rejected::Malformed)
}
