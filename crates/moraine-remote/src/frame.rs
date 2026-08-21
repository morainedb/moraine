//! Message framing: every framed message begins with a 4-byte magic and a
//! 1-byte encoding version, then the protobuf payload. A wrong magic or a
//! version newer than this reader supports fails as [`Error::Protocol`].

use crate::error::Error;

/// The 4-byte message magic.
pub(crate) const MAGIC: [u8; 4] = *b"MRMT";

/// The encoding version this binary writes and the newest it reads.
pub(crate) const ENCODING_VERSION: u8 = 0;

/// Total header length: magic + version byte.
pub(crate) const HEADER_LEN: usize = MAGIC.len() + 1;

/// A ceiling on a single framed message, guarding a reader against a length
/// prefix that would allocate unboundedly.
pub(crate) const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Prepend the framing header to a payload, returning the framed bytes a
/// length-prefixed write emits.
pub(crate) fn frame(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(HEADER_LEN + payload.len());
    framed.extend_from_slice(&MAGIC);
    framed.push(ENCODING_VERSION);
    framed.extend_from_slice(payload);

    framed
}

/// Strip and validate the framing header, returning the payload.
pub(crate) fn unframe(bytes: &[u8]) -> Result<&[u8], Error> {
    let (header, payload) = bytes
        .split_at_checked(HEADER_LEN)
        .ok_or_else(|| Error::Protocol("truncated framing header".to_string()))?;
    if header[..MAGIC.len()] != MAGIC {
        return Err(Error::Protocol("bad magic".to_string()));
    }

    let version = header[MAGIC.len()];
    if version > ENCODING_VERSION {
        return Err(Error::Protocol(format!(
            "encoding version {version} is newer than supported {ENCODING_VERSION}"
        )));
    }

    Ok(payload)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn frame_prepends_magic_and_version() {
        let framed = frame(b"payload");
        assert_eq!(&framed[..4], b"MRMT");
        assert_eq!(framed[4], ENCODING_VERSION);
        assert_eq!(&framed[5..], b"payload");
    }

    #[test]
    fn unframe_rejects_corrupt_magic() {
        let mut framed = frame(b"payload");
        framed[0] = b'X';
        assert!(matches!(unframe(&framed), Err(Error::Protocol(_))));
    }

    #[test]
    fn unframe_rejects_truncated_header() {
        for len in 0..HEADER_LEN {
            let framed = frame(b"payload");
            assert!(
                unframe(&framed[..len]).is_err(),
                "len {len} must not decode"
            );
        }
    }

    #[test]
    fn unframe_rejects_newer_encoding_version() {
        let mut framed = frame(b"payload");
        framed[4] = ENCODING_VERSION + 1;
        assert!(matches!(unframe(&framed), Err(Error::Protocol(_))));
    }

    proptest! {
        #[test]
        fn roundtrip(payload in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let framed = frame(&payload);
            prop_assert_eq!(unframe(&framed).unwrap(), payload.as_slice());
        }

        // Unframe is total: arbitrary bytes unframe or fail as `Protocol`,
        // never panic.
        #[test]
        fn unframe_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let _ = unframe(&bytes);
        }
    }
}
