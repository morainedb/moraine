//! Slot framing: every slot object begins with a 4-byte magic and a 1-byte
//! encoding version, then the payload. A wrong magic or a version newer
//! than this reader supports fails as [`Error::Corruption`].

use crate::error::Error;

/// The 4-byte slot-object magic.
pub(crate) const MAGIC: [u8; 4] = *b"MWAL";

/// The slot encoding version this binary writes and the newest it reads.
pub(crate) const ENCODING_VERSION: u8 = 0;

/// Total header length: magic + version byte.
pub(crate) const HEADER_LEN: usize = MAGIC.len() + 1;

/// A buffer holding the framing header, with capacity reserved for a
/// `payload_len`-byte payload to append directly.
pub(crate) fn header_buf(payload_len: usize) -> Vec<u8> {
    let mut framed = Vec::with_capacity(HEADER_LEN + payload_len);
    framed.extend_from_slice(&MAGIC);
    framed.push(ENCODING_VERSION);
    framed
}

/// Prepend the framing header to a payload. Test-only fixture helper;
/// production code frames through [`header_buf`].
#[cfg(test)]
pub(crate) fn frame(payload: &[u8]) -> Vec<u8> {
    let mut framed = header_buf(payload.len());
    framed.extend_from_slice(payload);

    framed
}

/// Strip and validate the framing header, returning the payload.
pub(crate) fn unframe(bytes: &[u8]) -> Result<&[u8], Error> {
    let (header, payload) = bytes
        .split_at_checked(HEADER_LEN)
        .ok_or_else(|| Error::corruption("slot: truncated framing header".to_string()))?;
    if header[..MAGIC.len()] != MAGIC {
        return Err(Error::corruption("slot: bad magic".to_string()));
    }

    let version = header[MAGIC.len()];
    if version > ENCODING_VERSION {
        return Err(Error::corruption(format!(
            "slot: encoding version {version} is newer than supported {ENCODING_VERSION}"
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
        assert_eq!(&framed[..4], b"MWAL");
        assert_eq!(framed[4], ENCODING_VERSION);
        assert_eq!(&framed[5..], b"payload");
    }

    #[test]
    fn unframe_rejects_corrupt_magic() {
        let mut framed = frame(b"payload");
        framed[0] = b'X';
        let err = unframe(&framed).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    #[test]
    fn unframe_rejects_truncated_header() {
        for len in 0..5 {
            let framed = frame(b"payload");
            assert!(
                unframe(&framed[..len]).is_err(),
                "header truncated to {len} bytes must not decode"
            );
        }
    }

    #[test]
    fn unframe_rejects_newer_encoding_version() {
        let mut framed = frame(b"payload");
        framed[4] = ENCODING_VERSION + 1;
        let err = unframe(&framed).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    #[test]
    fn empty_payload_roundtrips() {
        assert_eq!(unframe(&frame(b"")).unwrap(), b"");
    }

    proptest! {
        #[test]
        fn roundtrip(payload in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let framed = frame(&payload);
            prop_assert_eq!(unframe(&framed).unwrap(), payload.as_slice());
        }

        // Unframe is total: arbitrary bytes unframe or fail as
        // `Corruption`, never panic.
        #[test]
        fn unframe_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let _ = unframe(&bytes);
        }
    }
}
