//! The read path is total: an arbitrary key/value pair decodes into a
//! record or fails as an error, never a panic.
//!
//! This is the half the codec proptests do not reach. They cover keys in
//! isolation; a scan decodes a *pair*, letting the key choose which value
//! message the bytes are parsed as. A key that decodes into one subspace
//! and a value that is another subspace's message is exactly the shape
//! store damage takes, and it must be refused rather than fatal.
//!
//! The input is split rather than taken as two `Arbitrary` fields so the
//! fuzzer's byte-level mutations reach both halves and the split point
//! itself.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // A leading length byte lets the fuzzer move the boundary between the
    // two halves, so it can grow a key it likes while reshaping the value.
    let Some((&split, rest)) = data.split_first() else {
        return;
    };
  
    let at = usize::from(split).min(rest.len());
    let (key, value) = rest.split_at(at);
    moraine::fuzz::decode_record(key, value);
});
