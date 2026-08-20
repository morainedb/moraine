//! Key decoding is total: arbitrary bytes decode into a key or fail as an
//! error, never a panic.
//!
//! The codec's proptests already assert this over uniform random bytes and
//! over single-byte corruptions of valid keys. What this adds is
//! coverage-guided depth: the fuzzer steers toward the discriminants,
//! lengths, and component boundaries a uniform sampler reaches only by
//! accident, and keeps a corpus of what it found.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    moraine::fuzz::decode_key(data);
});
