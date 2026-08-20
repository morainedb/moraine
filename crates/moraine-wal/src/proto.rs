//! Generated protobuf wire messages (see `proto/wal.proto` and `build.rs`).

#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::doc_markdown,
    clippy::module_name_repetitions
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/moraine.wal.rs"));
}

pub(crate) use generated::{
    CommitValue, EnvelopeValue, LeaderAdvertValue, SlotPayloadValue, SlotWriteValue,
};
