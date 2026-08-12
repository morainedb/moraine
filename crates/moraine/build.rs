//! Compiles the store's protobuf value schemas with `protox` (a pure-Rust
//! protobuf front-end feeding `prost-build`).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/moraine.proto");
    let descriptors = protox::compile(["proto/moraine.proto"], ["proto/"])?;
    let mut config = prost_build::Config::new();
    config.bytes([
        ".moraine.store.InlineSchemaValue.arrow_schema",
        ".moraine.store.InlineChunkValue.body",
    ]);
    // Test builds derive proptest strategies for every message, so the
    // per-message roundtrip property tests need no hand-written strategies.
    config.type_attribute(".", "#[cfg_attr(test, derive(proptest_derive::Arbitrary))]");
    let bytes_strategy = "#[cfg_attr(test, proptest(strategy = \"proptest::strategy::Strategy::prop_map(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256), bytes::Bytes::from)\"))]";
    config.field_attribute(
        ".moraine.store.InlineSchemaValue.arrow_schema",
        bytes_strategy,
    );
    config.field_attribute(".moraine.store.InlineChunkValue.body", bytes_strategy);
    config.compile_fds(descriptors)?;
    Ok(())
}
