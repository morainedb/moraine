//! Compiles the commit-slot log's protobuf schemas with `protox` (a
//! pure-Rust protobuf front-end feeding `prost-build`).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/wal.proto");
    let descriptors = protox::compile(["proto/wal.proto"], ["proto/"])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}
