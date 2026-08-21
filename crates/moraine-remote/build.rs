//! Compiles the forwarded-session protocol's protobuf schema with `protox`
//! (a pure-Rust protobuf front-end feeding `prost-build`).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/remote.proto");
    let descriptors = protox::compile(["proto/remote.proto"], ["proto/"])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}
