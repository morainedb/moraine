//! Drives a real, pinned DuckDB CLI + the `ducklake` extension against a
//! store pre-seeded through the `moraine` API, proving the whole nested
//! attach chain: `ATTACH 'ducklake:moraine:<dir>' AS lake (DATA_PATH
//! '<dir2>')` resolves DuckLake's metadata connection through this shim's
//! `moraine:` prefix dispatch and synthesized `ducklake_*` tables, and
//! DuckLake's own reader — not this crate's scan — serves the data back.
//!
//! Ignored by default: needs the downloaded DuckDB CLI, the packaged Moraine
//! extension, and the patched DuckLake extension. Run manually after
//! `cargo xtask e2e` has produced the artifacts once:
//!
//! ```text
//! MORAINE_DUCKDB_CLI=target/duckdb-cli/cli/duckdb \
//! MORAINE_DUCKDB_EXT=build/release/extension/moraine/moraine.duckdb_extension \
//! MORAINE_DUCKLAKE_EXT=target/patched-ducklake/build-extension-static/extension/ducklake/ducklake.duckdb_extension \
//! cargo test -p moraine-duckdb --release --test ducklake_load -- --ignored
//! ```

// The tests-exempt lints (`clippy.toml`) reach `#[test]` functions and
// `#[cfg(test)]` modules, not an integration crate's plain helper
// functions — exempted here instead, crate-wide, as tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

mod helpers;

mod attach;
mod change_feed;
mod checkpoints;
mod commit_protocol;
mod crash;
mod ddl;
mod deletes;
mod index;
mod inline;
mod maintenance;
mod migrate;
mod multi_writer;
mod partitioning;
mod time_travel;
mod types;
mod wire_contract;
