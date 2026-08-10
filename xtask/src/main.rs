//! Repo automation. Invoked as `cargo xtask <command>`.
//!
//! - `e2e` packages the extension and drives it through a real DuckDB CLI (see
//!   `e2e.rs`).
//! - `bench` compares DuckLake metadata catalogs — moraine's SlateDB store, a
//!   stock DuckDB file, and Postgres — on identical workloads (see `bench.rs`).
//! - `commit-bench` breaks an S3-backed DuckLake commit into DuckLake metadata
//!   statements, Moraine core time, and physical object-store requests (see
//!   `commit_bench.rs`).
//! - `s3` runs the catalog's object storage suite against a pinned MinIO server
//!   (see `s3.rs`).
//! - `check-pins` verifies every place naming a DuckDB version agrees with
//!   `.github/duckdb-versions` (see `pins.rs`), and `version-matrix` prints
//!   that manifest as the JSON array the release workflows build from.
//! - `check-release-assets <directory>` verifies a release carries a build for
//!   every supported version on every published platform (see `release.rs`).
//! - `bump-duckdb <version>` moves the primary pin to a new DuckDB release,
//!   writing every place `check-pins` checks (see `bump.rs`).
//! - `ducklake-patch` builds the repository's DuckLake patch as a loadable
//!   extension for moraine's primary DuckDB pin (see `ducklake_patch.rs`).

use anyhow::bail;

mod bench;
mod bump;
mod commit_bench;
mod duckdb;
mod ducklake_patch;
mod e2e;
mod pins;
mod release;
mod s3;

fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(2).collect();
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("e2e") => e2e::e2e(),
        Some("bench") => bench::bench(&arguments),
        Some("commit-bench") => commit_bench::run(&arguments),
        Some("s3") => s3::s3(),
        Some("ducklake-patch") => ducklake_patch::build(&arguments),
        Some("check-pins") => pins::check_pins(),
        Some("check-release-assets") => release::check_release_assets(&arguments),
        Some("bump-duckdb") => bump::bump_duckdb(&arguments),
        Some("version-matrix") => {
            pins::print_version_matrix();
            Ok(())
        }
        Some(other) => {
            bail!(
                "unknown task `{other}`; available: e2e, bench, commit-bench, s3, check-pins, \
                 check-release-assets, version-matrix, bump-duckdb, ducklake-patch"
            )
        }
        None => bail!(
            "usage: cargo xtask <task>; available: e2e, bench, commit-bench, s3, check-pins, \
             check-release-assets, version-matrix, bump-duckdb, ducklake-patch"
        ),
    }
}
