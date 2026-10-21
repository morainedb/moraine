//! Repo automation. Invoked as `cargo xtask <command>`.
//!
//! - `fmt-cpp [--check]` formats or checks handwritten C/C++ sources.
//! - `tidy-cpp` configures the extension and runs clang-tidy on handwritten
//!   sources.
//!
//! `session-bench` compares the pinned pre-review, post-review, and SlateDB
//! upgrade revisions with shared core and SQL workloads.
//!
//! - `e2e` packages the extension and drives it through a real DuckDB CLI (see
//!   `e2e.rs`).
//! - `bench` compares DuckLake metadata catalogs — moraine's SlateDB store, a
//!   stock DuckDB file, and Postgres — on identical workloads (see `bench.rs`).
//! - `clickbench` runs ClickBench against a moraine-backed DuckLake through
//!   upstream's own driver and ranks the result against the numbers published
//!   at <https://benchmark.clickhouse.com> (see `clickbench.rs`).
//! - `commit-bench` breaks an S3-backed DuckLake commit into DuckLake metadata
//!   statements, Moraine core time, and physical object-store requests (see
//!   `commit_bench.rs`).
//! - `s3` runs the catalog's object storage suite against a pinned RustFS
//!   server (see `s3.rs`).
//! - `check-pins` verifies every place naming a DuckDB version agrees with
//!   `.github/duckdb-versions` (see `pins.rs`).
//! - `check-patch-pins` applies the DuckLake patch series to every source
//!   `patches/ducklake/source-pins` names, which `e2e` also does.
//! - `filter-bench <extension> <results-directory>` measures wide located
//!   filters against clustered, scattered, and broad matches.
//! - `check-release-assets <directory>` verifies a release carries a build for
//!   every supported version on every published platform (see `release.rs`).
//! - `validate-release-artifact <version> <artifact>` proves one published
//!   extension's bundled DuckLake records row-ID statistics and prunes files.
//! - `bump-duckdb <version>` moves the primary pin to a new DuckDB release,
//!   writing every place `check-pins` checks (see `bump.rs`).
//! - `ducklake-patch` builds the repository's DuckLake patch series as a
//!   standalone loadable, which only `session-bench`'s pre-bundling revisions
//!   need (see `ducklake_patch.rs`).

use anyhow::bail;

mod bench;
mod bump;
mod catalog_target;
mod clickbench;
mod commit_bench;
mod cpp;
mod duckdb;
mod ducklake_patch;
mod e2e;
mod filter_bench;
mod locate_bench;
mod pins;
mod reader_bench;
mod release;
mod s3;
mod session_bench;

fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(2).collect();
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("fmt-cpp") => cpp::format(&arguments),
        Some("tidy-cpp") => cpp::tidy(&arguments),
        Some("e2e") => e2e::e2e(),
        Some("build-extension") => e2e::build_extension(),
        Some("bench") => bench::bench(&arguments),
        Some("clickbench") => clickbench::run(&arguments),
        Some("commit-bench") => commit_bench::run(&arguments),
        Some("locate-bench") => locate_bench::run(&arguments),
        Some("filter-bench") => filter_bench::run(&arguments),
        Some("reader-bench") => reader_bench::run(&arguments),
        Some("session-bench") => session_bench::run(&arguments),
        Some("s3") => s3::s3(),
        Some("ducklake-patch") => ducklake_patch::build(&arguments),
        Some("check-pins") => pins::check_pins(),
        Some("check-patch-pins") => ducklake_patch::check_pins(),
        Some("check-release-assets") => release::check_release_assets(&arguments),
        Some("stage-release-assets") => release::stage_release_assets(&arguments),
        Some("validate-release-artifact") => release::validate_release_artifact(&arguments),
        Some("bump-duckdb") => bump::bump_duckdb(&arguments),
        Some(other) => {
            bail!(
                "unknown task `{other}`; available: fmt-cpp, tidy-cpp, e2e, build-extension, bench, clickbench, commit-bench, locate-bench, filter-bench, reader-bench, session-bench, s3, check-pins, \
                 check-patch-pins, check-release-assets, stage-release-assets, validate-release-artifact, \
                 bump-duckdb, ducklake-patch"
            )
        }
        None => bail!(
            "usage: cargo xtask <task>; available: fmt-cpp, tidy-cpp, e2e, build-extension, bench, clickbench, commit-bench, locate-bench, filter-bench, reader-bench, session-bench, s3, check-pins, \
             check-patch-pins, check-release-assets, stage-release-assets, validate-release-artifact, \
             bump-duckdb, ducklake-patch"
        ),
    }
}
