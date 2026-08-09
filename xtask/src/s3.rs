//! The `s3` task: downloads/caches pinned MinIO server and client
//! binaries, starts the server on a localhost port, and runs
//! `crates/moraine/tests/object_storage.rs` un-ignored against it — the
//! catalog's public API over a real S3 endpoint.

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail};

use crate::duckdb::{download, make_executable, workspace_root};

/// The pinned MinIO server release `s3` downloads and runs. Server and
/// client releases are tagged independently.
const MINIO_PIN: &str = "RELEASE.2025-09-07T16-13-09Z";

/// The pinned MinIO client (`mc`) release, used to create the test
/// bucket and as the server-readiness probe.
const MINIO_CLIENT_PIN: &str = "RELEASE.2025-08-13T08-35-41Z";

/// Not MinIO's default 9000, so the suite never collides with a locally
/// running MinIO.
const S3_ADDRESS: &str = "127.0.0.1:9124";

const S3_BUCKET: &str = "moraine";

/// Every `#[ignore]`d test in `object_storage.rs`, run together. A
/// deleted test or a changed `#[ignore]` fails `s3` instead of silently
/// shrinking the suite.
const OBJECT_STORAGE_TEST_COUNT: &str = "3 passed";

/// Downloads/caches the pinned MinIO server and client, starts the
/// server on `S3_ADDRESS` over a fresh data directory, creates the test
/// bucket, and runs `object_storage.rs` un-ignored against it. The
/// server is killed when this returns, pass or fail.
pub fn s3() -> anyhow::Result<()> {
    let minio_root = minio_root();

    let server_binary = ensure_minio_binary(&minio_root, "minio", MINIO_PIN, "server/minio")?;
    let client_binary = ensure_minio_binary(&minio_root, "mc", MINIO_CLIENT_PIN, "client/mc")?;
    println!(
        "ok: minio binaries under {}",
        minio_root.join("bin").display()
    );

    // A fresh data directory per run: no state leaks between runs.
    let data_dir = minio_root.join("data");
    if data_dir.exists() {
        fs::remove_dir_all(&data_dir)
            .with_context(|| format!("clearing stale data dir at {}", data_dir.display()))?;
    }
    fs::create_dir_all(&data_dir).with_context(|| format!("creating {}", data_dir.display()))?;

    // The server's own logs go to a file, kept out of the test output but
    // available when startup fails.
    let log_path = minio_root.join("server.log");
    let log =
        fs::File::create(&log_path).with_context(|| format!("creating {}", log_path.display()))?;
    let _server = KillOnDrop(
        Command::new(&server_binary)
            .arg("server")
            .arg(&data_dir)
            .args(["--address", S3_ADDRESS])
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            .stdout(
                log.try_clone()
                    .with_context(|| "duplicating the server log handle")?,
            )
            .stderr(log)
            .spawn()
            .with_context(|| format!("spawning {}", server_binary.display()))?,
    );

    create_bucket(&client_binary, &minio_root)
        .with_context(|| format!("server log: {}", log_path.display()))?;
    println!("ok: minio serving bucket `{S3_BUCKET}` on {S3_ADDRESS}");

    let endpoint = format!("http://{S3_ADDRESS}");
    // Release, single-threaded, and uncaptured: the suite carries a commit
    // latency measurement whose numbers a debug build or a parallel run
    // would make meaningless, and whose table is the point of running it.
    crate::duckdb::run_ignored_suite(
        "moraine",
        "object_storage",
        true,
        &["--test-threads=1", "--nocapture"],
        &[
            ("AWS_ACCESS_KEY_ID", OsStr::new("minioadmin")),
            ("AWS_SECRET_ACCESS_KEY", OsStr::new("minioadmin")),
            ("AWS_REGION", OsStr::new("us-east-1")),
            ("AWS_ALLOW_HTTP", OsStr::new("true")),
            ("MORAINE_S3_ENDPOINT", endpoint.as_ref()),
            ("MORAINE_S3_BUCKET", S3_BUCKET.as_ref()),
        ],
        OBJECT_STORAGE_TEST_COUNT,
    )?;
    println!("ok: the catalog round-tripped through a real S3 endpoint");

    Ok(())
}

/// Kills the child when dropped, so the MinIO server never outlives the
/// run — including failing ones.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Cache root for the MinIO binaries, data directory, and client
/// config, gitignored (`/target`) and never committed.
fn minio_root() -> PathBuf {
    workspace_root().join("target/minio")
}

/// Downloads and caches the pinned MinIO binary `name` into
/// `<root>/bin/<name>.<pin>`, skipping the download if it is already
/// cached. The pin-suffixed filename makes a pin bump miss the cache
/// naturally. `release_path` is the dl.min.io path segment (`server/minio`
/// or `client/mc`); the pinned URLs redirect to GitHub release assets,
/// which `download`'s `--location` follows.
fn ensure_minio_binary(
    root: &Path,
    name: &str,
    pin: &str,
    release_path: &str,
) -> anyhow::Result<PathBuf> {
    let bin_dir = root.join("bin");
    let binary = bin_dir.join(format!("{name}.{pin}"));
    if binary.exists() {
        return Ok(binary);
    }

    fs::create_dir_all(&bin_dir).with_context(|| format!("creating {}", bin_dir.display()))?;
    let platform = minio_download_platform()?;
    let url = format!("https://dl.min.io/{release_path}/release/{platform}/archive/{name}.{pin}");
    download(&url, &binary)?;

    #[cfg(unix)]
    make_executable(&binary)?;

    Ok(binary)
}

/// The dl.min.io platform tag for the host. Scoped to macOS and Linux,
/// matching the DuckDB CLI platform map in `duckdb.rs`.
fn minio_download_platform() -> anyhow::Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("darwin-arm64"),
        ("macos", "x86_64") => Ok("darwin-amd64"),
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("linux", "aarch64") => Ok("linux-arm64"),
        (os, arch) => {
            bail!("no pinned MinIO mapping for {os}/{arch}; add one in xtask/src/s3.rs")
        }
    }
}

/// `mc` doubles as the readiness probe: retry the alias until the server
/// answers, then create the test bucket. Config stays under `<root>` so
/// the run never touches `~/.mc`.
fn create_bucket(client_binary: &Path, root: &Path) -> anyhow::Result<()> {
    let config_dir = root.join("mc-config");
    let endpoint = format!("http://{S3_ADDRESS}");
    for _ in 0..30 {
        let aliased = Command::new(client_binary)
            .env("MC_CONFIG_DIR", &config_dir)
            .args([
                "alias",
                "set",
                "moraine",
                &endpoint,
                "minioadmin",
                "minioadmin",
            ])
            .output()
            .with_context(|| format!("spawning {}", client_binary.display()))?;
        if aliased.status.success() {
            let made = Command::new(client_binary)
                .env("MC_CONFIG_DIR", &config_dir)
                .args(["mb", "--ignore-existing", &format!("moraine/{S3_BUCKET}")])
                .output()
                .with_context(|| format!("spawning {}", client_binary.display()))?;
            if made.status.success() {
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    bail!("MinIO did not become ready on {S3_ADDRESS} within 30 seconds")
}
