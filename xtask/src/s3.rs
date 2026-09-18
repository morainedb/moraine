//! The `s3` task: downloads/caches a pinned RustFS server binary, starts
//! it on a localhost port, and runs
//! `crates/moraine/tests/object_storage.rs` un-ignored against it — the
//! catalog's public API over a real S3 endpoint.

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use anyhow::{Context, bail, ensure};

use crate::duckdb::{download, make_executable, run, workspace_root};

/// The pinned RustFS release `s3` downloads and runs.
const RUSTFS_PIN: &str = "1.0.0";

/// Not the 9000 an S3 server usually defaults to, so the suite never
/// collides with one already running locally.
const S3_ADDRESS: &str = "127.0.0.1:9124";

const S3_BUCKET: &str = "moraine";

/// The server's only credentials, for a server reachable only on
/// localhost and thrown away with the run.
const S3_ACCESS_KEY: &str = "moraineadmin";
const S3_SECRET_KEY: &str = "moraineadmin";

/// Every `#[ignore]`d test in `object_storage.rs`, run together. A
/// deleted test or a changed `#[ignore]` fails `s3` instead of silently
/// shrinking the suite.
const OBJECT_STORAGE_TEST_COUNT: &str = "6 passed";

/// Downloads/caches the pinned RustFS server, starts it on `S3_ADDRESS`
/// over a fresh data directory, creates the test bucket, and runs
/// `object_storage.rs` un-ignored against it. The server is killed when
/// this returns, pass or fail.
pub fn s3() -> anyhow::Result<()> {
    let root = rustfs_root();

    let server_binary = ensure_rustfs_binary(&root)?;
    println!("ok: rustfs {RUSTFS_PIN} at {}", server_binary.display());

    // A fresh data directory per run: no state leaks between runs.
    let data_dir = root.join("data");
    if data_dir.exists() {
        fs::remove_dir_all(&data_dir)
            .with_context(|| format!("clearing stale data dir at {}", data_dir.display()))?;
    }
    fs::create_dir_all(&data_dir).with_context(|| format!("creating {}", data_dir.display()))?;

    // The server's own logs go to a file, kept out of the test output but
    // available when startup fails.
    let log_path = root.join("server.log");
    let log =
        fs::File::create(&log_path).with_context(|| format!("creating {}", log_path.display()))?;
    let _server = KillOnDrop(
        Command::new(&server_binary)
            .arg("server")
            .args(["--address", S3_ADDRESS])
            .args(["--access-key", S3_ACCESS_KEY])
            .args(["--secret-key", S3_SECRET_KEY])
            .arg(&data_dir)
            .stdout(
                log.try_clone()
                    .with_context(|| "duplicating the server log handle")?,
            )
            .stderr(log)
            .spawn()
            .with_context(|| format!("spawning {}", server_binary.display()))?,
    );

    create_bucket(&log_path)?;
    println!("ok: rustfs serving bucket `{S3_BUCKET}` on {S3_ADDRESS}");

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
            ("AWS_ACCESS_KEY_ID", OsStr::new(S3_ACCESS_KEY)),
            ("AWS_SECRET_ACCESS_KEY", OsStr::new(S3_SECRET_KEY)),
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

/// Kills the child when dropped, so the server never outlives the run —
/// including failing ones.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Cache root for the rustfs binary and data directory, gitignored
/// (`/target`) and never committed.
fn rustfs_root() -> PathBuf {
    workspace_root().join("target/rustfs")
}

/// Downloads and caches the pinned RustFS server into
/// `<root>/bin/rustfs.<pin>`, skipping the download if it is already
/// cached. The pin-suffixed filename makes a pin bump miss the cache
/// naturally.
fn ensure_rustfs_binary(root: &Path) -> anyhow::Result<PathBuf> {
    let bin_dir = root.join("bin");
    let binary = bin_dir.join(format!("rustfs.{RUSTFS_PIN}"));
    if binary.exists() {
        return Ok(binary);
    }

    fs::create_dir_all(&bin_dir).with_context(|| format!("creating {}", bin_dir.display()))?;
    let asset = format!("rustfs-{}-v{RUSTFS_PIN}.zip", rustfs_download_platform()?);
    let release = format!("https://github.com/rustfs/rustfs/releases/download/{RUSTFS_PIN}");

    let archive = bin_dir.join(&asset);
    download(&format!("{release}/{asset}"), &archive)?;
    let sums = bin_dir.join(format!("SHA256SUMS.{RUSTFS_PIN}"));
    download(&format!("{release}/SHA256SUMS"), &sums)?;
    verify_checksum(&archive, &sums, &asset)?;

    // Unpacked beside the archive, then renamed: a half-written unpack
    // never looks like a cached binary to the next run.
    let staged = bin_dir.join(format!("staged.{RUSTFS_PIN}"));
    if staged.exists() {
        fs::remove_dir_all(&staged).with_context(|| format!("clearing {}", staged.display()))?;
    }
    run(Command::new("unzip")
        .args(["-o", "-q"])
        .arg(&archive)
        .arg("-d")
        .arg(&staged))?;

    let unpacked = staged.join("rustfs");
    ensure!(
        unpacked.exists(),
        "unzipped {} but {} is still missing",
        archive.display(),
        unpacked.display()
    );
    fs::rename(&unpacked, &binary)
        .with_context(|| format!("moving the server into {}", binary.display()))?;

    // The archive is larger than the binary and nothing reads it again.
    let _ = fs::remove_dir_all(&staged);
    let _ = fs::remove_file(&archive);

    #[cfg(unix)]
    make_executable(&binary)?;

    Ok(binary)
}

/// Verifies `archive` against its line in a downloaded `SHA256SUMS`, so a
/// truncated or substituted download fails here rather than later as a
/// confusing server error.
fn verify_checksum(archive: &Path, sums: &Path, asset: &str) -> anyhow::Result<()> {
    let listing = fs::read_to_string(sums)
        .with_context(|| format!("reading checksums from {}", sums.display()))?;
    let expected = listing
        .lines()
        .find_map(|line| {
            let (hash, name) = line.split_once("  ")?;
            (name.trim() == asset).then_some(hash.trim())
        })
        .with_context(|| format!("{asset} has no entry in {}", sums.display()))?;

    let actual = sha256_of(archive)?;
    ensure!(
        actual == expected,
        "checksum mismatch for {asset}: got {actual}, expected {expected}"
    );
    Ok(())
}

/// The SHA-256 of `file`, through whichever checksum tool the platform
/// ships: `sha256sum` on Linux, `shasum` on macOS. Both print the digest
/// as the first word.
fn sha256_of(file: &Path) -> anyhow::Result<String> {
    for (tool, leading) in [("sha256sum", &[][..]), ("shasum", &["-a", "256"][..])] {
        let Ok(output) = Command::new(tool).args(leading).arg(file).output() else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        if let Some(digest) = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .next()
        {
            return Ok(digest.to_owned());
        }
    }
    bail!("no SHA-256 tool on PATH; looked for `sha256sum` and `shasum`")
}

/// The RustFS release asset tag for the host. Scoped to the platforms
/// RustFS publishes, which is narrower than the extension supports.
fn rustfs_download_platform() -> anyhow::Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("macos-aarch64"),
        ("linux", "x86_64") => Ok("linux-x86_64-musl"),
        ("linux", "aarch64") => Ok("linux-aarch64-musl"),
        // RustFS publishes no macOS x86_64 asset, so this task cannot run
        // on an Intel Mac. The rest of the suite is unaffected.
        ("macos", "x86_64") => bail!(
            "RustFS publishes no macOS x86_64 build, so `cargo xtask s3` \
             needs Apple Silicon or Linux; CI runs it on every push"
        ),
        (os, arch) => {
            bail!("no pinned rustfs mapping for {os}/{arch}; add one in xtask/src/s3.rs")
        }
    }
}

/// Creates the test bucket, retrying until the server answers — which
/// doubles as the readiness probe. `curl` signs the request itself, so
/// the task needs no S3 client binary.
fn create_bucket(log_path: &Path) -> anyhow::Result<()> {
    let url = format!("http://{S3_ADDRESS}/{S3_BUCKET}");
    let credentials = format!("{S3_ACCESS_KEY}:{S3_SECRET_KEY}");

    for _ in 0..60 {
        let made = Command::new("curl")
            .args(["--silent", "--show-error", "--fail"])
            .args(["--aws-sigv4", "aws:amz:us-east-1:s3"])
            .args(["--user", &credentials])
            .args(["-X", "PUT", &url])
            .output()
            .with_context(|| "spawning `curl` to create the test bucket")?;
        if made.status.success() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    bail!(
        "rustfs did not serve {S3_ADDRESS} within 60 seconds; server log: {}",
        log_path.display()
    )
}
