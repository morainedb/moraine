//! Shared DuckDB plumbing for xtask commands: downloading and caching the
//! pinned CLI, and building the loadable `.duckdb_extension` through
//! DuckDB's own extension toolchain (`make release`).

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::OnceLock,
};

use anyhow::{Context, bail, ensure};

/// The DuckDB versions manifest, read once so every consumer sees one
/// copy.
fn duckdb_versions_manifest() -> &'static str {
    static MANIFEST: OnceLock<String> = OnceLock::new();
    MANIFEST.get_or_init(|| {
        fs::read_to_string(workspace_root().join(".github/duckdb-versions")).unwrap_or_default()
    })
}

/// The manifest's content lines, comments and blanks dropped, newest
/// first.
fn manifest_entries() -> impl Iterator<Item = &'static str> {
    duckdb_versions_manifest()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
}

/// Every DuckDB release moraine builds for, newest first, as
/// `.github/duckdb-versions` lists them.
pub fn supported_duckdb_versions() -> Vec<String> {
    manifest_entries()
        .filter_map(|entry| entry.split_whitespace().next())
        .map(str::to_owned)
        .collect()
}

/// The `(submodule path, commit)` pins the primary entry carries — the
/// two submodules the local build compiles against. Empty for any entry
/// but the first: CI checks DuckDB out by tag for those and never touches
/// a submodule.
pub fn primary_submodule_pins() -> Vec<(String, String)> {
    manifest_entries()
        .next()
        .into_iter()
        .flat_map(str::split_whitespace)
        .skip(1)
        .filter_map(|pin| pin.split_once('='))
        .map(|(path, commit)| (path.to_owned(), commit.to_owned()))
        .collect()
}

/// The primary DuckDB pin: the version the `duckdb` and
/// `extension-ci-tools` submodules sit on, that the extension is built
/// against locally, and whose CLI xtask downloads to load the artifact.
/// The first entry of `.github/duckdb-versions`.
///
/// An empty manifest yields an empty string, which every consumer then
/// fails on with its own message — `check-pins` first among them.
pub fn duckdb_pin() -> &'static str {
    manifest_entries()
        .next()
        .and_then(|entry| entry.split_whitespace().next())
        .unwrap_or_default()
}

const DUCKDB_RELEASE_BASE_URL: &str = "https://github.com/duckdb/duckdb/releases/download";

/// Runs `cmd`, failing with its exact invocation and exit status if it
/// didn't succeed. Stdio is inherited, so the child's own output (e.g.
/// `curl`'s error text) reaches the caller.
pub fn run(cmd: &mut Command) -> anyhow::Result<()> {
    let status = cmd.status().with_context(|| format!("spawning {cmd:?}"))?;
    ensure!(status.success(), "{cmd:?} exited with {status}");
    Ok(())
}

/// The GCC 14 executables used for Linux C++ builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CppCompilers {
    /// C compiler executable.
    pub c: &'static str,
    /// C++ compiler executable.
    pub cxx: &'static str,
}

fn select_linux_gcc14(mut available: impl FnMut(&str) -> bool) -> Option<CppCompilers> {
    [("gcc-14", "g++-14"), ("gcc14-gcc", "gcc14-g++")]
        .into_iter()
        .find(|(c, cxx)| available(c) && available(cxx))
        .map(|(c, cxx)| CppCompilers { c, cxx })
}

/// Resolves the compiler pair for a DuckDB C++ build.
///
/// Linux uses the same GCC 14 generation as DuckLake's extension pipeline.
/// Other platforms retain their native compiler.
pub fn cpp_compilers() -> anyhow::Result<Option<CppCompilers>> {
    if !cfg!(target_os = "linux") {
        return Ok(None);
    }

    let available = |executable: &str| {
        Command::new(executable)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    };
    select_linux_gcc14(available).map(Some).context(
        "GCC 14 is required for Linux C++ builds; install `gcc-14 g++-14` (Debian/Ubuntu) or \
         `gcc14 gcc14-c++` (Amazon Linux)",
    )
}

/// Runs one package's `#[ignore]`d test target with `envs` set, echoing
/// both output streams, and fails unless the run succeeds **and** reports
/// `expected_count` — a filter matching nothing still exits 0 ("0
/// passed"), which would let a deleted or renamed test pass vacuously.
pub fn run_ignored_suite(
    package: &str,
    test_target: &str,
    release: bool,
    extra_args: &[&str],
    envs: &[(&str, &std::ffi::OsStr)],
    expected_count: &str,
) -> anyhow::Result<()> {
    let mut command = Command::new("cargo");
    command.args(["test", "-p", package]);
    if release {
        command.arg("--release");
    }
    command.args(["--test", test_target, "--", "--ignored"]);
    command.args(extra_args);
    for (key, value) in envs {
        command.env(key, value);
    }

    let output = command
        .output()
        .with_context(|| format!("spawning the {test_target} integration test"))?;
    // Stdio isn't inherited (unlike `run`), so the count can be checked
    // below; echo both streams so failures stay visible on the console.
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    ensure!(
        output.status.success(),
        "{test_target} integration test failed"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        stdout.contains(expected_count),
        "expected {test_target} to report `{expected_count}`; a test may have been deleted, \
         renamed, or its #[ignore] removed/changed. Got:\n{stdout}"
    );
    Ok(())
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// Cache root for the downloaded CLI, gitignored (`/target`) and never
/// committed.
///
/// Keyed by version: a CLI only loads an extension built for its own
/// version string, so a pin bump must miss the cache rather than serve
/// the release it downloaded before.
fn duckdb_cli_root() -> PathBuf {
    workspace_root()
        .join("target/duckdb-cli")
        .join(duckdb_pin())
}

/// Cache root for `INSTALL ducklake`-style downloaded extensions,
/// gitignored under `target/`.
pub fn extension_install_directory() -> PathBuf {
    workspace_root().join("target/duckdb-extensions")
}

fn cli_binary_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "duckdb.exe"
    } else {
        "duckdb"
    }
}

/// Downloads and unpacks the pinned DuckDB CLI for the host platform into
/// the gitignored cache, skipping both the download and the unpack if the
/// binary is already cached there. Returns the CLI's path.
///
/// The cache is keyed by the pinned version, so a bump downloads afresh
/// instead of handing back the previous release — which would build the
/// extension for the new version and then fail to load it into the old
/// CLI, reported as a load error rather than a stale cache.
pub fn ensure_duckdb_cli() -> anyhow::Result<PathBuf> {
    let duckdb_root = duckdb_cli_root();
    let cli_dir = duckdb_root.join("cli");
    let cli_path = cli_dir.join(cli_binary_name());
    if cli_path.exists() {
        return Ok(cli_path);
    }

    let pin = duckdb_pin();
    let platform = cli_download_platform(pin)?;
    let zip_name = format!("duckdb_cli-{platform}.zip");
    let zip_path = duckdb_root.join(&zip_name);
    if !zip_path.exists() {
        fs::create_dir_all(&duckdb_root)
            .with_context(|| format!("creating {}", duckdb_root.display()))?;
        let url = format!("{DUCKDB_RELEASE_BASE_URL}/{pin}/{zip_name}");
        download(&url, &zip_path)?;
    }

    fs::create_dir_all(&cli_dir).with_context(|| format!("creating {}", cli_dir.display()))?;
    run(Command::new("unzip")
        .args(["-o", "-q"])
        .arg(&zip_path)
        .arg("-d")
        .arg(&cli_dir))?;
    ensure!(
        cli_path.exists(),
        "unzipped {} but {} is still missing",
        zip_path.display(),
        cli_path.display()
    );

    #[cfg(unix)]
    make_executable(&cli_path)?;

    Ok(cli_path)
}

/// Builds the extension through DuckDB's extension toolchain (`make
/// release`) and returns the path to the loadable `.duckdb_extension`. The
/// toolchain statically links DuckDB, links moraine's Rust core, and writes
/// the metadata footer; this replaces the old cdylib-plus-hand-written-
/// footer packaging. Requires `ninja` on PATH (the toolchain generator).
pub fn build_and_package_extension() -> anyhow::Result<PathBuf> {
    // Stamp the artifact with the pinned DuckDB version explicitly.
    let override_describe = format!("OVERRIDE_GIT_DESCRIBE={}", duckdb_pin());
    let compilers = cpp_compilers()?;
    // CMake clears its cache and reconfigures when an existing build tree
    // changes compiler. That internal reconfigure drops the extension-config
    // argument, so run the idempotent Make target again to restore it. A fresh
    // build makes the second invocation a no-op.
    for _ in 0..2 {
        let mut build = Command::new("make");
        build
            .args(["release", "GEN=ninja", &override_describe])
            .current_dir(workspace_root());
        if let Some(compilers) = &compilers {
            build
                .env("CC", compilers.c)
                .env("CXX", compilers.cxx)
                .arg(format!(
                    "EXT_FLAGS=-DCMAKE_C_COMPILER={} -DCMAKE_CXX_COMPILER={}",
                    compilers.c, compilers.cxx
                ));
        }
        run(&mut build)?;
    }
    let artifact =
        workspace_root().join("build/release/extension/moraine/moraine.duckdb_extension");
    ensure!(
        artifact.exists(),
        "`make release` finished but the loadable extension is missing at {}",
        artifact.display()
    );
    Ok(artifact)
}

/// Downloads `url` to `dest` via the system `curl`, removing any partial
/// file on failure. Fails with a message naming the URL and hinting at
/// connectivity issues when offline; curl's own diagnostics still reach
/// stderr.
pub fn download(url: &str, dest: &Path) -> anyhow::Result<()> {
    println!("downloading {url}");
    let outcome = Command::new("curl")
        .args(["--fail", "--location", "--show-error", "-o"])
        .arg(dest)
        .arg(url)
        .status();
    match outcome {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let _ = fs::remove_file(dest);
            bail!(
                "curl exited with {status} downloading {url}; if this machine is offline, \
                 cache the zip at that path manually and rerun"
            )
        }
        Err(e) => {
            let _ = fs::remove_file(dest);
            bail!("failed to run `curl` (required to download the DuckDB CLI): {e}")
        }
    }
}

#[cfg(unix)]
pub fn make_executable(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = fs::metadata(path)
        .with_context(|| format!("reading permissions of {}", path.display()))?
        .permissions();
    perms.set_mode(perms.mode() | 0o111);
    fs::set_permissions(path, perms)
        .with_context(|| format!("marking {} executable", path.display()))?;
    Ok(())
}

/// The `duckdb_cli-<platform>.zip` platform tag for the host, matching
/// the URL scheme in `crates/moraine-duckdb/README.md`. Scoped to macOS
/// and Linux, the platforms the extension supports.
fn cli_download_platform(pin: &str) -> anyhow::Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("osx-arm64"),
        // No standalone macOS x86_64 asset is published; the universal
        // binary covers both architectures.
        ("macos", "x86_64") => Ok("osx-universal"),
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("linux", "aarch64") => Ok("linux-arm64"),
        (os, arch) => bail!(
            "no pinned DuckDB {pin} CLI mapping for {os}/{arch}; add one in \
             xtask/src/duckdb.rs (see crates/moraine-duckdb/README.md's CLI URL list)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_compiler_prefers_debian_names() {
        let compilers = select_linux_gcc14(|name| matches!(name, "gcc-14" | "g++-14"));

        assert_eq!(
            compilers,
            Some(CppCompilers {
                c: "gcc-14",
                cxx: "g++-14",
            })
        );
    }

    #[test]
    fn linux_compiler_accepts_amazon_names() {
        let compilers = select_linux_gcc14(|name| matches!(name, "gcc14-gcc" | "gcc14-g++"));

        assert_eq!(
            compilers,
            Some(CppCompilers {
                c: "gcc14-gcc",
                cxx: "gcc14-g++",
            })
        );
    }

    #[test]
    fn linux_compiler_requires_a_complete_pair() {
        assert_eq!(select_linux_gcc14(|name| name == "gcc-14"), None);
    }
}
