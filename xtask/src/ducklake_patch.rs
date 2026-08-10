//! Builds the repository's DuckLake patch against moraine's primary DuckDB
//! pin without compiling DuckDB core.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, bail, ensure};

use crate::duckdb;

const DUCKLAKE_URL: &str = "https://github.com/duckdb/ducklake.git";
const DUCKLAKE_REVISION: &str = "d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4";
const SUPPORTED_DUCKDB_PIN: &str = "v1.5.5";
const VCPKG_URL: &str = "https://github.com/microsoft/vcpkg.git";
const VCPKG_REVISION: &str = "ea1a7396b05637a53bf23c078647ecc0edee4b80";
const PATCH_PATH: &str = "patches/ducklake/0001-perf-prune-DuckLake-files-by-row-id.patch";
const CONFIG_PATH: &str = "patches/ducklake/extension_config.cmake";
const PATCHED_FILES: [&str; 17] = [
    "src/functions/ducklake_flush_inlined_data.cpp",
    "src/metadata_manager/quack_metadata_manager.cpp",
    "src/storage/ducklake_insert.cpp",
    "src/storage/ducklake_multi_file_list.cpp",
    "src/storage/ducklake_server_side_commit.cpp",
    "src/storage/ducklake_stats.cpp",
    "src/storage/ducklake_transaction.cpp",
    "test/sql/add_files/add_files_complex_nested_stats_mre.test",
    "test/sql/add_files/add_files_nested_list_struct_nulls.test",
    "test/sql/geo/ducklake_geometry.test",
    "test/sql/geo/ducklake_geometry_add_files.test",
    "test/sql/geo/ducklake_geometry_nested_list.test",
    "test/sql/geo/ducklake_geometry_nested_map.test",
    "test/sql/geo/ducklake_geometry_nested_struct.test",
    "test/sql/metadata/appender_data_files.test",
    "test/sql/rowid/ducklake_row_id_file_pruning.test",
    "test/sql/stats/variant_shredded_stats.test",
];

#[derive(Debug, PartialEq, Eq)]
struct Options {
    root: PathBuf,
    duckdb_static: PathBuf,
}

#[derive(Debug)]
struct BuildPaths {
    root: PathBuf,
    source: PathBuf,
    vcpkg: PathBuf,
    build: PathBuf,
}

impl BuildPaths {
    fn new(root: PathBuf) -> Self {
        Self {
            source: root.join("source"),
            vcpkg: root.join("vcpkg"),
            build: root.join("build-extension-static"),
            root,
        }
    }

    fn artifact(&self) -> PathBuf {
        self.build
            .join("extension/ducklake/ducklake.duckdb_extension")
    }
}

/// Fetches the pinned DuckLake source and vcpkg, applies the tracked patch,
/// and builds only the loadable DuckLake extension.
pub fn build(arguments: &[String]) -> anyhow::Result<()> {
    let workspace = duckdb::workspace_root();
    let options = parse_arguments(&workspace, arguments)?;
    let paths = BuildPaths::new(options.root);

    ensure!(
        duckdb::duckdb_pin() == SUPPORTED_DUCKDB_PIN,
        "the DuckLake patch targets DuckDB {SUPPORTED_DUCKDB_PIN}, but moraine's primary pin is {}",
        duckdb::duckdb_pin()
    );
    ensure!(
        workspace.join(PATCH_PATH).exists(),
        "the DuckLake patch is missing at {}",
        workspace.join(PATCH_PATH).display()
    );

    fs::create_dir_all(&paths.root)
        .with_context(|| format!("creating {}", paths.root.display()))?;
    validate_duckdb_checkout(&workspace)?;
    ensure!(
        options.duckdb_static.exists(),
        "DuckDB's static library is missing at {}; build moraine once with `make release GEN=ninja \
         OVERRIDE_GIT_DESCRIBE={}` or pass `--duckdb-static PATH`",
        options.duckdb_static.display(),
        duckdb::duckdb_pin()
    );
    prepare_checkout(&paths.source, DUCKLAKE_URL, DUCKLAKE_REVISION, "DuckLake")?;
    apply_patch(&workspace, &paths.source)?;
    prepare_checkout(&paths.vcpkg, VCPKG_URL, VCPKG_REVISION, "vcpkg")?;
    bootstrap_vcpkg(&paths.vcpkg)?;

    let cmake_args = cmake_arguments(
        &workspace,
        &paths,
        &options.duckdb_static,
        duckdb::duckdb_pin(),
    );
    let mut configure = Command::new("cmake");
    configure.args(&cmake_args);
    duckdb::run(&mut configure)?;

    duckdb::run(
        Command::new("cmake")
            .args(["--build"])
            .arg(&paths.build)
            .args([
                "--target",
                "ducklake_loadable_extension",
                "--config",
                "Release",
            ]),
    )?;

    let artifact = paths.artifact();
    ensure!(
        artifact.exists(),
        "DuckLake build completed but {} is missing",
        artifact.display()
    );
    verify_loadable(&artifact)?;
    println!("ok: patched DuckLake extension at {}", artifact.display());
    println!(
        "load it into DuckDB {} with `duckdb -unsigned`, then `LOAD '{}';`",
        duckdb::duckdb_pin(),
        artifact.display()
    );
    Ok(())
}

fn parse_arguments(workspace: &Path, arguments: &[String]) -> anyhow::Result<Options> {
    let mut root = workspace.join("target/patched-ducklake");
    let mut duckdb_static = workspace.join("build/release/src/libduckdb_static.a");
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--root" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .context("`--root` requires a directory")?;
                let requested = PathBuf::from(value);
                root = if requested.is_absolute() {
                    requested
                } else {
                    workspace.join(requested)
                };
            }
            "--duckdb-static" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .context("`--duckdb-static` requires a file")?;
                let requested = PathBuf::from(value);
                duckdb_static = if requested.is_absolute() {
                    requested
                } else {
                    workspace.join(requested)
                };
            }
            unknown => bail!(
                "unknown argument `{unknown}`; usage: ducklake-patch \
                 [--root DIRECTORY] [--duckdb-static FILE]"
            ),
        }
        index += 1;
    }
    Ok(Options {
        root,
        duckdb_static,
    })
}

fn validate_duckdb_checkout(workspace: &Path) -> anyhow::Result<()> {
    let expected = duckdb::primary_submodule_pins()
        .into_iter()
        .find_map(|(path, revision)| (path == "duckdb").then_some(revision))
        .context("the primary DuckDB manifest entry has no `duckdb` submodule pin")?;
    let source = workspace.join("duckdb");
    ensure!(
        source.join("CMakeLists.txt").exists(),
        "the DuckDB submodule is missing; run `git submodule update --init duckdb`"
    );
    let actual = command_output(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&source),
    )?;
    ensure!(
        actual.trim() == expected,
        "DuckDB submodule is {}, expected {}; run `git submodule update --init duckdb`",
        actual.trim(),
        expected
    );
    Ok(())
}

fn prepare_checkout(path: &Path, url: &str, revision: &str, name: &str) -> anyhow::Result<()> {
    if !path.join(".git").exists() {
        ensure_directory_is_empty(path, name)?;
        fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
        duckdb::run(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(path),
        )?;
        duckdb::run(
            Command::new("git")
                .args(["remote", "add", "origin", url])
                .current_dir(path),
        )?;
    }

    let head = optional_command_output(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(path),
    )?;
    if head.is_none() {
        duckdb::run(
            Command::new("git")
                .args(["fetch", "--depth", "1", "origin", revision])
                .current_dir(path),
        )?;
        duckdb::run(
            Command::new("git")
                .args(["switch", "--quiet", "--detach", "FETCH_HEAD"])
                .current_dir(path),
        )?;
    }

    let head = command_output(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(path),
    )?;
    ensure!(
        head.trim() == revision,
        "cached {name} checkout at {} is {}, expected {}; choose another `--root` or remove the cache",
        path.display(),
        head.trim(),
        revision
    );
    Ok(())
}

fn ensure_directory_is_empty(path: &Path, name: &str) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(
        entries.next().is_none(),
        "{} exists but is not a {name} checkout; choose another `--root` or remove the cache",
        path.display()
    );
    Ok(())
}

fn apply_patch(workspace: &Path, source: &Path) -> anyhow::Result<()> {
    let patch = workspace.join(PATCH_PATH);
    if command_succeeds(
        Command::new("git")
            .args(["apply", "--unidiff-zero", "--reverse", "--check"])
            .arg(&patch)
            .current_dir(source),
    )? {
        verify_patched_files(source)?;
        return Ok(());
    }

    duckdb::run(
        Command::new("git")
            .args(["apply", "--unidiff-zero", "--check"])
            .arg(&patch)
            .current_dir(source),
    )?;
    duckdb::run(
        Command::new("git")
            .args(["apply", "--unidiff-zero"])
            .arg(&patch)
            .current_dir(source),
    )?;
    verify_patched_files(source)
}

fn verify_patched_files(source: &Path) -> anyhow::Result<()> {
    let changed = command_output(
        Command::new("git")
            .args(["diff", "--name-only"])
            .current_dir(source),
    )?;
    let mut actual: Vec<&str> = changed.lines().collect();
    let mut expected = PATCHED_FILES.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    ensure!(
        actual == expected,
        "patched DuckLake checkout has unexpected changes: {}",
        actual.join(", ")
    );
    Ok(())
}

fn bootstrap_vcpkg(vcpkg: &Path) -> anyhow::Result<()> {
    let executable = if cfg!(windows) {
        vcpkg.join("vcpkg.exe")
    } else {
        vcpkg.join("vcpkg")
    };
    if executable.exists() {
        return Ok(());
    }

    if cfg!(windows) {
        duckdb::run(
            Command::new("cmd")
                .args(["/C", "bootstrap-vcpkg.bat", "-disableMetrics"])
                .current_dir(vcpkg),
        )
    } else {
        duckdb::run(
            Command::new("sh")
                .args(["bootstrap-vcpkg.sh", "-disableMetrics"])
                .current_dir(vcpkg),
        )
    }
}

fn cmake_arguments(
    workspace: &Path,
    paths: &BuildPaths,
    duckdb_static: &Path,
    duckdb_pin: &str,
) -> Vec<String> {
    vec![
        "-G".to_string(),
        "Ninja".to_string(),
        "-S".to_string(),
        workspace.join("duckdb").display().to_string(),
        "-B".to_string(),
        paths.build.display().to_string(),
        "-DCMAKE_BUILD_TYPE=Release".to_string(),
        "-DBUILD_EXTENSIONS_ONLY=TRUE".to_string(),
        "-DEXTENSION_STATIC_BUILD=TRUE".to_string(),
        format!("-DPREBUILT_BINARY={}", duckdb_static.display()),
        format!(
            "-DDUCKDB_EXTENSION_CONFIGS={}",
            workspace.join(CONFIG_PATH).display()
        ),
        format!("-DDUCKLAKE_PATCH_SOURCE={}", paths.source.display()),
        format!(
            "-DCMAKE_TOOLCHAIN_FILE={}",
            paths
                .vcpkg
                .join("scripts/buildsystems/vcpkg.cmake")
                .display()
        ),
        format!("-DVCPKG_MANIFEST_DIR={}", paths.source.display()),
        format!("-DOVERRIDE_GIT_DESCRIBE={duckdb_pin}"),
    ]
}

fn verify_loadable(artifact: &Path) -> anyhow::Result<()> {
    let cli = duckdb::ensure_duckdb_cli()?;
    let artifact_literal = artifact.display().to_string().replace('\'', "''");
    duckdb::run(
        Command::new(cli)
            .arg("-unsigned")
            .arg("-c")
            .arg(format!("LOAD '{artifact_literal}';")),
    )
    .context("the patched DuckLake artifact compiled but did not load in the pinned DuckDB CLI")
}

fn command_output(command: &mut Command) -> anyhow::Result<String> {
    let output = command
        .output()
        .with_context(|| format!("spawning {command:?}"))?;
    ensure!(
        output.status.success(),
        "{command:?} exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).context("command output was not UTF-8")
}

fn optional_command_output(command: &mut Command) -> anyhow::Result<Option<String>> {
    let output = command
        .output()
        .with_context(|| format!("spawning {command:?}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    String::from_utf8(output.stdout)
        .map(Some)
        .context("command output was not UTF-8")
}

fn command_succeeds(command: &mut Command) -> anyhow::Result<bool> {
    let status = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("spawning {command:?}"))?;
    Ok(status.success())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    #[test]
    fn default_output_stays_under_target() {
        let workspace = Path::new("/repo");
        let options = parse_arguments(workspace, &[]).expect("default arguments");

        assert_eq!(options.root, PathBuf::from("/repo/target/patched-ducklake"));
        assert_eq!(
            options.duckdb_static,
            PathBuf::from("/repo/build/release/src/libduckdb_static.a")
        );
    }

    #[test]
    fn output_override_may_live_elsewhere() {
        let workspace = Path::new("/repo");
        let arguments = vec!["--root".to_string(), "/tmp/ducklake".to_string()];
        let options = parse_arguments(workspace, &arguments).expect("explicit root");

        assert_eq!(options.root, PathBuf::from("/tmp/ducklake"));
    }

    #[test]
    fn static_library_override_is_resolved_from_the_workspace() {
        let workspace = Path::new("/repo");
        let arguments = vec![
            "--duckdb-static".to_string(),
            "artifacts/libduckdb.a".to_string(),
        ];
        let options = parse_arguments(workspace, &arguments).expect("static archive override");

        assert_eq!(
            options.duckdb_static,
            PathBuf::from("/repo/artifacts/libduckdb.a")
        );
    }

    #[test]
    fn unknown_arguments_are_rejected() {
        let workspace = Path::new("/repo");
        let error = parse_arguments(workspace, &["--mystery".to_string()])
            .expect_err("unknown argument must fail");

        assert!(error.to_string().contains("unknown argument `--mystery`"));
    }

    #[test]
    fn extension_only_configuration_reuses_the_prebuilt_duckdb_core() {
        let paths = BuildPaths::new(PathBuf::from("/work"));
        let arguments = cmake_arguments(
            Path::new("/repo"),
            &paths,
            Path::new("/repo/build/libduckdb_static.a"),
            "v1.5.5",
        );

        assert!(
            arguments
                .iter()
                .any(|arg| arg == "-DBUILD_EXTENSIONS_ONLY=TRUE")
        );
        assert!(
            arguments
                .iter()
                .any(|arg| arg == "-DEXTENSION_STATIC_BUILD=TRUE")
        );
        assert!(
            arguments
                .iter()
                .any(|arg| arg == "-DPREBUILT_BINARY=/repo/build/libduckdb_static.a")
        );
        assert!(
            arguments
                .iter()
                .any(|arg| arg == "-DOVERRIDE_GIT_DESCRIBE=v1.5.5")
        );
    }
}
