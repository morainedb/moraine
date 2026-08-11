//! Builds the repository's DuckLake patch against moraine's primary DuckDB
//! pin without compiling DuckDB core.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
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
const ROW_ID_TEST_PATH: &str = "test/sql/rowid/ducklake_row_id_file_pruning.test";

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
    let artifact = build_artifact(arguments)?;
    println!("ok: patched DuckLake extension at {}", artifact.display());
    println!(
        "load it into DuckDB {} with `duckdb -unsigned`, then `LOAD '{}';`",
        duckdb::duckdb_pin(),
        artifact.display()
    );
    Ok(())
}

/// Builds and validates the patched extension and returns its artifact path.
pub fn build_artifact(arguments: &[String]) -> anyhow::Result<PathBuf> {
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
        "DuckDB's static library is missing at {}; build moraine once with `cargo xtask e2e` or \
         pass `--duckdb-static PATH`",
        options.duckdb_static.display(),
    );
    prepare_checkout(&paths.source, DUCKLAKE_URL, DUCKLAKE_REVISION, "DuckLake")?;
    apply_patch(&workspace, &paths.source)?;
    prepare_checkout(&paths.vcpkg, VCPKG_URL, VCPKG_REVISION, "vcpkg")?;
    bootstrap_vcpkg(&paths.vcpkg)?;

    let compilers = duckdb::cpp_compilers()?;
    reset_build_for_compiler_change(&paths.build, compilers.as_ref())?;
    let cmake_args = cmake_arguments(
        &workspace,
        &paths,
        &options.duckdb_static,
        duckdb::duckdb_pin(),
        compilers.as_ref(),
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
    run_row_id_regression(&workspace, &paths, &artifact)?;
    Ok(artifact)
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
    ensure_clean_checkout(&source, "DuckDB submodule")?;
    Ok(())
}

fn prepare_checkout(path: &Path, url: &str, revision: &str, name: &str) -> anyhow::Result<()> {
    let git_directory = path.join(".git");
    if git_directory.is_dir() && !git_directory.join("HEAD").exists() {
        println!(
            "repairing cached {name} checkout with stripped Git metadata at {}",
            path.display()
        );
        fs::remove_dir_all(path)
            .with_context(|| format!("removing incomplete cached {name} at {}", path.display()))?;
    }

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
    if checkout_matches_patch(source, &patch)? {
        return Ok(());
    }

    ensure_clean_checkout(source, "cached DuckLake checkout")?;

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
    ensure!(
        checkout_matches_patch(source, &patch)?,
        "applying {} did not produce its exact tracked diff",
        patch.display()
    );
    Ok(())
}

fn checkout_matches_patch(source: &Path, patch: &Path) -> anyhow::Result<bool> {
    let parent = source
        .parent()
        .context("the DuckLake source checkout has no parent")?;
    let indexes = TemporaryDirectory::create(parent, "patch-validation")?;
    let expected_index = indexes.path().join("expected.index");
    let actual_index = indexes.path().join("actual.index");

    initialize_temporary_index(source, &expected_index)?;
    command_output(
        Command::new("git")
            .args(["apply", "--cached", "--unidiff-zero"])
            .arg(patch)
            .env("GIT_INDEX_FILE", &expected_index)
            .current_dir(source),
    )?;
    let expected_tree = write_temporary_tree(source, &expected_index)?;

    initialize_temporary_index(source, &actual_index)?;
    command_output(
        Command::new("git")
            .args(["add", "--all", "--", "."])
            .env("GIT_INDEX_FILE", &actual_index)
            .current_dir(source),
    )?;
    let actual_tree = write_temporary_tree(source, &actual_index)?;

    Ok(expected_tree == actual_tree)
}

fn initialize_temporary_index(source: &Path, index: &Path) -> anyhow::Result<()> {
    command_output(
        Command::new("git")
            .args(["read-tree", "HEAD"])
            .env("GIT_INDEX_FILE", index)
            .current_dir(source),
    )?;
    Ok(())
}

fn write_temporary_tree(source: &Path, index: &Path) -> anyhow::Result<String> {
    command_output(
        Command::new("git")
            .arg("write-tree")
            .env("GIT_INDEX_FILE", index)
            .current_dir(source),
    )
    .map(|tree| tree.trim().to_string())
}

fn ensure_clean_checkout(source: &Path, name: &str) -> anyhow::Result<()> {
    let status = command_output(
        Command::new("git")
            .args(["status", "--porcelain=v1", "--untracked-files=all"])
            .current_dir(source),
    )?;
    ensure_clean_checkout_status(name, &status)
}

fn ensure_clean_checkout_status(name: &str, status: &str) -> anyhow::Result<()> {
    ensure!(
        status.trim().is_empty(),
        "{name} is dirty; commit, stash, or remove these changes before building:\n{status}"
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
    compilers: Option<&duckdb::CppCompilers>,
) -> Vec<String> {
    let mut arguments = vec![
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
    ];
    if let Some(compilers) = compilers {
        arguments.push(format!("-DCMAKE_C_COMPILER={}", compilers.c));
        arguments.push(format!("-DCMAKE_CXX_COMPILER={}", compilers.cxx));
    }
    arguments
}

fn reset_build_for_compiler_change(
    build: &Path,
    compilers: Option<&duckdb::CppCompilers>,
) -> anyhow::Result<()> {
    let (Some(compilers), cache) = (compilers, build.join("CMakeCache.txt")) else {
        return Ok(());
    };
    if !cache.exists() {
        return Ok(());
    }
    let contents = fs::read_to_string(&cache)
        .with_context(|| format!("reading CMake cache {}", cache.display()))?;
    if cmake_cache_uses_compilers(&contents, compilers) {
        return Ok(());
    }

    let metadata = fs::symlink_metadata(build)
        .with_context(|| format!("inspecting generated build tree {}", build.display()))?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "refusing to remove compiler-stale build tree symlink {}",
        build.display()
    );
    fs::remove_dir_all(build)
        .with_context(|| format!("removing compiler-stale build tree {}", build.display()))?;
    Ok(())
}

fn cmake_cache_uses_compilers(cache: &str, compilers: &duckdb::CppCompilers) -> bool {
    cached_compiler_name(cache, "CMAKE_C_COMPILER:") == Some(compilers.c)
        && cached_compiler_name(cache, "CMAKE_CXX_COMPILER:") == Some(compilers.cxx)
}

fn cached_compiler_name<'a>(cache: &'a str, key: &str) -> Option<&'a str> {
    let value = cache
        .lines()
        .find(|line| line.starts_with(key))?
        .split_once('=')?
        .1;
    Path::new(value).file_name()?.to_str()
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

fn preload_ducklake_test(script: &str) -> anyhow::Result<String> {
    const REQUIREMENT: &str = "require ducklake\n";
    ensure!(
        script.matches(REQUIREMENT).count() == 1,
        "{ROW_ID_TEST_PATH} must contain exactly one `{}` directive",
        REQUIREMENT.trim()
    );
    Ok(script.replacen(
        REQUIREMENT,
        "# ducklake is preloaded from the artifact under test\n",
        1,
    ))
}

fn run_row_id_regression(
    workspace: &Path,
    paths: &BuildPaths,
    artifact: &Path,
) -> anyhow::Result<()> {
    let runner = workspace.join("build/release/test/unittest");
    ensure!(
        runner.exists(),
        "the sqllogictest runner is missing at {}; `cargo xtask e2e` builds it",
        runner.display()
    );

    let source_test = paths.source.join(ROW_ID_TEST_PATH);
    let script = fs::read_to_string(&source_test)
        .with_context(|| format!("reading patched test {}", source_test.display()))?;
    let script = preload_ducklake_test(&script)?;
    let test_root = TemporaryDirectory::create(&paths.root, "row-id-regression")?;
    let copied_test = test_root.path().join(ROW_ID_TEST_PATH);
    let parent = copied_test
        .parent()
        .context("the row-ID regression path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    fs::write(&copied_test, script)
        .with_context(|| format!("writing {}", copied_test.display()))?;

    let artifact_literal = artifact.display().to_string().replace('\'', "''");
    let output = Command::new(&runner)
        .args(["--test-dir"])
        .arg(test_root.path())
        .arg(ROW_ID_TEST_PATH)
        .env("DUCKDB_TEST_ON_INIT", format!("LOAD '{artifact_literal}';"))
        .output()
        .with_context(|| format!("spawning {}", runner.display()))?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    ensure!(
        output.status.success(),
        "the patched DuckLake row-ID sqllogictest failed"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        stdout.contains("All tests passed") && stdout.contains("1 test case"),
        "the patched DuckLake row-ID sqllogictest did not report one passing case"
    );
    Ok(())
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn create(parent: &Path, name: &str) -> anyhow::Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system time is before the Unix epoch")?
            .as_nanos();
        let path = parent.join(format!(".{name}-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    fn commit_fixture(repository: &Path, name: &str, contents: &str) -> String {
        fs::write(repository.join(name), contents).expect("fixture file");
        command_output(
            Command::new("git")
                .args(["add", "."])
                .current_dir(repository),
        )
        .expect("stage fixture");
        command_output(
            Command::new("git")
                .args([
                    "-c",
                    "user.name=Moraine Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-m",
                    "fixture",
                ])
                .current_dir(repository),
        )
        .expect("commit fixture");
        command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repository),
        )
        .expect("fixture revision")
        .trim()
        .to_owned()
    }

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
    fn cache_with_stripped_nested_git_metadata_is_rebuilt() {
        let root = TemporaryDirectory::create(&std::env::temp_dir(), "stripped-git-test")
            .expect("temporary root");
        let origin = root.path().join("origin");
        fs::create_dir(&origin).expect("origin directory");
        command_output(Command::new("git").arg("init").current_dir(&origin))
            .expect("initialize origin");
        let revision = commit_fixture(&origin, "pinned.txt", "pinned\n");

        // Rust cache restoration can retain the nested `.git` directory but
        // strip its contents. Git then walks up to the workspace repository
        // and reports the PR merge commit as this checkout's HEAD.
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).expect("workspace directory");
        command_output(Command::new("git").arg("init").current_dir(&workspace))
            .expect("initialize workspace");
        let workspace_revision = commit_fixture(&workspace, "workspace.txt", "workspace\n");
        let source = workspace.join("target/patched-ducklake/source");
        fs::create_dir_all(source.join(".git")).expect("stripped git marker");
        fs::write(source.join("stale.txt"), "stale\n").expect("stale cache file");
        let escaped = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )
        .expect("parent revision escapes into nested cache");
        assert_eq!(escaped.trim(), workspace_revision);

        prepare_checkout(
            &source,
            origin.to_str().expect("UTF-8 origin path"),
            &revision,
            "fixture",
        )
        .expect("repair stripped cache");

        let head = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )
        .expect("repaired revision");
        assert_eq!(head.trim(), revision);
        assert!(source.join("pinned.txt").exists());
        assert!(!source.join("stale.txt").exists());

        let later_revision = commit_fixture(&origin, "later.txt", "later\n");
        let error = prepare_checkout(
            &source,
            origin.to_str().expect("UTF-8 origin path"),
            &later_revision,
            "fixture",
        )
        .expect_err("a real checkout at another revision stays protected");
        assert!(error.to_string().contains("cached fixture checkout"));
        let preserved = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )
        .expect("preserved revision");
        assert_eq!(preserved.trim(), revision);
    }

    #[test]
    fn cached_patch_requires_the_exact_resulting_tree() {
        let root = TemporaryDirectory::create(&std::env::temp_dir(), "patch-tree-test")
            .expect("temporary root");
        let source = root.path().join("source");
        fs::create_dir(&source).expect("source directory");
        command_output(Command::new("git").arg("init").current_dir(&source))
            .expect("initialize repository");
        command_output(
            Command::new("git")
                .args(["config", "diff.noprefix", "true"])
                .current_dir(&source),
        )
        .expect("configure alternate diff rendering");
        fs::write(source.join("source.cpp"), "old\n").expect("source file");
        fs::write(source.join("sibling.cpp"), "unchanged\n").expect("sibling file");
        command_output(Command::new("git").args(["add", "."]).current_dir(&source))
            .expect("stage baseline");
        command_output(
            Command::new("git")
                .args([
                    "-c",
                    "user.name=Moraine Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-m",
                    "baseline",
                ])
                .current_dir(&source),
        )
        .expect("commit baseline");

        let patch = root.path().join("change.patch");
        fs::write(
            &patch,
            "diff --git a/source.cpp b/source.cpp\n\
             --- a/source.cpp\n\
             +++ b/source.cpp\n\
             @@ -1 +1 @@\n\
             -old\n\
             +new\n",
        )
        .expect("patch file");
        fs::write(source.join("source.cpp"), "new\n").expect("apply expected edit");

        assert!(checkout_matches_patch(&source, &patch).expect("matching tree"));

        fs::write(source.join("sibling.cpp"), "extra edit\n").expect("extra tracked edit");
        assert!(!checkout_matches_patch(&source, &patch).expect("extra tracked edit"));

        fs::write(source.join("sibling.cpp"), "unchanged\n").expect("restore sibling");
        fs::write(source.join("untracked.cpp"), "extra\n").expect("extra untracked file");
        assert!(!checkout_matches_patch(&source, &patch).expect("extra untracked file"));
    }

    #[test]
    fn dirty_pinned_checkout_is_rejected() {
        let error = ensure_clean_checkout_status("DuckDB submodule", " M CMakeLists.txt\n")
            .expect_err("tracked edits must fail");

        assert!(error.to_string().contains("DuckDB submodule is dirty"));
        assert!(ensure_clean_checkout_status("DuckDB submodule", "").is_ok());
    }

    #[test]
    fn patched_regression_preloads_only_ducklake() {
        let script = "require ducklake\n\nrequire parquet\n\nstatement ok\nSELECT 1\n";
        let transformed = preload_ducklake_test(script).expect("DuckLake requirement");

        assert!(!transformed.contains("require ducklake"));
        assert!(transformed.contains("require parquet"));
        assert!(transformed.contains("statement ok\nSELECT 1"));
    }

    #[test]
    fn cmake_cache_compilers_must_match_the_selected_pair() {
        let selected = duckdb::CppCompilers {
            c: "gcc14-gcc",
            cxx: "gcc14-g++",
        };
        let gcc14 = "CMAKE_C_COMPILER:FILEPATH=/usr/bin/gcc14-gcc\n\
                     CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/gcc14-g++\n";
        let gcc11 = "CMAKE_C_COMPILER:FILEPATH=/usr/bin/gcc\n\
                     CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/g++\n";

        assert!(cmake_cache_uses_compilers(gcc14, &selected));
        assert!(!cmake_cache_uses_compilers(gcc11, &selected));
    }

    #[test]
    fn extension_only_configuration_reuses_the_prebuilt_duckdb_core() {
        let paths = BuildPaths::new(PathBuf::from("/work"));
        let arguments = cmake_arguments(
            Path::new("/repo"),
            &paths,
            Path::new("/repo/build/libduckdb_static.a"),
            "v1.5.5",
            Some(&duckdb::CppCompilers {
                c: "gcc-14",
                cxx: "g++-14",
            }),
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
        assert!(
            arguments
                .iter()
                .any(|arg| arg == "-DCMAKE_C_COMPILER=gcc-14")
        );
        assert!(
            arguments
                .iter()
                .any(|arg| arg == "-DCMAKE_CXX_COMPILER=g++-14")
        );
    }
}
