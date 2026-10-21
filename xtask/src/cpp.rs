//! Formatting and static analysis of the repository's C and C++ sources.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Instant,
};

use anyhow::{Context, anyhow, ensure};
use serde::Deserialize;

use crate::{duckdb, ducklake_patch};

const VERSION: &str = include_str!("../../.clang-tools-version");
const SOURCE_DIRECTORIES: [&str; 3] = [
    "crates/moraine-duckdb/cpp",
    "crates/moraine-duckdb/tests/cpp",
    "xtask/cpp",
];

/// Formats handwritten sources, or checks them with `--check`.
pub fn format(arguments: &[String]) -> anyhow::Result<()> {
    ensure!(
        arguments.is_empty() || arguments == ["--check"],
        "usage: cargo xtask fmt-cpp [--check]"
    );
    let tool = tool("clang-format")?;
    let sources = sources()?;
    let mut command = Command::new(tool);
    if arguments.is_empty() {
        command.arg("-i");
    } else {
        command.args(["--dry-run", "--Werror"]);
    }
    duckdb::run(command.args(&sources).current_dir(duckdb::workspace_root()))
}

/// Configures the extension and checks its handwritten sources with clang-tidy.
pub fn tidy(arguments: &[String]) -> anyhow::Result<()> {
    ensure!(arguments.is_empty(), "usage: cargo xtask tidy-cpp");
    let tool = tool("clang-tidy")?;
    let started = Instant::now();
    let root = duckdb::workspace_root();
    let patched = ducklake_patch::prepare()?;
    duckdb::configure_extension(&patched)?;
    let database = root.join("build/release");
    let commands: Vec<CompileCommand> = serde_json::from_slice(
        &fs::read(database.join("compile_commands.json"))
            .context("reading the configured extension's compilation database")?,
    )?;
    let mut sources: Vec<_> = sources()?
        .into_iter()
        .filter(|path| {
            matches!(
                path.extension().and_then(|part| part.to_str()),
                Some("c" | "cpp")
            )
        })
        .collect();
    let shim: Vec<_> = sources
        .iter()
        .filter(|path| path.starts_with(SOURCE_DIRECTORIES[0]))
        .cloned()
        .collect();
    check_coverage(&root, &shim, &commands)?;

    sources.retain(|source| {
        let skip =
            cfg!(target_os = "macos") && source == Path::new("xtask/cpp/concurrent_writers.c");
        if skip {
            println!("clang-tidy: skipping concurrent_writers.c (macOS has no pthread barriers)");
        }
        !skip
    });
    let workers = thread::available_parallelism()
        .map_or(1, usize::from)
        .min(4);
    let next = AtomicUsize::new(0);
    let success = thread::scope(|scope| -> anyhow::Result<bool> {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut success = true;
                    while let Some(source) = sources.get(next.fetch_add(1, Ordering::Relaxed)) {
                        if let Err(error) = analyze(&tool, &root, &database, source) {
                            eprintln!("{error:#}");
                            success = false;
                        }
                    }
                    success
                })
            })
            .collect();
        let mut success = true;
        for handle in handles {
            success &= handle
                .join()
                .map_err(|_| anyhow!("clang-tidy worker panicked"))?;
        }
        Ok(success)
    })?;
    println!(
        "clang-tidy: {} sources checked with {workers} workers in {:.1}s (including configuration)",
        sources.len(),
        started.elapsed().as_secs_f64()
    );
    ensure!(success, "clang-tidy failed; see diagnostics above");
    Ok(())
}

fn analyze(tool: &Path, root: &Path, database: &Path, source: &Path) -> anyhow::Result<()> {
    let mut command = Command::new(tool);
    command.current_dir(root).arg(source);
    if source.starts_with(SOURCE_DIRECTORIES[0]) {
        command.arg("-p").arg(database);
    } else {
        // Standalone drivers are compiled directly by xtask, outside CMake.
        command.arg("--");
        if source
            .extension()
            .is_some_and(|extension| extension == "cpp")
        {
            command.arg("-std=c++17");
        }
        command
            .arg("-I")
            .arg(root.join("duckdb/src/include"))
            .arg("-I")
            .arg(root.join(SOURCE_DIRECTORIES[0]))
            .arg("-pthread");
    }
    let output = command
        .output()
        .with_context(|| format!("spawning {command:?}"))?;
    // Emit each file's diagnostics together so worker output cannot interleave.
    print!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        output.status.success(),
        "clang-tidy failed for {}",
        source.display()
    );
    Ok(())
}

fn tool(name: &str) -> anyhow::Result<PathBuf> {
    let installed = duckdb::workspace_root()
        .join("target/clang-tools/bin")
        .join(name);
    let executable = if installed.is_file() {
        installed
    } else {
        PathBuf::from(name)
    };
    let output = Command::new(&executable)
        .arg("--version")
        .output()
        .with_context(|| {
            format!("running {name}; install the tools documented in CONTRIBUTING.md")
        })?;
    let version = String::from_utf8_lossy(&output.stdout);
    ensure!(
        output.status.success()
            && version
                .split_whitespace()
                .any(|word| word == VERSION.trim()),
        "{name} {} is required; found {}. See CONTRIBUTING.md",
        VERSION.trim(),
        version.trim()
    );
    Ok(executable)
}

fn sources() -> anyhow::Result<Vec<PathBuf>> {
    let root = duckdb::workspace_root();
    let output = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
        ])
        .args(SOURCE_DIRECTORIES)
        .current_dir(&root)
        .output()
        .context("listing C/C++ sources")?;
    ensure!(output.status.success(), "git ls-files failed");
    let listing = String::from_utf8(output.stdout)?;
    let mut paths: Vec<_> = listing
        .split('\0')
        .map(PathBuf::from)
        .filter(|path| handwritten_source(path) && root.join(path).is_file())
        .collect();
    paths.sort();
    paths.dedup();
    ensure!(!paths.is_empty(), "no handwritten C/C++ sources found");
    Ok(paths)
}

fn handwritten_source(path: &Path) -> bool {
    SOURCE_DIRECTORIES
        .iter()
        .any(|directory| path.starts_with(directory))
        && path != Path::new("crates/moraine-duckdb/cpp/moraine_abi.h")
        && matches!(
            path.extension().and_then(|part| part.to_str()),
            Some("c" | "cpp" | "h" | "hpp")
        )
}

#[derive(Deserialize)]
struct CompileCommand {
    directory: PathBuf,
    file: PathBuf,
}

fn check_coverage(
    root: &Path,
    sources: &[PathBuf],
    commands: &[CompileCommand],
) -> anyhow::Result<()> {
    for source in sources {
        let source = root.join(source).canonicalize()?;
        ensure!(
            commands.iter().any(|command| {
                command
                    .directory
                    .join(&command.file)
                    .canonicalize()
                    .is_ok_and(|path| path == source)
            }),
            "{} has no compile command; add it to CMakeLists.txt",
            source.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_handwritten_sources_and_headers_only() {
        for path in [
            "crates/moraine-duckdb/cpp/catalog.cpp",
            "crates/moraine-duckdb/cpp/detail/new.hpp",
            "crates/moraine-duckdb/tests/cpp/s3_credentials.cpp",
            "xtask/cpp/concurrent_writers.c",
            "xtask/cpp/driver.h",
        ] {
            assert!(handwritten_source(Path::new(path)), "{path}");
        }
        for path in [
            "crates/moraine-duckdb/cpp/moraine_abi.h",
            "crates/moraine-duckdb/cpp/README.md",
            "crates/moraine-duckdb/src/abi.rs",
            "duckdb/src/main.cpp",
            "target/generated.cpp",
            "xtask/cpp-other/driver.c",
        ] {
            assert!(!handwritten_source(Path::new(path)), "{path}");
        }
    }

    #[test]
    fn missing_compile_command_fails_before_analysis() {
        let root = duckdb::workspace_root();
        let source = PathBuf::from("crates/moraine-duckdb/cpp/catalog.cpp");
        let unrelated = CompileCommand {
            directory: root.clone(),
            file: PathBuf::from("xtask/cpp/concurrent_writers.c"),
        };
        let error = check_coverage(&root, &[source], &[unrelated]).unwrap_err();
        assert!(error.to_string().contains("has no compile command"));
    }

    #[test]
    fn compile_commands_accept_absolute_and_directory_relative_files() {
        let root = duckdb::workspace_root();
        let source = PathBuf::from("crates/moraine-duckdb/cpp/catalog.cpp");
        for file in [source.clone(), root.join(&source)] {
            let command = CompileCommand {
                directory: root.clone(),
                file,
            };
            check_coverage(&root, std::slice::from_ref(&source), &[command]).unwrap();
        }
    }
}
