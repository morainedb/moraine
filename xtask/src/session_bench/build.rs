//! Builds revision artifacts sequentially, reusing the release dependency
//! cache.

use std::{fmt::Write as _, fs, path::Path, process::Command};

use anyhow::{Context, ensure};

use super::{Options, REVISIONS};

pub(super) fn logged(command: &mut Command, path: &Path) -> anyhow::Result<()> {
    let log = fs::File::create(path)?;
    let status = command.stdout(log.try_clone()?).stderr(log).status()?;
    ensure!(
        status.success(),
        "{command:?} failed; see {}",
        path.display()
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn run(options: &Options) -> anyhow::Result<()> {
    let workspace = crate::duckdb::workspace_root().canonicalize()?;
    let cli = crate::duckdb::ensure_duckdb_cli()?;
    fs::write(
        options.root.join("cli-path.txt"),
        cli.to_string_lossy().as_bytes(),
    )?;
    let ducklake = workspace.join("target/patched-ducklake/build-extension-static/extension/ducklake/ducklake.duckdb_extension");
    ensure!(
        ducklake.exists(),
        "build patched DuckLake with cargo xtask e2e first"
    );
    fs::copy(&ducklake, options.root.join("ducklake.duckdb_extension"))?;
    let mut manifest = String::from("label,commit\n");
    for (label, revision) in REVISIONS {
        println!("Building {label} ({revision})");
        let checkout = options.root.join(format!("source-{label}"));
        if !checkout.exists() {
            logged(
                Command::new("git")
                    .args(["worktree", "add", "--detach"])
                    .arg(&checkout)
                    .arg(revision),
                &options.root.join(format!("checkout-{label}.log")),
            )?;
        }
        let hash = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&checkout)
            .output()?;
        ensure!(hash.status.success(), "reading revision failed");
        let hash = String::from_utf8(hash.stdout)?;
        ensure!(
            hash.trim().starts_with(revision),
            "checkout {label} is at an unexpected revision"
        );
        writeln!(manifest, "{label},{}", hash.trim())?;
        prepare(&workspace, &checkout)?;
        refresh_build_inputs(&checkout)?;
        logged(
            Command::new("cargo").arg("fetch").current_dir(&checkout),
            &options.root.join(format!("fetch-{label}.log")),
        )?;
        let artifacts = options.root.join(label);
        fs::create_dir_all(&artifacts)?;
        let mut command = Command::new("cargo");
        command
            .current_dir(&checkout)
            .env("CARGO_INCREMENTAL", "0")
            .env("CARGO_TARGET_DIR", workspace.join("target"))
            .args([
                "build",
                "--release",
                "--locked",
                "-p",
                "moraine",
                "-p",
                "moraine-duckdb",
                "--lib",
            ]);
        for example in [
            "campaign_verb",
            "campaign_verb_alloc",
            "campaign_row",
            "campaign_row_alloc",
            "campaign_memory",
            "campaign_build",
            "campaign_index",
            "campaign_index_alloc",
        ] {
            command.args(["--example", example]);
        }
        logged(
            &mut command,
            &options.root.join(format!("rust-{label}.log")),
        )?;
        for example in [
            "campaign_verb",
            "campaign_verb_alloc",
            "campaign_row",
            "campaign_row_alloc",
            "campaign_memory",
            "campaign_build",
            "campaign_index",
            "campaign_index_alloc",
        ] {
            fs::copy(
                workspace.join("target/release/examples").join(example),
                artifacts.join(example),
            )?;
        }
        let changes = Command::new("git")
            .args([
                "diff",
                "--",
                "Cargo.toml",
                "Cargo.lock",
                "crates/moraine/Cargo.toml",
                "crates/moraine/src/transaction/commit.rs",
            ])
            .current_dir(&checkout)
            .output()?;
        fs::write(artifacts.join("harness.patch"), changes.stdout)?;
        native(&workspace, &checkout, &artifacts, options)?;
        logged(
            Command::new(&cli).args(["-unsigned", "-c"]).arg(format!(
                "LOAD '{}';",
                artifacts.join("moraine.duckdb_extension").display()
            )),
            &options.root.join(format!("load-{label}.log")),
        )?;
        println!("Built and loaded {label}");
    }
    fs::write(options.root.join("revisions.csv"), manifest)?;
    Ok(())
}

fn prepare(workspace: &Path, checkout: &Path) -> anyhow::Result<()> {
    let crate_path = checkout.join("crates/moraine");
    fs::create_dir_all(crate_path.join("examples"))?;
    for (source, destination) in [
        ("verb_commit_bench", "campaign_verb"),
        ("row_lookup_bench", "campaign_row"),
        ("index_mutation_bench", "campaign_index"),
    ] {
        let instrumented =
            fs::read_to_string(workspace.join(format!("crates/moraine/examples/{source}.rs")))?;
        fs::write(
            crate_path.join(format!("examples/{destination}_alloc.rs")),
            &instrumented,
        )?;
        fs::write(
            crate_path.join(format!("examples/{destination}.rs")),
            instrumented.replace("#[global_allocator]\n", ""),
        )?;
    }
    fs::copy(
        workspace.join("crates/moraine/examples/staged_build_memory.rs"),
        crate_path.join("examples/campaign_memory.rs"),
    )?;
    prepare_build_latency(workspace, checkout)?;
    let manifest = checkout.join("Cargo.toml");
    let mut text = fs::read_to_string(&manifest)?;
    for (dependency, version) in [
        ("cpu-time", "=1.0.0"),
        ("stats_alloc", "=0.1.10"),
        ("dhat", "=0.3.3"),
    ] {
        if !text
            .lines()
            .any(|line| line.starts_with(&format!("{dependency} =")))
        {
            text = text.replace(
                "[workspace.dependencies]\n",
                &format!("[workspace.dependencies]\n{dependency} = \"{version}\"\n"),
            );
        }
    }
    fs::write(manifest, text)?;
    let manifest = crate_path.join("Cargo.toml");
    let mut text = fs::read_to_string(&manifest)?;
    for dependency in ["cpu-time", "stats_alloc", "dhat"] {
        if !text
            .lines()
            .any(|line| line.starts_with(&format!("{dependency} =")))
        {
            text = text.replace(
                "[dev-dependencies]\n",
                &format!("[dev-dependencies]\n{dependency} = {{ workspace = true }}\n"),
            );
        }
    }
    fs::write(manifest, text)?;
    // The old commit emits integer milliseconds; add the same timing precision.
    let commit = crate_path.join("src/transaction/commit.rs");
    let text = fs::read_to_string(&commit)?;
    let text = if text.contains("elapsed_ns =") {
        text
    } else {
        text.replace(
        "elapsed_ms = crate::telemetry::milliseconds(durable),",
        "elapsed_ms = crate::telemetry::milliseconds(durable),\n                elapsed_ns = u64::try_from(durable.as_nanos()).unwrap_or(u64::MAX),\n                projection_ns = u64::try_from(projection.as_nanos()).unwrap_or(u64::MAX),",
    )
    };
    fs::write(commit, text)?;
    Ok(())
}

fn prepare_build_latency(workspace: &Path, checkout: &Path) -> anyhow::Result<()> {
    let source =
        fs::read_to_string(workspace.join("crates/moraine/examples/staged_build_memory.rs"))?
            .replace("#[global_allocator]\n", "")
            .replace("static ALLOCATOR: dhat::Alloc = dhat::Alloc;\n", "")
            .replace(
                "let started = Instant::now();",
                "let cpu = cpu_time::ProcessTime::now();\n    let started = Instant::now();",
            )
            .replace(
                "let elapsed = started.elapsed();",
                "let elapsed = started.elapsed();\n    let cpu = cpu.elapsed();",
            )
            .replace(
                "total_allocated_bytes,total_ms",
                "total_allocated_bytes,total_ms,cpu_ms",
            )
            .replace(
                "{source_bytes},{},{},{},{:.3}\"",
                "{source_bytes},{},{},{},{:.3},{:.3}\"",
            )
            .replace(
                "elapsed.as_secs_f64() * 1000.0",
                "elapsed.as_secs_f64() * 1000.0,\n        cpu.as_secs_f64() * 1000.0",
            );
    fs::write(
        checkout.join("crates/moraine/examples/campaign_build.rs"),
        source,
    )?;
    Ok(())
}

pub(super) fn latency(options: &Options) -> anyhow::Result<()> {
    let workspace = crate::duckdb::workspace_root().canonicalize()?;
    for (label, _) in REVISIONS {
        println!("Building normal staged-build measurement for {label}");
        let checkout = options.root.join(format!("source-{label}"));
        prepare_build_latency(&workspace, &checkout)?;
        refresh_build_inputs(&checkout)?;
        logged(
            Command::new("cargo")
                .current_dir(checkout)
                .env("CARGO_INCREMENTAL", "0")
                .env("CARGO_TARGET_DIR", workspace.join("target"))
                .args([
                    "build",
                    "--release",
                    "--locked",
                    "--offline",
                    "-p",
                    "moraine",
                    "--example",
                    "campaign_build",
                ]),
            &options.root.join(format!("build-time-{label}.log")),
        )?;
        fs::copy(
            workspace.join("target/release/examples/campaign_build"),
            options.root.join(label).join("campaign_build"),
        )?;
    }
    Ok(())
}

// Historical packages share Cargo's generated-output directory. Force their
// schema and header generators to run when returning to an older checkout.
fn refresh_build_inputs(checkout: &Path) -> anyhow::Result<()> {
    for path in [
        "crates/moraine/build.rs",
        "crates/moraine/proto/moraine.proto",
        "crates/moraine-duckdb/build.rs",
    ] {
        fs::File::open(checkout.join(path))?.set_modified(std::time::SystemTime::now())?;
    }
    Ok(())
}

fn native(
    workspace: &Path,
    checkout: &Path,
    artifacts: &Path,
    options: &Options,
) -> anyhow::Result<()> {
    let source = options.root.join("native-source");
    let build = options.root.join("native-build");
    fs::create_dir_all(&source)?;
    let original = fs::read_to_string(checkout.join("CMakeLists.txt"))?;
    let (_, body) = original
        .split_once("set(CPP_DIR")
        .context("missing extension source list")?;
    let body = format!("set(CPP_DIR{body}").replace(
        "${CMAKE_CURRENT_SOURCE_DIR}/crates/moraine-duckdb/cpp",
        &checkout
            .join("crates/moraine-duckdb/cpp")
            .display()
            .to_string(),
    );
    let rust = workspace.join("target/release/libmoraine_duckdb.a");
    ensure!(rust.exists(), "missing Rust static library");
    fs::write(
        source.join("CMakeLists.txt"),
        format!(
            "cmake_minimum_required(VERSION 3.5)\nset(TARGET_NAME moraine)\nproject(moraine CXX)\nadd_library(moraine_duckdb-static STATIC IMPORTED GLOBAL)\nset_target_properties(moraine_duckdb-static PROPERTIES IMPORTED_LOCATION \"{}\" INTERFACE_LINK_LIBRARIES \"pthread;dl;m\")\n{body}",
            rust.display()
        ),
    )?;
    let config = options.root.join("native-config.cmake");
    fs::write(
        &config,
        format!(
            "duckdb_extension_load(moraine SOURCE_DIR {} DONT_LINK)\n",
            source.display()
        ),
    )?;
    let compilers = crate::duckdb::cpp_compilers()?;
    let mut command = Command::new("cmake");
    command
        .args(["-G", "Ninja", "-S"])
        .arg(workspace.join("duckdb"))
        .arg("-B")
        .arg(&build)
        .args([
            "-DCMAKE_BUILD_TYPE=Release",
            "-DBUILD_EXTENSIONS_ONLY=TRUE",
            "-DEXTENSION_STATIC_BUILD=TRUE",
        ])
        .arg(format!(
            "-DPREBUILT_BINARY={}",
            workspace
                .join("build/release/src/libduckdb_static.a")
                .display()
        ))
        .arg(format!("-DDUCKDB_EXTENSION_CONFIGS={}", config.display()))
        .arg(format!(
            "-DOVERRIDE_GIT_DESCRIBE={}",
            crate::duckdb::duckdb_pin()
        ));
    if let Some(compilers) = compilers {
        command
            .arg(format!("-DCMAKE_C_COMPILER={}", compilers.c))
            .arg(format!("-DCMAKE_CXX_COMPILER={}", compilers.cxx));
    }
    let label = artifacts
        .file_name()
        .context("artifact directory has no name")?
        .to_string_lossy();
    logged(
        &mut command,
        &options.root.join(format!("cmake-{label}.log")),
    )?;
    logged(
        Command::new("cmake").arg("--build").arg(build).args([
            "--target",
            "moraine_loadable_extension",
            "-j",
            "4",
        ]),
        &options.root.join(format!("native-{label}.log")),
    )?;
    fs::copy(
        options
            .root
            .join("native-build/extension/moraine/moraine.duckdb_extension"),
        artifacts.join("moraine.duckdb_extension"),
    )?;
    Ok(())
}
