# Contributing

## Setup

```bash
rustup toolchain install  # respects rust-toolchain.toml
rustup toolchain install nightly-2026-08-29 --profile minimal --component rustfmt
cargo install cargo-deny
python3 -m venv target/clang-tools
target/clang-tools/bin/python -m pip install \
  "clang-format==$(cat .clang-tools-version)" \
  "clang-tidy==$(cat .clang-tools-version)"
```

Formatting uses a dated nightly so CI and local hooks produce the same output.
Update the formatter pin in CI, workspace setup, both commit hooks, and these
instructions together.

C/C++ tools share the version in `.clang-tools-version`. The xtask commands
use `target/clang-tools/bin` when installed there, or the same version on
`PATH`. Python needs venv support (`python3-venv` on Debian/Ubuntu).

Run `cargo xtask fmt-cpp` to format handwritten C/C++ sources and headers.
The pre-commit hook and CI run `cargo xtask fmt-cpp --check`. Dependencies
and the generated `moraine_abi.h` are excluded.

`cargo xtask tidy-cpp` prepares the patched DuckLake sources and configures
the release build without compiling DuckDB or the Rust core. It needs the
same submodules, C++ compiler, CMake, and Ninja as `e2e`; a first run may
download and build vcpkg dependencies. It checks both static and loadable
shim configurations, plus the standalone C/C++ drivers. Checks live in
`.clang-tidy`; findings fail CI and the full local gate.
Analysis uses up to four workers, with each file's diagnostics kept together.
On macOS, tidy skips the concurrent-writer C benchmark because it uses
unavailable pthread barriers; Linux CI checks it. Formatting covers it on
every platform.

`cargo xtask e2e` compiles DuckDB itself. Install `ccache` (or `sccache`)
and DuckDB's cmake finds it on `PATH` by itself, which turns the
ten-minute rebuild a fresh worktree otherwise pays into about a minute.
There is nothing else to configure.

## The local gate

Everything CI enforces, runnable locally:

```bash
cargo +nightly-2026-08-29 fmt --check
cargo xtask fmt-cpp --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo deny check -D advisory-not-detected
cargo xtask tidy-cpp
cargo xtask e2e
```

## Conventions

- **Conventional commits** (`feat:`, `fix:`, `docs:`, …) — release-plz derives
  changelogs and versions from them.
- **TDD**: test first, watch it fail, implement. Bugfixes land with a
  regression test.
- **RFCs** (`docs/rfcs/`) are required for expensive-to-reverse decisions:
  key layout, commit protocol, public API shape.
- Full conventions: [`AGENTS.md`](AGENTS.md).
- PRs into `main`, squash-merged.

## Conduct & licensing

We follow the [Code of Conduct](CODE_OF_CONDUCT.md). Contributions are
dual-licensed MIT OR Apache-2.0.
