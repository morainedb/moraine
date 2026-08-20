# Contributing

## Setup

```bash
rustup toolchain install  # respects rust-toolchain.toml
cargo install cargo-deny
```

`cargo xtask e2e` compiles DuckDB itself. Install `ccache` (or `sccache`)
and DuckDB's cmake finds it on `PATH` by itself, which turns the
ten-minute rebuild a fresh worktree otherwise pays into about a minute.
There is nothing else to configure.

## The local gate

Everything CI enforces, runnable locally:

```bash
cargo +nightly fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo deny check -D advisory-not-detected
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
