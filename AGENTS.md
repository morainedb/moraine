# Working in moraine

Moraine brings a SlateDB backend to DuckLake. Structure and conventions are
specified in [RFC 0001](docs/rfcs/0001-repository-structure-and-conventions.md);
this file is the operational summary.

## Layout

- `crates/moraine` — core library. `catalog` (DuckLake domain) owns the
  store handle but no key/value knowledge — that lives in `store`
  (keys/codecs, which knows nothing about DuckLake); `transaction` (the commit
  protocol) bridges them. `data_file` is the only module that reads Parquet:
  scoped reads of registered files, knowing nothing about the catalog
  keyspace. `lib.rs` is docs + re-exports only.
- `crates/moraine-duckdb` — DuckDB extension: a thin C++ shim registering a
  `StorageExtension` over a C ABI to the Rust core (RFC 0006). Thin by policy:
  if logic accumulates here, move it to the core.
- `test/sql` — sqllogictest files run by DuckDB's own runner, for what the
  CLI cannot express: several connections over one instance (concurrent
  DuckLake transactions). Driven by `cargo xtask e2e`.
- `xtask` — automation (`cargo xtask e2e`). Rust, not shell scripts.

## Rules

- TDD: write the failing test first. No mocks of the store layer — use real
  SlateDB on in-memory `object_store`.
- Proptest roundtrips are mandatory for anything in `store` that
  encodes/decodes.
- No `unwrap`/`expect`/`panic` in library code (lint-enforced; tests exempt
  via `clippy.toml`). One sanctioned exception: a targeted
  `#[allow(clippy::expect_used)]` where failure is impossible by
  construction (e.g. encoding into a `Vec`), with the invariant stated in a
  comment at the call site — never a fabricated fallback path. `unsafe` is
  forbidden in `moraine`, and in `moraine-duckdb` requires a `// SAFETY:`
  comment.
- Modules: start as `foo.rs`, split into `foo.rs` + `foo/` (never `mod.rs`).
- Public items are documented (`missing_docs` warns); key APIs carry doctests;
  crate-root docs teach by worked example.
- No decorative comment banners (`// --- section ---`, `// ====`, etc.).
  Comments carry content, not typography; if a file needs section markers,
  split the module instead.
- Comments are direct and succinct: state what the item does and any hard
  constraint, no rationale essays.
- Use blank lines to group code into readable stanzas.
- Names prefer full words, for every symbol; abbreviate only when the word
  is long and the abbreviation is conventional (`tx`, not `tbl`).
- Code and code comments never cite RFCs by number or name. State the
  constraint itself in the comment; RFCs reference code, not the reverse.
- Features are additive-only and documented in the crate root.
- Conventional commits; PRs squash-merge into `main`.

## Design docs

- Design/brainstorm outputs are written directly as RFCs in `docs/rfcs/`
  (`NNNN-kebab-title.md`; no status field — every RFC is the current,
  binding design; update or replace, never re-label).
  **Do not** create `docs/superpowers/specs/` — the RFC directory overrides
  that default.
- Implementation plans go to `docs/plans/` (not-committed).
- RFCs are required for: KV key layout, commit protocol, public API shape.

## The local gate

```bash
cargo +nightly fmt --check && cargo clippy --workspace --all-targets -- -D warnings \
  && cargo test --workspace --locked \
  && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps \
  && cargo deny check -D advisory-not-detected \
  && cargo xtask check-pins && cargo xtask e2e
```

`e2e` compiles DuckDB itself, and DuckDB's cmake picks up `ccache` or
`sccache` off `PATH` on its own. Installing one is the difference between
a ten-minute and a one-minute rebuild in a fresh worktree.

The fmt/clippy portion also runs as a pre-commit hook from `.githooks/`,
and a commit-msg hook there rejects non-conventional commit subjects (the
changelog is generated from them; CI validates PR titles the same way).
Conductor workspaces enable those hooks from `.conductor/settings.toml`,
which also installs the toolchain a cloud workspace starts without. A plain
clone needs it once: `git config core.hooksPath .githooks`.
