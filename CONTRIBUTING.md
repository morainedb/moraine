# Contributing

## Setup

```bash
rustup toolchain install  # respects rust-toolchain.toml
rustup toolchain install nightly-2026-08-29 --profile minimal --component rustfmt
cargo install cargo-deny
python3 -m venv target/clang-tools
target/clang-tools/bin/python -m pip install \
  "clang-format==$(cat .clang-tools-version)" \
  "clang-tidy==$(cat .clang-tools-version)" \
  "rumdl==$(cat .rumdl-version)"
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

Markdown is linted and formatted by [rumdl](https://rumdl.dev), pinned in
`.rumdl-version` and configured in `.rumdl.toml`. `rumdl fmt .` rewraps
prose at 80 columns and fixes what it can; the pre-commit hook and CI run
`rumdl check .`. Zed formats on save through rumdl's language server (the
project settings install its extension).

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

[mbx](https://github.com/jdx/mr-boxington) does the same for Rust: install
it, run `mbx setup`, and prefix Cargo commands with `mbx` — or put the shim
directory it prints on `PATH` and keep using plain `cargo`. A second
checkout then restores what the first compiled, and CI caches the same way
through `jdx/mr-boxington-action`. Conductor workspaces install it in setup.
mbx holds `target/` in its own store, behind a `target` symlink, and
collects it when the disk runs short. When the disk runs out, run `mbx gc`:
it collects managed targets and incremental state first, then evicts store
objects to budget, and a build rebuilds whatever it evicted. Do not
`cargo clean`, `rm -rf target`, or replace the link with a directory; each
discards state every worktree shares. A collection takes the `clang-tools`
venv above with it, so recreate it afterwards.

## The local gate

Everything CI enforces, runnable locally:

```bash
cargo +nightly-2026-08-29 fmt --check
cargo xtask fmt-cpp --check
rumdl check .
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo deny check -D advisory-not-detected
cargo xtask tidy-cpp
cargo xtask e2e
```

## One-off extension releases

Run the **Extension** workflow on `main` with a fresh release tag:

```bash
gh workflow run extension.yml --repo morainedb/moraine --ref main \
  -f tag=ext-v0.7.0-build.2
```

Tags use `ext-v<version>` or `ext-v<version>-build.<number>`. After every
platform builds and passes validation, the workflow publishes unsigned
binaries to that GitHub release and pins the community descriptor to the
built commit. This does not bump the crate version or publish to crates.io.

## The real S3 benchmark

The `Real S3 benchmark` Actions workflow is a manually triggered,
`main`-only measurement. GitHub exchanges its OIDC token for a narrowly
scoped launcher role, starts one managed ARM CodeBuild container in the
bucket's region, runs the ignored `object_storage` suite against AWS S3
under a unique prefix, and keeps the printed tables as a GitHub artifact.
No self-hosted runner or long-lived AWS key is involved. What the suite
measures is in [`docs/benchmarks.md`](docs/benchmarks.md).

### One-time AWS setup

The account must have GitHub's OIDC provider registered with URL
`https://token.actions.githubusercontent.com` and audience
`sts.amazonaws.com`. Create it once per AWS account if it does not already
exist:

```sh
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com
```

Pass that provider's ARN to the repository template:

```sh
aws cloudformation deploy \
  --stack-name moraine-real-s3-benchmark \
  --template-file .github/aws/real-s3-benchmark.yml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    GitHubOidcProviderArn=arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com
```

The template creates the private lifecycle-managed bucket, the ephemeral
CodeBuild project, its S3-only execution role, and an OIDC launcher role
whose trust policy accepts only `morainedb/moraine`'s `main` ref. The
subject includes GitHub's immutable organization and repository IDs, so a
future rename, transfer, or reuse of either name cannot inherit the trust
relationship.

The parameter defaults target this repository:

| Parameter | Default |
|---|---|
| `GitHubOrganization` | `morainedb` |
| `GitHubOrganizationId` | `310371634` |
| `GitHubRepository` | `moraine` |
| `GitHubRepositoryId` | `1294736153` |

Override all four for a fork. Obtain the IDs with
`gh api users/OWNER --jq .id` and `gh api repos/OWNER/REPOSITORY --jq .id`.

Copy the four stack outputs into repository Actions variables:

| Repository variable | CloudFormation output |
|---|---|
| `MORAINE_BENCHMARK_AWS_REGION` | `AwsRegion` |
| `MORAINE_BENCHMARK_CODEBUILD_PROJECT` | `CodeBuildProjectName` |
| `MORAINE_BENCHMARK_S3_BUCKET` | `BenchmarkBucketName` |
| `MORAINE_BENCHMARK_ROLE_ARN` | `GitHubLauncherRoleArn` |

For example, fetch the outputs with:

```sh
aws cloudformation describe-stacks \
  --stack-name moraine-real-s3-benchmark \
  --query 'Stacks[0].Outputs' --output table
```

### Running it

Open **Actions → Real S3 benchmark → Run workflow** and select `main`. A
dispatch from any other ref fails before requesting AWS credentials, and
the AWS trust policy independently refuses its OIDC subject.

The Actions summary links to the CodeBuild logs and names the unique S3
data prefix. The `real-s3-<run>-<attempt>` artifact contains the benchmark
output and CodeBuild metadata. Benchmark data expires after seven days;
GitHub keeps the artifact for 30 days. CloudFormation retains the bucket
when the stack is deleted so benchmark data is not destroyed implicitly;
empty it explicitly when retiring the stack.

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
