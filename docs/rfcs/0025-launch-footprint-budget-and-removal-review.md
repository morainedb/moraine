# RFC 0025: Launch footprint budget and removal review

- **Date:** 2026-10-22

## Constraints

The launch requires a 25% reduction in handwritten production code. Indexes stay:
creation, building, maintenance, uniqueness enforcement, optimizer acceleration,
and selective indexed reads remain supported. Removing the index data path or
forcing indexed queries through ordinary full scans does not satisfy this design.

Feature and compatibility removals require the owner's selection before
implementation. No removal is approved. No feature has been removed for this budget.

## Measurement

The baseline is commit `663667e1`, before the eight simplification passes. The
same census is applied to that commit and the current working tree: nonblank,
noncomment Rust/C/C++ implementation lines under `crates/`, including examples,
excluding generated `moraine_abi.h`, integration tests, test modules extracted
with the Rust syntax tree, test-support files, and the fault-injection test log.
`xtask`, documentation, generated output, and tests cannot satisfy this budget.

| Measurement | Production lines |
| --- | ---: |
| Baseline, consistently classified | 58,590 |
| Maximum after a 25% reduction | 43,942 |
| Current tree, including live-memory admission and publication fixes | 59,744 |
| Further net reduction required | 15,802 |

Earlier notes quoted a 59,212-line baseline. This audit freezes a specific commit
and corrects classification, including 598 lines of fault-injection test-log
support previously placed in production. Reclassification is applied on both
sides and is not credited as code removed. Census artifacts are in
`.context/launch-census-baseline.json` and
`.context/launch-census-admission-final.json` in this review workspace.

## Enterprise compatibility

Moraine Enterprise must remain operational. Preserve the commit-log plug API,
committer/folder paths, migrations, WAL relocation, and named storage checkpoints.
These are not removal candidates for this launch.

The Enterprise extension registers `moraine_wal::Provider` through
`register_commit_log`; its maintenance service uses that provider and
`Catalog::fold_sprint`. Its commit-log design carries WAL relocation forward,
uses migration for existing stores, and recommends checkpoint-pinned readers.
Removing these surfaces would violate the compatibility requirement or require a
separate Enterprise redesign.

The earlier 1,888-line removal estimate is withdrawn. No feature-removal package
is approved, and there is no currently demonstrated path to the 25% target.

## Refactoring with indexes retained

The remaining reduction must come from shared implementations or separately
reviewed removals that preserve Enterprise. No measured replacement design yet
demonstrates the required 15,802-line saving; it must not be presented as a
guaranteed result.

The next consolidation targets are:

- Define metadata fields once for Rust/C row conversion and C++ table projection,
  preserving ownership, nullability, column order, and both catalog versions.
- Share mechanical operation dispatch between staged and committed metadata
  handling. Keep transaction conflict checks and index maintenance explicit.

Count every handwritten declaration and generator needed by a replacement.
Moving code, compressing formatting, generating identical repetition, or deleting
tests does not earn savings. A consolidation must reduce the maintained concepts
and retain readable control flow. Measure completed pilots before assigning them
savings or claiming a complete path to 25%.

## Validation

Keep regressions for index creation, unique constraints, mutation maintenance,
verification, automatic indexed SELECT/UPDATE/DELETE, and selective reads.
Preserve ordinary DDL, nested types, inline/Parquet CRUD, row-ID invariants,
metadata lifecycle, durability, read-only operation, and concurrent transactions.
Add rejection tests before implementing any approved removal.

Run the full repository gate and representative SQL comparisons. The final tree
must contain at most 43,942 production lines under the frozen classifier. Report
any shortfall explicitly; neither the 25% requirement nor the index requirement
can be traded away without a new owner decision.

Background reliability, the shared budget for retained lookup caches, and the
metadata representation remain separate implementation obligations. This review
document does not complete them.

