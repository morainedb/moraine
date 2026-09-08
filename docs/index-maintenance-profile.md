# Index-maintenance allocation profile

The original 1,024-entry append benchmark used roughly 66 allocations per
inserted entry. Its Valgrind DHAT profile attributed **49 calls per entry** to
uniqueness point reads. The three follow-ups below are now implemented; the
original profile is retained here as the baseline.

## Workload and attribution

The ignored `index_maintenance_profile` test creates a real SlateDB store in
memory, seeds a unique integer index with 1,024 entries, and commits twenty
1,024-entry appends. Inputs are prepared before each commit, and every resulting
batch is verified afterwards. It uses the same registration path as the
[index-maintenance benchmark](allocation-benchmarks.md).

A non-inlined synchronous `profile_index_append` frame encloses commit polling.
DHAT stacks containing that frame exclude fixture creation, input preparation,
result verification, and shutdown. They cover synchronous commit preparation;
the detached durable commit and SlateDB background work run on other threads.
The complete raw profile includes those threads and setup/shutdown too.

Collected September 8, 2026, with Rust 1.96.1, SlateDB 0.15.0, imbl 5.0.0, and
Valgrind 3.19.0 on the Linux x86-64 VM, using the optimized release test executable.
The workload passed all result checks under Valgrind. Timing from that run is
not a performance measurement.

Before the probe optimizations, across the 20,480 inserted entries, the
`stats_alloc` commit intervals counted
1,348,384 allocation/reallocation calls: **65.84 per entry**, with 120,200,521
cumulative allocated bytes. The same workload without Valgrind counted
1,347,576 calls (**65.80 per entry**) and 118,176,911 bytes. Background work
during the intervals varies with instrumentation.
The filtered DHAT preparation stacks contained
1,174,387 allocation/reallocation events: **57.34 per entry**.

| Preparation category | Calls per entry | DHAT bytes per entry |
| --- | ---: | ---: |
| Uniqueness point read | 49.000 | 1,832 |
| Canonical key buffer growth | 3.000 | 62 |
| Transaction write batch | 2.167 | 242 |
| Probe scheduling | 2.000 | 1,520 |
| In-batch duplicate tracking | 1.010 | 188 |
| Other preparation | 0.166 | 331 |
| Total under the preparation frame | 57.343 | 4,176 |

Categories are exclusive and classified by stack ancestry. The remaining
approximately 8.5 calls per entry in the commit intervals include work outside
the preparation frame; they are not all attributed to one function. Whole-process
DHAT totals also include seeding, validation, and closing, so dividing those
totals by the inserted entry count would overstate commit cost.

DHAT's byte totals count each successful reallocation's full new size;
`stats_alloc` counts its growth. Their byte totals are not interchangeable.
[Baseline allocation-site counts](index-maintenance-profile-before-probe-optimizations.csv) retain the top two stack
frames for each grouped site. The complete local profile is saved as
`.context/index-maintenance.dhat.json`.

## What the baseline stacks show

- `resolve_probe → ReadHandle::get → Reader::get_key_value_with_options` accounts
  for exactly 49 events per inserted entry in this fixture. SlateDB constructs
  its point read from write-batch, memory-table, and segment iterators, then
  layers filtering and merge handling around them. The stacks include boxed
  iterator futures, iterator/source vectors, and key copies even when the key
  is absent and no object GET is needed.
- `CanonicalKeyBuilder::append` grows the encoded-key buffer three times per
  entry in this integer fixture. The initial flag, framed value, and escaped
  bytes trigger buffer growth.
- `schedule_probe_plan` accounts for two allocations per entry: the boxed
  probe future and its `FuturesUnordered` task. These hold 1,520 bytes per entry
  in total, so allocation count and byte cost identify different priorities.
- Write-batch insertion copies the key and row-id value once each, plus
  amortized B-tree allocation. Duplicate tracking includes shared-key ownership
  promotion and hash-table growth.

## Implemented follow-ups

1. Physical key encoding reserves the exact framed capacity as each component
   arrives. The integer-entry regression checks one allocation across NULL and
   non-NULL values, both directions, and unique/non-unique shapes. Existing
   property tests compare the resulting bytes with the original codec.
2. Probe futures have concrete types. The queue no longer boxes each probe;
   its task allocations remain, and batches amortize those allocations.
3. Up to 128 ready probes can share SlateDB's transactional range-read setup.
   Dense keys advance through one iterator. A gap switches remaining keys to
   concurrent point reads. Batches overlapping earlier claims or deletions,
   deletion-phase prefetch, and later grouped-commit members use point reads,
   avoiding repeated copies of transaction-local writes. A single key keeps
   its ordinary point read. No SlateDB dependency fork is required.

The reader retains the original transaction's snapshot and local-write
semantics. Regression coverage compares unordered, duplicate, missing, and
sparse keys against point reads after local writes and later committed changes.
The scheduler retains bounded concurrency and delete-before-add staging.

## After the three optimizations

The normal 20,480-entry profile workload now counts **275,715 calls**
(**13.46 per entry**) and **58,063,391 bytes**, down from 1,347,576 calls
and 118,176,911 bytes. That is about **80% fewer calls and 51% fewer bytes**.
Under DHAT, the intervals counted 276,201 calls and 58,998,033 bytes.

The filtered preparation stacks now contain **102,867 events**
(**5.02 per entry**), down from 1,174,387 (**57.34 per entry**).

| Identified preparation category | Calls per entry | DHAT bytes per entry |
| --- | ---: | ---: |
| Key encoding | 1.000 | 38 |
| Transactional scan setup | 0.547 | 86 |
| Transaction write batch | 2.167 | 242 |
| Probe planning and duplicate tracking | 1.018 | 244 |
| Probe scheduling and results | 0.008 | 18 |
| Other preparation | 0.284 | 596 |

The transactional scan setup category has **70 events per 128-key batch**,
or **0.547 per entry**, replacing the fixture's 49 point-read events per entry.
Encoding uses one allocation per entry. Compiler inlining hides some batch
bookkeeping frames; those sites remain in other preparation rather than being
assigned to a function without evidence. Category boundaries therefore do not
all match the baseline, while filtered totals use the same enclosing frame.

[Updated allocation sites](index-maintenance-profile.csv) retain the grouped
stacks; the raw local output is `.context/index-maintenance-optimized.dhat.json`.
See the [benchmark comparison](allocation-benchmarks.md#probe-optimization-measurements)
for normal CPU, latency, durability, sparse-key, and input-order results.

These improvements apply most strongly to dense append batches. The fallback
still pays SlateDB's point-read costs, and batching adds fixed storage overhead.
This profile does not establish an equivalent improvement for sparse probes,
deletion-heavy work, or later members of grouped commits.

## Reproducing

Install Valgrind and build the release unit-test executable:

```bash
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked --no-run
```

Set `MORAINE_TEST_BINARY` to the executable path Cargo prints, then run:

```bash
valgrind --tool=dhat --num-callers=40 \
  --dhat-out-file=.context/index-maintenance-optimized.dhat.json \
  "$MORAINE_TEST_BINARY" \
  transaction::commit::tests::allocation::index_maintenance_profile \
  --exact --ignored --test-threads=1 --nocapture
```

Open the JSON in Valgrind's bundled `dh_view.html`. Filter stack ancestry by
`profile_index_append` for commit preparation; retain the unfiltered profile to
inspect detached commit and WAL work. To compare normal allocation counts,
run the same test executable and filter without Valgrind.
