# ClickBench

[ClickBench](https://github.com/ClickHouse/ClickBench) is a query benchmark
for analytical databases: one flat 100-million-row web-analytics table
(`hits`) and 43 queries over it, published with every system's numbers at
<https://benchmark.clickhouse.com>. This directory is a ClickBench *system
entry* for a moraine-backed DuckLake, so moraine's numbers come out of the
same driver — and onto the same scale — as everyone else's.

## What it measures, and what it does not

ClickBench is a read benchmark. Its queries are scans and aggregations over
one table, and they spend their time in the data plane: reading Parquet,
decoding it, grouping. moraine replaces DuckLake's *metadata* catalog, which
a scan touches once at the start of a statement. So the expected result here
is that moraine lands next to the other DuckDB-family entries rather than
above or below them, and the interesting number is how little the catalog
costs on a read path — not a win over ClickHouse.

For the metadata path itself — commit latency, DDL, deletes, cold attach —
see `cargo xtask bench` and [BENCHMARK.md](../../BENCHMARK.md). That is
where moraine's differences show up.

## Running it

A run needs a checkout of ClickBench, whose `lib/benchmark-common.sh` is the
shared driver every system is measured by:

```sh
git clone https://github.com/ClickHouse/ClickBench.git
cargo xtask clickbench run --clickbench ./ClickBench --machine c6a.4xlarge
```

That builds the extension, copies this directory into the checkout as
`ducklake-moraine/`, runs the driver, and writes
`results/<date>/<machine>.json` here in ClickBench's published format.

`--machine` is required and is not detected. The dashboard compares within
one instance type, so a result carrying the wrong label would be ranked
against the wrong field; there is no safe default.

A run driven by hand, or one whose log came back from CI, becomes a result
document the same way:

```sh
cargo xtask clickbench publish --log benchmark.log --machine c6a.4xlarge
```

The run wants a machine the published numbers also cover — `c6a.4xlarge`
(16 vCPU, 32 GiB, 500 GB gp2) is the reference, and what the dashboard
selects. It needs about 40 GB free: 15 GB for the source Parquet, which the
load deletes afterwards, and the lake itself. The driver drops the page
cache between queries, so it needs passwordless `sudo`.

## Reading the result against the published numbers

```sh
cargo xtask clickbench report --clickbench ./ClickBench
cargo xtask clickbench report --clickbench ./ClickBench --metric hot --queries
```

`report` ranks the newest result in this directory against the newest
published run of every other system on the same machine, using ClickBench's
own scoring: each query's time offset by 10 ms and divided by the best time
for that query, geometric mean across the 43, with an unanswered query
charged twice the system's own worst time (floored at five minutes).
`--metric` picks `combined` (the default: hot 60%, cold 20%, load time 10%,
data size 10%), `cold`, `hot`, `load` or `size`. A system barred from a
metric — an in-memory engine has no cold read, a stateless one no load —
prints a dash rather than a number it has no basis for.

## The files

The suite itself is not in this repository. ClickBench is CC BY-NC-SA 4.0,
which does not mix with moraine's MIT/Apache-2.0, so `create.sql` and
`queries.sql` are taken from the checkout's own `duckdb` entry when the run
stages this directory: the DuckLake table takes the same column list, and
the queries are already in DuckDB's dialect. What lives here is the
moraine-specific half of the contract upstream's driver calls:

| file | role |
|---|---|
| `template.json` | the metadata the dashboard filters and labels on |
| `install` | builds the extension and the CLI that can load it, into `env.sh` |
| `lake.sh` | the CLI flags that open the lake, shared so they cannot drift |
| `load` | `CREATE TABLE` + one `INSERT` from `hits.parquet` |
| `query` | one query against the lake, timed |
| `data-size` | bytes the catalog and the Parquet data files occupy |
| `check` | readiness probe: can the CLI load the extension |
| `start`, `stop` | no-ops — an embedded engine has no daemon |
| `benchmark.sh` | the driver's knobs for an embedded, single-process engine |

Two choices worth stating, both matching what the other embedded entries do:

- Loading the extension and attaching the lake happen before `.timer on`, so
  a query's reported time is the query's and not the catalog open. The cost
  of opening a moraine catalog cold is measured in `BENCHMARK.md` instead.
- The concurrent-QPS test is skipped. Each query is a fresh CLI process with
  no shared scheduler, so concurrent connections would oversubscribe memory
  rather than measure throughput.

The DuckDB pin is a development revision with no published CLI, and an
extension only loads into the exact DuckDB version stamped in its footer, so
`install` uses the CLI that the extension build produces rather than an
installed `duckdb`.
