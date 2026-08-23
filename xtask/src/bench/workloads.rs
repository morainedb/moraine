//! Workload definitions: identical statement streams every backend runs.
//! A workload is an optional untimed seeding session plus a measured
//! session; the session runner prepends the warm-up and `ATTACH`
//! statements, so definitions here start from an attached `lake`.

use anyhow::bail;

use super::timing::Statement;

/// Knobs a scale name expands to.
pub struct Scale {
    pub name: &'static str,
    pub bulk_rows: u64,
    pub small_commits: u64,
    pub tables: u64,
    pub deletes: u64,
}

impl Scale {
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        let (name, bulk_rows, small_commits, tables, deletes) = match name {
            "small" => ("small", 100_000, 20, 10, 20),
            "medium" => ("medium", 1_000_000, 50, 25, 50),
            "large" => ("large", 10_000_000, 200, 100, 200),
            other => bail!("unknown scale `{other}`; valid: small, medium, large"),
        };

        Ok(Self {
            name,
            bulk_rows,
            small_commits,
            tables,
            deletes,
        })
    }
}

/// One benchmark workload: `seed` runs in its own untimed session first
/// (empty for write workloads), then `measured` runs in a fresh session
/// whose per-phase timings are reported.
pub struct Workload {
    pub name: &'static str,
    pub seed: Vec<String>,
    pub measured: Vec<Statement>,
    /// Set when the workload calls a moraine-only function, so the other
    /// backends cannot run its SQL and the report leaves their cells empty.
    pub moraine_only: bool,
}

fn bulk_create_and_insert(rows: u64) -> [String; 2] {
    [
        "CREATE TABLE lake.main.items (id BIGINT, amount DOUBLE);".to_owned(),
        format!(
            "INSERT INTO lake.main.items \
             SELECT i::BIGINT, (i * 1.5)::DOUBLE FROM range({rows}) t(i);"
        ),
    ]
}

fn small_commit_inserts(count: u64) -> impl Iterator<Item = String> {
    (0..count)
        .map(|index| format!("INSERT INTO lake.main.events VALUES ({index}, 'event-{index}');"))
}

/// DuckLake inlines an insert of at most this many rows into the catalog
/// instead of writing a Parquet data file (its `data_inlining_row_limit`
/// default). A commit must exceed it to produce a real, mergeable file.
const INLINE_ROW_LIMIT: u64 = 10;

/// `count` inserts, each writing `INLINE_ROW_LIMIT * 2` rows so every one
/// exceeds the inline limit and lands a separate small Parquet data file —
/// the adjacent files `merge` is meant to coalesce. Row ids are disjoint
/// across batches so the rows are genuinely distinct.
fn data_file_inserts(count: u64) -> impl Iterator<Item = String> {
    let rows_each = INLINE_ROW_LIMIT * 2;
    (0..count).map(move |batch| {
        let base = batch * rows_each;
        format!(
            "INSERT INTO lake.main.events \
             SELECT {base} + i, 'event-' || i FROM range({rows_each}) t(i);"
        )
    })
}

/// Rows per delete key. Well over `INLINE_ROW_LIMIT`, so a delete of one
/// key writes a delete file rather than inlined tombstones, and large
/// enough that the rewritten file grows measurably across the run.
const ROWS_PER_KEY: u64 = 200;

/// Loads `keys` keys of [`ROWS_PER_KEY`] rows into `table` as one data
/// file, so deletes of a key concentrate on a single target.
fn keyed_rows(table: &str, keys: u64) -> String {
    format!(
        "INSERT INTO lake.main.{table} \
         SELECT i, i // {ROWS_PER_KEY} FROM range({}) t(i);",
        keys * ROWS_PER_KEY
    )
}

fn keyed_table(table: &str) -> String {
    format!("CREATE TABLE lake.main.{table} (id BIGINT, k BIGINT);")
}

fn partition_by_key(table: &str) -> String {
    format!("ALTER TABLE lake.main.{table} SET PARTITIONED BY (k);")
}

/// Every workload at `scale`, in report order.
pub fn workloads(scale: &Scale) -> Vec<Workload> {
    let [create_items, insert_items] = bulk_create_and_insert(scale.bulk_rows);

    let bulk_load = Workload {
        name: "bulk_load",
        seed: Vec::new(),
        measured: vec![
            Statement::measured("create_table", create_items.clone()),
            Statement::measured("insert", insert_items.clone()),
        ],
        moraine_only: false,
    };

    // Each autocommitted single-row insert is one catalog commit; the
    // sum across all of them is the headline catalog-latency phase.
    let small_commits = Workload {
        name: "small_commits",
        seed: Vec::new(),
        measured: std::iter::once(Statement::setup(
            "CREATE TABLE lake.main.events (id BIGINT, note VARCHAR);",
        ))
        .chain(
            small_commit_inserts(scale.small_commits)
                .map(|sql| Statement::measured("inserts", sql)),
        )
        .collect(),
        moraine_only: false,
    };

    let many_tables = Workload {
        name: "many_tables",
        seed: Vec::new(),
        measured: (0..scale.tables)
            .map(|index| {
                Statement::measured(
                    "creates",
                    format!("CREATE TABLE lake.main.table_{index} (id BIGINT, name VARCHAR);"),
                )
            })
            .collect(),
        moraine_only: false,
    };

    let scan = Workload {
        name: "scan",
        seed: vec![create_items, insert_items],
        measured: vec![
            Statement::measured("full_scan", "SELECT sum(amount) FROM lake.main.items;"),
            Statement::measured(
                "filtered_scan",
                format!(
                    "SELECT count(*) FROM lake.main.items WHERE id = {};",
                    scale.bulk_rows / 2
                ),
            ),
            Statement::measured(
                "time_travel",
                "SELECT count(*) FROM lake.main.items AT (VERSION => 1);",
            ),
            Statement::measured(
                "snapshots",
                "SELECT count(*) FROM ducklake_snapshots('lake');",
            ),
        ],
        moraine_only: false,
    };

    // Seeded with one data-file-writing insert per commit (each over the
    // inline limit), so `merge` has adjacent Parquet files to coalesce
    // rather than only inlined rows — otherwise it would measure call
    // overhead, not compaction.
    let maintenance = Workload {
        name: "maintenance",
        seed: std::iter::once(
            "CREATE TABLE lake.main.events (id BIGINT, note VARCHAR);".to_owned(),
        )
        .chain(data_file_inserts(scale.small_commits))
        .collect(),
        measured: vec![
            Statement::measured("merge", "CALL ducklake_merge_adjacent_files('lake');"),
            Statement::measured(
                "expire",
                "CALL ducklake_expire_snapshots('lake', older_than => now());",
            ),
            Statement::measured(
                "cleanup",
                "CALL ducklake_cleanup_old_files('lake', cleanup_all => true);",
            ),
        ],
        moraine_only: false,
    };

    let mut all = vec![bulk_load, small_commits, many_tables, scan, maintenance];
    all.extend(delete_workloads(scale));
    all
}

/// The table every delete workload targets. One name across all of them:
/// the seeds are then identical strings apart from partitioning, which is
/// what makes the timings comparable.
const DELETE_TARGET: &str = "target";

/// One delete path, alone in its own lake.
///
/// Delete cost rises with how many tables the lake holds, so a path
/// measured beside its siblings is charged for them. Each gets its own
/// workload — the harness gives every workload fresh directories — with
/// one table and nothing else to pay for.
fn delete_workload(
    name: &'static str,
    partitioned: bool,
    keys: u64,
    statements: Vec<String>,
) -> Workload {
    let mut seed = vec![keyed_table(DELETE_TARGET)];
    if partitioned {
        seed.push(partition_by_key(DELETE_TARGET));
    }
    seed.push(keyed_rows(DELETE_TARGET, keys));

    Workload {
        name,
        seed,
        measured: statements
            .into_iter()
            .map(|sql| Statement::measured("delete", sql))
            .collect(),
        moraine_only: false,
    }
}

/// The same workload with an equality index over the deleted column, so
/// each delete also maintains index entries. Renamed with an `indexed_`
/// infix and moraine-only, since no other backend has the function.
fn indexed(mut workload: Workload, name: &'static str) -> Workload {
    workload.name = name;
    workload.seed.push(format!(
        "SELECT * FROM moraine_index_create('lake','main','{DELETE_TARGET}','by_id',['id'],false);"
    ));
    workload.moraine_only = true;
    workload
}

/// The delete paths, in report order: the statement-shape pair first, then
/// the drop pair.
///
/// One key more than the run deletes, so `deletes_repeated` never covers
/// its file and never falls into the drop path.
fn delete_workloads(scale: &Scale) -> [Workload; 6] {
    let keys = scale.deletes + 1;
    let repeated = |name| {
        delete_workload(
            name,
            false,
            keys,
            (0..scale.deletes)
                .map(|key| format!("DELETE FROM lake.main.{DELETE_TARGET} WHERE k = {key};"))
                .collect(),
        )
    };
    let covering = |name| {
        delete_workload(
            name,
            true,
            keys,
            vec![format!(
                "DELETE FROM lake.main.{DELETE_TARGET} WHERE k = 0;"
            )],
        )
    };
    [
        // One key per statement, so each rewrites a delete file the last
        // one grew.
        repeated("deletes_repeated"),
        // The rows `deletes_repeated` removes, in one statement.
        delete_workload(
            "deletes_bulk",
            false,
            keys,
            vec![format!(
                "DELETE FROM lake.main.{DELETE_TARGET} WHERE k < {};",
                scale.deletes
            )],
        ),
        // Partition-aligned, so the delete covers its file exactly and
        // DuckLake ends the file instead of writing a delete file for it.
        covering("deletes_covering"),
        // `deletes_covering`'s control: the same partitioned shape and the
        // same target partition, one row short of covering it. What
        // separates the two timings is the drop alone.
        delete_workload(
            "deletes_partial",
            true,
            keys,
            vec![format!(
                "DELETE FROM lake.main.{DELETE_TARGET} WHERE k = 0 AND id > 0;"
            )],
        ),
        // The pair above, against an indexed table. Each is its unindexed
        // twin's seed plus the index, so the difference between them is
        // what maintaining index entries costs a delete: on `repeated`,
        // re-reading a delete file that grows every statement; on
        // `covering`, deriving the dropped file's entries from the file
        // itself, which is the only thing naming them.
        indexed(repeated("x"), "deletes_indexed_repeated"),
        indexed(covering("x"), "deletes_indexed_covering"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> Scale {
        Scale::parse("small").unwrap()
    }

    #[test]
    fn scale_parses_known_names_and_rejects_others() {
        assert_eq!(small().bulk_rows, 100_000);
        assert_eq!(Scale::parse("large").unwrap().tables, 100);
        assert!(Scale::parse("huge").is_err());
    }

    #[test]
    fn workload_names_and_phases_match_the_report_contract() {
        let all = workloads(&small());
        let names: Vec<&str> = all.iter().map(|workload| workload.name).collect();
        assert_eq!(
            names,
            [
                "bulk_load",
                "small_commits",
                "many_tables",
                "scan",
                "maintenance",
                "deletes_repeated",
                "deletes_bulk",
                "deletes_covering",
                "deletes_partial",
                "deletes_indexed_repeated",
                "deletes_indexed_covering"
            ]
        );

        let phases = |name: &str| -> Vec<&'static str> {
            let workload = all.iter().find(|workload| workload.name == name).unwrap();
            let mut seen = Vec::new();
            for statement in &workload.measured {
                if let Some(phase) = statement.phase
                    && !seen.contains(&phase)
                {
                    seen.push(phase);
                }
            }
            seen
        };
        assert_eq!(phases("bulk_load"), ["create_table", "insert"]);
        assert_eq!(phases("small_commits"), ["inserts"]);
        assert_eq!(phases("many_tables"), ["creates"]);
        assert_eq!(
            phases("scan"),
            ["full_scan", "filtered_scan", "time_travel", "snapshots"]
        );
        assert_eq!(phases("maintenance"), ["merge", "expire", "cleanup"]);
        for name in DELETE_WORKLOADS {
            assert_eq!(phases(name), ["delete"], "{name}");
        }
    }

    const DELETE_WORKLOADS: [&str; 6] = [
        "deletes_repeated",
        "deletes_bulk",
        "deletes_covering",
        "deletes_partial",
        "deletes_indexed_repeated",
        "deletes_indexed_covering",
    ];

    fn delete_workload_named(name: &str) -> Workload {
        workloads(&small())
            .into_iter()
            .find(|workload| workload.name == name)
            .unwrap()
    }

    /// Every delete workload holds exactly one table, or its statement is
    /// charged for the rest of the lake and the timings stop comparing.
    #[test]
    fn each_delete_workload_seeds_one_table_alone() {
        for name in DELETE_WORKLOADS {
            let workload = delete_workload_named(name);
            let created = workload
                .seed
                .iter()
                .filter(|sql| sql.contains("CREATE TABLE"))
                .count();
            assert_eq!(created, 1, "{name}");
            assert!(
                workload.seed.iter().all(|sql| sql.contains(DELETE_TARGET)),
                "{name}"
            );
        }
    }

    /// `deletes_repeated` and `deletes_bulk` must remove the same rows
    /// from the same seed, or the statement-shape cost they bracket is not
    /// what the report shows.
    #[test]
    fn repeated_and_bulk_deletes_cover_the_same_keys_from_one_seed() {
        let scale = small();
        let repeated = delete_workload_named("deletes_repeated");
        let bulk = delete_workload_named("deletes_bulk");

        assert_eq!(repeated.seed, bulk.seed);

        assert_eq!(repeated.measured.len() as u64, scale.deletes);
        assert!(repeated.measured[0].sql.contains("k = 0"));
        assert!(
            repeated
                .measured
                .last()
                .unwrap()
                .sql
                .contains(&format!("k = {}", scale.deletes - 1))
        );

        assert_eq!(bulk.measured.len(), 1);
        assert!(
            bulk.measured[0]
                .sql
                .contains(&format!("k < {}", scale.deletes))
        );
    }

    /// `deletes_covering` and `deletes_partial` differ in the drop and
    /// nothing else: one seed, one target partition, and `partial` leaves
    /// a row behind so its file survives.
    #[test]
    fn partial_controls_for_covering_on_an_identical_seed() {
        let covering = delete_workload_named("deletes_covering");
        let partial = delete_workload_named("deletes_partial");

        assert_eq!(covering.seed, partial.seed);
        assert!(
            covering
                .seed
                .iter()
                .any(|sql| sql.contains("SET PARTITIONED BY (k)"))
        );

        assert!(covering.measured[0].sql.contains("k = 0;"));
        assert!(partial.measured[0].sql.contains("k = 0 AND id > 0"));
    }

    /// An indexed workload is its unindexed twin plus the index, or the
    /// difference between their timings is not the index's cost. Only
    /// moraine has the function, so no other backend may run it.
    #[test]
    fn indexed_delete_workloads_add_only_the_index_to_their_twin() {
        for (twin, name) in [
            ("deletes_repeated", "deletes_indexed_repeated"),
            ("deletes_covering", "deletes_indexed_covering"),
        ] {
            let plain = delete_workload_named(twin);
            let with_index = delete_workload_named(name);

            assert!(with_index.moraine_only, "{name}");
            assert!(!plain.moraine_only, "{twin}");
            assert_eq!(plain.measured.len(), with_index.measured.len(), "{name}");

            let (added, shared) = with_index.seed.split_last().unwrap();
            assert_eq!(shared, plain.seed.as_slice(), "{name}");
            assert!(added.contains("moraine_index_create"), "{name}");
        }
    }

    /// Each delete must exceed the inline limit, so it writes a delete
    /// file rather than tombstones; and one key must outlive the run, so
    /// `deletes_repeated`'s file is never covered and never simply
    /// dropped.
    #[test]
    fn repeated_deletes_write_delete_files_against_a_surviving_file() {
        let scale = small();
        let repeated = delete_workload_named("deletes_repeated");

        const { assert!(ROWS_PER_KEY > INLINE_ROW_LIMIT) };
        assert!(
            repeated
                .seed
                .last()
                .unwrap()
                .contains(&format!("range({})", (scale.deletes + 1) * ROWS_PER_KEY))
        );
    }

    #[test]
    fn small_commits_counts_match_scale() {
        let all = workloads(&small());
        let commits = all
            .iter()
            .find(|workload| workload.name == "small_commits")
            .unwrap();
        let inserts = commits
            .measured
            .iter()
            .filter(|statement| statement.phase == Some("inserts"))
            .count();
        assert_eq!(inserts as u64, small().small_commits);
    }

    #[test]
    fn read_workloads_seed_the_tables_they_query() {
        let all = workloads(&small());
        let scan = all.iter().find(|workload| workload.name == "scan").unwrap();
        assert!(scan.seed[0].contains("CREATE TABLE lake.main.items"));
        assert!(scan.measured[0].sql.contains("FROM lake.main.items"));

        let maintenance = all
            .iter()
            .find(|workload| workload.name == "maintenance")
            .unwrap();
        assert!(maintenance.seed[0].contains("CREATE TABLE lake.main.events"));
        assert!(maintenance.seed.len() as u64 == 1 + small().small_commits);
        // Each seed insert must exceed the inline limit so `merge` has
        // real adjacent data files to coalesce, not just inlined rows.
        assert!(maintenance.seed[1].contains(&format!("range({})", INLINE_ROW_LIMIT * 2)));
    }
}
