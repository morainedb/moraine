use crate::helpers::*;

/// Snapshot expiry and file cleanup, differential against a stock
/// DuckLake catalog fed the identical statements: a dropped table's
/// snapshots expire (all but head), its rows vanish from every
/// metadata table identically to stock, its Parquet lands on the
/// deletion schedule with the bytes intact, and
/// `ducklake_cleanup_old_files` then deletes the bytes and drains the
/// schedule on both catalogs.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_expire_and_cleanup_reclaims_files() {
    let store = TempDir::new("expire-store");
    let data = TempDir::new("expire-data");
    let reference_meta = TempDir::new("expire-ref-meta");
    let reference_data = TempDir::new("expire-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };

    apply(
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(100) t(i);",
    );
    assert_eq!(parquet_files_under(data.path()).len(), 1);
    assert_eq!(parquet_files_under(reference_data.path()).len(), 1);
    apply("DROP TABLE lake.main.t;");

    // Expire everything below head: snapshots 1 (create) and 2
    // (insert) go; 3 (drop) survives. The dropped table's whole row
    // set is now dead, and both catalogs agree on the aftermath.
    apply("CALL ducklake_expire_snapshots('lake', older_than => now());");
    assert_eq!(
        probe("SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot;"),
        vec![vec!["3".to_string()]]
    );
    assert_eq!(
        probe(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_table UNION ALL \
             SELECT count(*) FROM __ducklake_metadata_lake.ducklake_column UNION ALL \
             SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file UNION ALL \
             SELECT count(*) FROM __ducklake_metadata_lake.ducklake_table_stats;"
        ),
        vec![
            vec!["0".to_string()],
            vec!["0".to_string()],
            vec!["0".to_string()],
            vec!["0".to_string()],
        ]
    );

    // Logical expiry deletes no bytes: the Parquet is scheduled, not
    // gone (paths carry catalog-unique names, so counts compare).
    assert_eq!(
        probe(
            "SELECT count(*), bool_and(path_is_relative) \
             FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
        ),
        vec![vec!["1".to_string(), "true".to_string()]]
    );
    assert_eq!(parquet_files_under(data.path()).len(), 1);
    assert_eq!(parquet_files_under(reference_data.path()).len(), 1);

    // Time travel below the horizon no longer resolves — on either.
    run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "SELECT count(*) FROM lake.main.t AT (VERSION => 2);",
    );
    run_reference_ducklake_sql_expect_err(
        reference_meta.path(),
        reference_data.path(),
        "SELECT count(*) FROM lake.main.t AT (VERSION => 2);",
    );

    apply("CALL ducklake_cleanup_old_files('lake', cleanup_all => true);");
    assert!(parquet_files_under(data.path()).is_empty());
    assert!(parquet_files_under(reference_data.path()).is_empty());
    assert_eq!(
        probe(
            "SELECT count(*) \
             FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
        ),
        vec![vec!["0".to_string()]]
    );
}

/// Read-your-writes for the metadata projections: inside one transaction, a
/// scan of a metadata table observes the rows that transaction has already
/// staged against it.
///
/// DuckLake's expiry and cleanup cascades are written this way — they stage
/// deletes and then re-read the same tables with `NOT EXISTS` subqueries to
/// decide what is dead — so a committed-state scan would make a cascade
/// re-plan work it has already done, or refuse to plan work that is now
/// due. Driven directly rather than through a cascade: a cascade that
/// happens not to re-read a given table at the tracked DuckLake version
/// would pass whether or not the overlay existed.
///
/// Every transaction here rolls back, so the assertion after each one is the
/// other half: the overlay is a view, not a write.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn metadata_scans_observe_their_own_transactions_staged_rows() {
    let store = TempDir::new("ryw-store");
    let data = TempDir::new("ryw-data");

    // A real Parquet file, so every file-shaped metadata table has a row:
    // an inlined insert would leave `ducklake_data_file` empty and the
    // whole probe vacuous.
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(100) t(i);",
    );

    // Every kind whose emptiness a plain DELETE can show. Three are left
    // out on purpose: `ducklake_metadata`'s bare DELETE is not a shape the
    // staged path accepts, `ducklake_tag` has no rows to delete here, and
    // `ducklake_schema_versions` is *derived* — its rows re-fold out of the
    // surviving snapshot records, so deleting the stored ones changes
    // nothing, exactly as it does not for the committed projection.
    for table in [
        "ducklake_data_file",
        "ducklake_file_column_stats",
        "ducklake_column",
        "ducklake_table",
        "ducklake_schema",
        "ducklake_table_stats",
        "ducklake_table_column_stats",
        "ducklake_snapshot",
    ] {
        let before = csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            &format!("SELECT count(*) FROM __ducklake_metadata_lake.{table};"),
        ));
        assert_ne!(
            before,
            vec![vec!["0".to_string()]],
            "{table} is empty, so deleting from it would prove nothing"
        );

        let staged = csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            &format!(
                "BEGIN TRANSACTION;\
                 DELETE FROM __ducklake_metadata_lake.{table};\
                 SELECT count(*) FROM __ducklake_metadata_lake.{table};\
                 ROLLBACK;"
            ),
        ));
        assert_eq!(
            staged,
            vec![vec!["0".to_string()]],
            "a scan of {table} inside the transaction that emptied it still \
             reported committed rows"
        );

        let after = csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            &format!("SELECT count(*) FROM __ducklake_metadata_lake.{table};"),
        ));
        assert_eq!(after, before, "the rolled-back {table} delete left a mark");
    }
}

/// Orphaned-file deletion, differential against a stock DuckLake
/// catalog: a stray Parquet no catalog row ever referenced is deleted
/// on both, while every catalogued file survives and both catalogs
/// still answer identically.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_delete_orphaned_files_ignores_catalogued_paths() {
    let store = TempDir::new("orphan-store");
    let data = TempDir::new("orphan-data");
    let reference_meta = TempDir::new("orphan-ref-meta");
    let reference_data = TempDir::new("orphan-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };

    apply(
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(100) t(i);",
    );
    let catalogued = parquet_files_under(data.path());
    assert_eq!(catalogued.len(), 1);

    // Plant a stray file under each table's data prefix: never
    // catalogued, so nothing references it.
    for base in [data.path(), reference_data.path()] {
        std::fs::write(
            base.join("main").join("t").join("stray.parquet"),
            b"not parquet",
        )
        .expect("plant stray file");
    }

    apply("CALL ducklake_delete_orphaned_files('lake', cleanup_all => true);");

    assert_eq!(parquet_files_under(data.path()), catalogued);
    assert_eq!(parquet_files_under(reference_data.path()).len(), 1);
    assert!(
        !data
            .path()
            .join("main")
            .join("t")
            .join("stray.parquet")
            .exists()
    );
    assert!(
        !reference_data
            .path()
            .join("main")
            .join("t")
            .join("stray.parquet")
            .exists()
    );
    assert_eq!(
        probe("SELECT count(*) FROM lake.main.t;"),
        vec![vec!["100".to_string()]]
    );
}

/// Merge never crosses a partition boundary: files spread over two
/// partition values compact to one file per value, never one combined
/// file, so a merged file still carries exactly one partition value and
/// the governing spec stays satisfied. The eligibility rule is
/// DuckLake's, applied before moraine sees the batch — this pins that
/// moraine's served projections do not mislead it into merging across.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_merge_does_not_cross_partition_boundaries() {
    let store = TempDir::new("merge-part-store");
    let data = TempDir::new("merge-part-data");
    let (store, data) = (store.path(), data.path());

    run_ducklake_sql(
        store,
        data,
        "CREATE TABLE lake.main.p (region VARCHAR, v INTEGER);",
    );
    run_ducklake_sql(
        store,
        data,
        "ALTER TABLE lake.main.p SET PARTITIONED BY (region);",
    );

    // Four separate statements, so four files: two per partition value.
    // Each exceeds the inlining limit so they land as real Parquet.
    for region in ["EU", "US"] {
        for batch in 0..2 {
            run_ducklake_sql(
                store,
                data,
                &format!(
                    "INSERT INTO lake.main.p \
                     SELECT '{region}', i FROM range({start}, {end}) t(i);",
                    start = batch * 100,
                    end = batch * 100 + 100,
                ),
            );
        }
    }

    let live_files = "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;";
    assert_eq!(
        csv_rows(&run_standalone_sql(store, live_files)),
        vec![vec!["4".to_string()]],
        "expected one file per insert before merging"
    );

    run_ducklake_sql(store, data, "CALL ducklake_merge_adjacent_files('lake');");

    // Two partition values in, two files out — not one.
    assert_eq!(
        csv_rows(&run_standalone_sql(store, live_files)),
        vec![vec!["2".to_string()]],
        "merge must compact within each partition and not across them"
    );

    // And each surviving file carries exactly one partition value.
    let values_per_file = run_standalone_sql(
        store,
        "SELECT count(DISTINCT pv.partition_value) \
         FROM m.ducklake_data_file f \
         JOIN m.ducklake_file_partition_value pv ON pv.data_file_id = f.data_file_id \
         WHERE f.end_snapshot IS NULL GROUP BY f.data_file_id ORDER BY 1;",
    );
    assert_eq!(
        csv_rows(&values_per_file),
        vec![vec!["1".to_string()], vec!["1".to_string()]],
        "a merged file spanning two partition values would break pruning"
    );

    // The rows themselves survive the merge intact.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data,
            "SELECT region, count(*) FROM lake.main.p GROUP BY region ORDER BY region;",
        )),
        vec![
            vec!["EU".to_string(), "200".to_string()],
            vec!["US".to_string(), "200".to_string()]
        ]
    );
}

/// Merge compaction, differential against a stock DuckLake catalog
/// fed the identical statements: three small files merge into one,
/// rows and row ids are identical to the reference before and after,
/// time travel to a pre-merge snapshot still answers pre-merge, the
/// sources land on the deletion schedule (bytes intact until
/// cleanup), `next_row_id` is untouched, and an UPDATE after the
/// merge still hits the right row on both catalogs (lineage held).
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
#[allow(clippy::too_many_lines)]
fn ducklake_merge_adjacent_files_preserves_rows_and_time_travel() {
    let store = TempDir::new("merge-store");
    let data = TempDir::new("merge-data");
    let reference_meta = TempDir::new("merge-ref-meta");
    let reference_data = TempDir::new("merge-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };

    apply("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    for batch in 0..3 {
        apply(&format!(
            "INSERT INTO lake.main.t \
             SELECT i + {}, concat('v', i) FROM range(100) t(i);",
            batch * 100
        ));
    }
    assert_eq!(
        probe(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
             WHERE end_snapshot IS NULL;"
        ),
        vec![vec!["3".to_string()]]
    );
    let rows_before = probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;");
    let next_row_id_before =
        probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;");
    let pre_merge = probe("SELECT count(*) FROM lake.main.t AT (VERSION => 3);");

    apply("CALL ducklake_merge_adjacent_files('lake');");

    assert_eq!(
        probe(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
             WHERE end_snapshot IS NULL;"
        ),
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;"),
        rows_before,
        "rows and row ids must survive the merge"
    );
    assert_eq!(
        probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;"),
        next_row_id_before,
        "compaction never allocates row ids"
    );

    // The sources are scheduled, bytes intact until cleanup.
    assert_eq!(
        probe(
            "SELECT count(*) \
             FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
        ),
        vec![vec!["3".to_string()]]
    );
    assert_eq!(parquet_files_under(data.path()).len(), 4);
    assert_eq!(parquet_files_under(reference_data.path()).len(), 4);

    // Time travel to a pre-merge snapshot answers exactly as before.
    assert_eq!(
        probe("SELECT count(*) FROM lake.main.t AT (VERSION => 3);"),
        pre_merge
    );

    // Row lineage holds through the merge.
    apply("UPDATE lake.main.t SET b = 'updated' WHERE a = 150;");
    assert_eq!(
        probe("SELECT b FROM lake.main.t WHERE a = 150;"),
        vec![vec!["updated".to_string()]]
    );

    apply("CALL ducklake_cleanup_old_files('lake', cleanup_all => true);");
    assert_eq!(
        probe(
            "SELECT count(*) \
             FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
        ),
        vec![vec!["0".to_string()]]
    );
    assert_eq!(
        probe("SELECT count(*) FROM lake.main.t;"),
        vec![vec!["300".to_string()]]
    );
}

/// Delete-rewrite compaction, differential against a stock DuckLake
/// catalog fed the identical statements: after a DELETE, the rewrite
/// leaves one live data file and no live delete file, survivors keep
/// their row ids row-for-row with the reference, and time travel to
/// the pre-rewrite snapshot still shows the deleted rows.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_rewrite_data_files_materializes_deletes() {
    let store = TempDir::new("rewrite-store");
    let data = TempDir::new("rewrite-data");
    let reference_meta = TempDir::new("rewrite-ref-meta");
    let reference_data = TempDir::new("rewrite-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };

    apply(
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(100) t(i);\
         DELETE FROM lake.main.t WHERE a % 2 = 0;",
    );
    assert_eq!(
        probe(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file \
             WHERE end_snapshot IS NULL;"
        ),
        vec![vec!["1".to_string()]]
    );
    let survivors_before = probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;");

    apply("CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.1);");

    assert_eq!(
        probe(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
             WHERE end_snapshot IS NULL;"
        ),
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        probe(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file \
             WHERE end_snapshot IS NULL;"
        ),
        vec![vec!["0".to_string()]],
        "the rewrite consumes the delete file"
    );
    assert_eq!(
        probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;"),
        survivors_before,
        "survivors keep their row ids"
    );

    // The ended rows stay in history: time travel to the pre-delete
    // snapshot still sees all 100 rows.
    assert_eq!(
        probe("SELECT count(*) FROM lake.main.t AT (VERSION => 2);"),
        vec![vec!["100".to_string()]]
    );
}

/// `moraine_maintenance` with nothing configured at attach is a
/// moraine-only pass: every DuckLake step reports `skipped`, the
/// orphaned-index sweep runs, and no data moves.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_without_configuration_runs_only_the_sweep() {
    let store = TempDir::new("maint-bare-store");
    let data = TempDir::new("maint-bare-data");

    let rows = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(20) t(i);\
         SELECT step, status FROM moraine_maintenance('lake');",
    ));
    let by_step: std::collections::HashMap<_, _> = rows
        .iter()
        .map(|row| (row[0].as_str(), row[1].as_str()))
        .collect();

    for step in [
        "expire_snapshots",
        "flush_inlined_data",
        "merge_adjacent_files",
        "rewrite_data_files",
        "cleanup_old_files",
        "delete_orphaned_files",
    ] {
        assert_eq!(
            by_step.get(step),
            Some(&"skipped"),
            "unconfigured `{step}` must not run: {rows:?}"
        );
    }
    assert_eq!(by_step.get("sweep_indexes"), Some(&"ran"), "{rows:?}");
    // Shares `sweep_indexes`' pass and its switch, so it runs with it.
    assert_eq!(by_step.get("sweep_file_stats"), Some(&"ran"), "{rows:?}");
    assert_eq!(by_step.get("compact_store"), Some(&"skipped"), "{rows:?}");

    // Nothing the pass did is observable in the data.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            "SELECT count(*), sum(a) FROM lake.main.t;"
        )),
        vec![vec!["20".to_string(), "190".to_string()]]
    );
}

/// The sweep reclaims a dropped index's entries and nothing else: a live
/// index is untouched, the drop orphans its range, and the next pass
/// reports exactly that range reclaimed. A second pass finds nothing.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_sweeps_a_dropped_index() {
    let store = TempDir::new("maint-sweep-store");
    let data = TempDir::new("maint-sweep-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    let detail = |sql: &str| -> String {
        let rows = csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &meta,
            sql,
        ));
        rows.into_iter()
            .next()
            .map(|row| row[0].clone())
            .unwrap_or_default()
    };

    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(25) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);",
    );

    // A live index is spared.
    assert!(
        detail("SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_indexes';")
            .contains("0 entries"),
        "a live index must not be swept"
    );

    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "SELECT * FROM moraine_index_drop('lake','main','t','by_a');",
    );

    let swept =
        detail("SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_indexes';");
    assert!(
        swept.contains("25 entries") && swept.contains("1 dropped index"),
        "expected the whole range reclaimed, got: {swept}"
    );

    // Idempotent: the range is gone.
    assert!(
        detail("SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_indexes';")
            .contains("0 entries"),
        "a second pass must find nothing"
    );
}

/// A configured pass runs DuckLake's own functions in sequence order and
/// leaves the lake's contents unchanged.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_runs_configured_ducklake_steps_in_order() {
    let store = TempDir::new("maint-full-store");
    let data = TempDir::new("maint-full-data");
    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN now(), \
         META_MAINTENANCE_FLUSH_INLINED_DATA true, META_MAINTENANCE_MERGE_ADJACENT_FILES true, \
         META_MAINTENANCE_CLEANUP_OLD_FILES_CLEANUP_ALL true",
        data.path().display()
    );

    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(10) t(i);\
         INSERT INTO lake.main.t VALUES (99);\
         SELECT step, status FROM moraine_maintenance('lake');",
    ));

    // Reported in sequence order, with the configured steps run.
    let order: Vec<_> = rows.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(
        order,
        vec![
            "expire_snapshots",
            "flush_inlined_data",
            "merge_adjacent_files",
            "rewrite_data_files",
            "cleanup_old_files",
            "delete_orphaned_files",
            "sweep_indexes",
            "sweep_file_stats",
            "compact_store",
        ],
        "steps must report in sequence order"
    );
    let by_step: std::collections::HashMap<_, _> = rows
        .iter()
        .map(|row| (row[0].as_str(), row[1].as_str()))
        .collect();
    for step in [
        "expire_snapshots",
        "flush_inlined_data",
        "merge_adjacent_files",
        "cleanup_old_files",
        "sweep_indexes",
        "sweep_file_stats",
    ] {
        assert_eq!(by_step.get(step), Some(&"ran"), "{step} in {rows:?}");
    }
    // Unconfigured steps stay skipped even in a full pass.
    assert_eq!(by_step.get("rewrite_data_files"), Some(&"skipped"));
    assert_eq!(by_step.get("delete_orphaned_files"), Some(&"skipped"));

    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT count(*), sum(a) FROM lake.main.t;"
        )),
        vec![vec!["11".to_string(), "144".to_string()]],
        "a maintenance pass must not change what the lake contains"
    );
}

/// The trigger refuses inside an explicit transaction rather than
/// deadlocking against the pass's own connection.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_refuses_inside_an_explicit_transaction() {
    let store = TempDir::new("maint-tx-store");
    let data = TempDir::new("maint-tx-data");
    let error = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         BEGIN;\
         SELECT * FROM moraine_maintenance('lake');",
    );
    assert!(
        error.contains("explicit transaction"),
        "expected a transaction refusal, got: {error}"
    );
}

/// The census reports one row per subspace, from a read-write and a
/// read-only attach alike, and carries live counts only when asked for
/// them — the scanning leg costs a full read of the store.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn store_census_reports_every_subspace() {
    let store = TempDir::new("census-store");
    let data = TempDir::new("census-data");

    let rows = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(20) t(i);\
         SELECT subspace, live_keys FROM moraine_store_census('lake') ORDER BY subspace;",
    ));
    let names: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    for subspace in ["current", "history", "index", "snapshot"] {
        assert!(names.contains(&subspace), "no `{subspace}` row: {rows:?}");
    }
    // Without the scanning leg the live columns are NULL, not zero: a
    // subspace with no live keys and one nobody counted differ.
    assert!(
        rows.iter().all(|row| row[1] == "NULL"),
        "unrequested live counts: {rows:?}"
    );

    let counted = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT live_keys, scheduled_files FROM moraine_store_census('lake', live := true) \
         WHERE subspace = 'current';",
    ));
    let live: u64 = counted[0][0].parse().expect("a live count");
    assert!(live > 0, "{counted:?}");
    // Nothing has been expired, so the deletion schedule is empty.
    assert_eq!(counted[0][1], "0", "{counted:?}");

    // An operator investigating a production store attaches read-only,
    // and the census is the part of this surface that serves them.
    let read_only = csv_rows(&run_ducklake_read_only_sql(
        store.path(),
        data.path(),
        "SELECT count(*) FROM moraine_store_census('lake');",
    ));
    assert_eq!(
        read_only[0][0].parse::<usize>().expect("a count"),
        names.len()
    );
}

/// A configured pass runs the store merge and reports it; an
/// unconfigured one skips it. The merge reclaims substrate bytes and
/// moves nothing a query can observe.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_merges_the_store_when_configured() {
    let store = TempDir::new("maint-merge-store");
    let data = TempDir::new("maint-merge-data");
    let configured =
        ", META_MAINTENANCE_COMPACT_STORE true, META_MAINTENANCE_COMPACT_STORE_TIMEOUT 60";

    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        configured,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(50) t(i);\
         SELECT step, status, detail FROM moraine_maintenance('lake') WHERE step = 'compact_store';",
    ));
    assert_eq!(rows[0][1], "ran", "{rows:?}");
    // Every subspace is accounted for, whether it had runs to merge or
    // not, so two passes stay comparable. The detail is one clause per
    // subspace, which CSV splits across fields.
    let detail = rows[0][2..].join(",");
    for subspace in ["current", "history", "index", "snapshot"] {
        assert!(
            detail.contains(subspace),
            "no `{subspace}` clause: {rows:?}"
        );
    }

    // The merge mints no snapshot and moves no rows.
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            configured,
            "SELECT count(*), sum(a) FROM lake.main.t;"
        )),
        vec![vec!["50".to_string(), "1225".to_string()]]
    );
}

/// The store merge has a trigger of its own, so merging once needs no
/// re-attach: it reports a row per subspace, refuses a name it does not
/// know, and moves nothing a query can observe.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn compact_store_merges_on_demand() {
    let store = TempDir::new("compact-now-store");
    let data = TempDir::new("compact-now-data");

    // No MAINTENANCE_* option anywhere: the point of the trigger is that a
    // one-off merge needs no attach-time configuration.
    let rows = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(50) t(i);\
         SELECT subspace, outcome FROM moraine_compact_store('lake') ORDER BY subspace;",
    ));
    let names: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    for subspace in ["current", "history", "index", "snapshot"] {
        assert!(names.contains(&subspace), "no `{subspace}` row: {rows:?}");
    }
    // A store this small has no sorted runs, so every tree is skipped —
    // reported rather than omitted, so two calls stay comparable.
    assert!(
        rows.iter().all(|row| row[1] == "skipped"),
        "unexpected outcome: {rows:?}"
    );

    // Narrowing to one subspace reports only that one.
    let one = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT subspace FROM moraine_compact_store('lake', subspace := 'current');",
    ));
    assert_eq!(one, vec![vec!["current".to_string()]]);

    let unknown = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "SELECT * FROM moraine_compact_store('lake', subspace := 'gcfile');",
    );
    assert!(
        unknown.contains("no subspace named") && unknown.contains("current"),
        "got: {unknown}"
    );

    // The merge mints no snapshot and moves no rows.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            "SELECT count(*), sum(a) FROM lake.main.t;"
        )),
        vec![vec!["50".to_string(), "1225".to_string()]]
    );
}

/// An operator can require the targeted merge to finish rather than
/// mistaking successful submission for successful compaction.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn compact_store_can_require_completion() {
    let store = TempDir::new("compact-verified-store");
    let data = TempDir::new("compact-verified-data");

    let rows = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t VALUES (1);\
         SELECT subspace, outcome FROM moraine_compact_store(\
           'lake', subspace := 'index', timeout := 10, require_completed := true\
         );",
    ));
    assert_eq!(rows, vec![vec!["index".to_string(), "skipped".to_string()]]);

    let refused = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "SELECT * FROM moraine_compact_store(\
           'lake', subspace := 'index', require_completed := true\
         );",
    );
    assert!(
        refused.contains("require_completed needs a timeout"),
        "got: {refused}"
    );
}

/// The merge runs inside the writer, so a read-only attach refuses it
/// while still serving the census.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn compact_store_refuses_a_read_only_attach() {
    let store = TempDir::new("compact-ro-store");
    let data = TempDir::new("compact-ro-data");

    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT); INSERT INTO lake.main.t VALUES (1);",
    );

    // The census serves a reader; the merge does not.
    let census = csv_rows(&run_ducklake_read_only_sql(
        store.path(),
        data.path(),
        "SELECT count(*) FROM moraine_store_census('lake');",
    ));
    assert!(census[0][0].parse::<usize>().expect("a count") > 0);

    let refused = run_ducklake_read_only_sql_expect_err(
        store.path(),
        data.path(),
        "SELECT * FROM moraine_compact_store('lake');",
    );
    assert!(
        refused.contains("read-only") && refused.contains("writer"),
        "got: {refused}"
    );
}

/// A merge step disabled while one of its own parameters is supplied is
/// two contradictory instructions, and fails at bind rather than
/// resolving silently.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_rejects_a_contradictory_merge_configuration() {
    let store = TempDir::new("maint-merge-badopt-store");
    let data = TempDir::new("maint-merge-badopt-data");

    let contradictory = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        ", META_MAINTENANCE_COMPACT_STORE false, META_MAINTENANCE_COMPACT_STORE_SUBSPACE 'current'",
        "SELECT 1;",
    );
    assert!(
        contradictory.contains("but one of its parameters was supplied"),
        "got: {contradictory}"
    );

    // A subspace name is checked at attach rather than when a pass runs:
    // a typo caught only at pass time would attach cleanly and then fail
    // every scheduled pass, unattended, for as long as it stood.
    let unknown_subspace = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        ", META_MAINTENANCE_COMPACT_STORE_SUBSPACE 'gcfile'",
        "SELECT 1;",
    );
    assert!(
        unknown_subspace.contains("names no subspace") && unknown_subspace.contains("current"),
        "got: {unknown_subspace}"
    );
}

/// A misconfigured attach fails at bind with a message naming the
/// problem, rather than starting a scheduler that silently does the
/// wrong thing.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_rejects_unknown_and_contradictory_options() {
    let store = TempDir::new("maint-badopt-store");
    let data = TempDir::new("maint-badopt-data");

    let unknown = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        ", META_MAINTENANCE_NONSENSE true",
        "SELECT 1;",
    );
    assert!(
        unknown.contains("unknown maintenance option"),
        "got: {unknown}"
    );

    let contradictory = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        ", META_MAINTENANCE_EXPIRE_SNAPSHOTS false, META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN now()",
        "SELECT 1;",
    );
    assert!(
        contradictory.contains("but one of its parameters was supplied"),
        "got: {contradictory}"
    );
}

/// Nesting the catalog store inside `DATA_PATH` is refused at attach:
/// orphan cleanup lists `DATA_PATH` and would delete the catalog itself.
///
/// The guard fires on the data path moraine is actually told about —
/// `META_DATA_PATH`, or a value already recorded for the lake. DuckLake
/// keeps its own unprefixed `DATA_PATH` for the data layer and does not
/// forward it to this metadata attach, so an attach that names only that
/// leaves moraine nothing to check.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn attach_refuses_a_data_path_containing_the_catalog() {
    let root = TempDir::new("maint-overlap");
    let store_dir = root.path().join("catalog");
    std::fs::create_dir_all(&store_dir).expect("create catalog dir");

    // DATA_PATH is the catalog's own parent, so orphan cleanup would
    // sweep the catalog's own objects.
    let nested = format!(", META_DATA_PATH '{}'", root.path().display());
    let error =
        run_ducklake_sql_expect_err_with_options(&store_dir, root.path(), &nested, "SELECT 1;");
    assert!(
        error.contains("nested on the same object store"),
        "expected the overlap refusal, got: {error}"
    );

    // A sibling data path attaches normally.
    let sibling_data = TempDir::new("maint-overlap-data");
    let safe = format!(", META_DATA_PATH '{}'", sibling_data.path().display());
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            &store_dir,
            sibling_data.path(),
            &safe,
            "SELECT 1;"
        )),
        vec![vec!["1".to_string()]],
        "sibling locations must attach"
    );
}

/// The status surface retains more than the newest pass, so a pass that
/// did something stays visible after a later one that did not — the
/// property that makes a failing schedule findable rather than erased by
/// the next success.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_status_survives_restart_and_retains_earlier_passes() {
    let store = TempDir::new("maint-status-store");
    let data = TempDir::new("maint-status-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    // Complete two passes and let the writer process exit before reading
    // status. The second attach is read-only, proving both that the rows
    // came from the catalog and that inspecting them schedules no work.
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(7) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);\
         SELECT * FROM moraine_index_drop('lake','main','t','by_a');\
         SELECT count(*) FROM moraine_maintenance('lake');\
         SELECT count(*) FROM moraine_maintenance('lake');",
    );
    let output = run_ducklake_read_only_sql(
        store.path(),
        data.path(),
        "SELECT 'PASS' AS marker, trigger, detail FROM moraine_maintenance_status('lake') \
           WHERE step = 'sweep_indexes' ORDER BY started_at DESC;",
    );
    let passes: Vec<Vec<String>> = csv_rows(&output)
        .into_iter()
        .filter(|row| row.first().is_some_and(|marker| marker == "PASS"))
        .collect();

    // Both passes are retained, newest first, and each records what drove
    // it. The reclaiming pass survives the empty one that followed.
    assert_eq!(passes.len(), 2, "both passes must be retained: {passes:?}");
    assert_eq!(passes[0][1], "manual");
    assert!(
        passes[0][2].contains("0 entries"),
        "newest pass reclaimed nothing: {passes:?}"
    );
    assert!(
        passes[1][2].contains("7 entries"),
        "the earlier reclaiming pass must still be visible: {passes:?}"
    );
}

/// Maintenance mutates, so a read-only attach neither schedules a pass nor
/// runs one on demand: the trigger refuses, and a fresh catalog with no prior
/// writer pass keeps an empty durable status history.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_never_runs_on_a_read_only_attach() {
    let store = TempDir::new("maint-ro-store");
    let data = TempDir::new("maint-ro-data");

    // Bootstrap read-write, then reattach read-only with a schedule that
    // would otherwise start a thread.
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(5) t(i);",
    );

    let output = run_ducklake_read_only_sql(
        store.path(),
        data.path(),
        "SELECT count(*) FROM moraine_maintenance_status('lake');",
    );
    assert_eq!(
        csv_rows(&output),
        vec![vec!["0".to_string()]],
        "a read-only attach must not add a status pass"
    );
}

/// A failed DuckLake step abandons the rest of that sequence — its steps
/// depend on each other — but never the sweep, which depends on none of
/// them. Suppressing the sweep would stop moraine's own reclamation on
/// every future pass for as long as the misconfiguration stood.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_sweeps_even_when_a_ducklake_step_fails() {
    let store = TempDir::new("maint-fail-store");
    let data = TempDir::new("maint-fail-data");
    // DuckLake rejects a delete threshold outside 0..1 at bind.
    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_REWRITE_DATA_FILES_DELETE_THRESHOLD 99.0",
        data.path().display()
    );

    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(9) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);\
         SELECT * FROM moraine_index_drop('lake','main','t','by_a');\
         SELECT step, status, detail FROM moraine_maintenance('lake');",
    ));
    let by_step: std::collections::HashMap<_, _> = rows
        .iter()
        .filter(|row| row.len() == 3)
        .map(|row| (row[0].as_str(), (row[1].as_str(), row[2].as_str())))
        .collect();

    assert_eq!(
        by_step.get("rewrite_data_files").map(|entry| entry.0),
        Some("failed"),
        "{rows:?}"
    );
    // Steps after the failure are reported, not dropped, so every pass
    // emits the same rows and two passes stay comparable.
    for later in ["cleanup_old_files", "delete_orphaned_files"] {
        let (status, detail) = by_step[later];
        assert_eq!(status, "skipped", "{later} in {rows:?}");
        assert!(
            detail.contains("not attempted: rewrite_data_files failed"),
            "{later} should name what aborted it: {detail}"
        );
    }
    // The whole point: reclamation still happened.
    let (status, detail) = by_step["sweep_indexes"];
    assert_eq!(status, "ran", "the sweep must survive a failed step");
    assert!(detail.contains("9 entries"), "got: {detail}");
}

/// The scheduler runs a pass with nobody driving it: an attach that
/// configures an interval reclaims a pre-existing orphaned range on its
/// own, and the retained window records the pass as `scheduled`.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn scheduler_runs_a_pass_unattended() {
    let store = TempDir::new("maint-timer-store");
    let data = TempDir::new("maint-timer-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    // Orphan a range in a session with no scheduler, so the only thing
    // that can reclaim it is the timer in the session that follows.
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(11) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);\
         SELECT * FROM moraine_index_drop('lake','main','t','by_a');",
    );

    let scheduled = format!("{meta}, META_MAINTENANCE_INTERVAL INTERVAL '200 milliseconds'");
    let output = run_ducklake_sql_with_pause(
        store.path(),
        data.path(),
        &scheduled,
        // Nothing but the attach; the timer is the only actor.
        "SELECT 1;\n",
        std::time::Duration::from_millis(1_500),
        "SELECT 'PASS' AS marker, trigger, detail FROM moraine_maintenance_status('lake') \
           WHERE step = 'sweep_indexes' ORDER BY started_at;\n",
    );
    let passes: Vec<Vec<String>> = csv_rows(&output)
        .into_iter()
        .filter(|row| row.first().is_some_and(|marker| marker == "PASS"))
        .collect();

    assert!(
        !passes.is_empty(),
        "the timer must have run at least one pass: {output}"
    );
    assert!(
        passes.iter().all(|pass| pass[1] == "scheduled"),
        "every pass here is timer-driven, not triggered: {passes:?}"
    );
    // The first pass found the orphaned range; later ones find nothing,
    // which also shows repeated ticks are harmless.
    assert!(
        passes[0][2].contains("11 entries"),
        "the first unattended pass must reclaim the range: {passes:?}"
    );
    assert!(
        passes[1..].iter().all(|pass| pass[2].contains("0 entries")),
        "later passes must find nothing left: {passes:?}"
    );
}

/// Orphans `entries` index entries in a session of its own, so a later
/// session's scheduler has something slow to reclaim.
///
/// A pass over them with `MAINTENANCE_BATCH_SIZE 1` takes one commit per
/// entry, so its wall-clock is the entry count times per-commit compute
/// (sub-millisecond, since reclaim batches do not await durability).
/// Enough entries and the pass outruns a sub-second interval, which is
/// the only way to provoke tick contention without a test-only knob.
fn orphaned_range(store: &TempDir, data: &TempDir, entries: u64) {
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        &format!(
            "CREATE TABLE lake.main.t(a BIGINT);\
             INSERT INTO lake.main.t SELECT i FROM range({entries}) t(i);\
             SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);\
             SELECT * FROM moraine_index_drop('lake','main','t','by_a');"
        ),
    );
}

/// Reads the marked pass rows out of a paused session's output.
fn marked_passes(output: &str) -> Vec<Vec<String>> {
    csv_rows(output)
        .into_iter()
        .filter(|row| row.first().is_some_and(|marker| marker == "PASS"))
        .collect()
}

/// A tick arriving while a pass is still running skips rather than
/// queueing behind it.
///
/// The signal is that **no pass ever observes a partially reclaimed
/// range**: one pass takes the whole range and every other takes
/// nothing. Two passes running concurrently over the same `index` range
/// would each delete part of it and report a split. Counting passes
/// would not work — once the slow pass finishes, later ticks correctly
/// run fast empty passes for the rest of the window.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn scheduler_ticks_skip_a_pass_already_running() {
    const ENTRIES: u64 = 1_500;
    let store = TempDir::new("maint-single-store");
    let data = TempDir::new("maint-single-data");
    orphaned_range(&store, &data, ENTRIES);

    // The pass takes 1 500 commits — about a second, several ticks — so
    // ticks land while it is still running. The window holds 14 ticks,
    // fewer than the report retains passes, so the pass that claims the
    // range cannot be pushed out of the report by the empty ones after
    // it however fast the machine is.
    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_INTERVAL INTERVAL '300 milliseconds', \
         META_MAINTENANCE_BATCH_SIZE 1",
        data.path().display()
    );
    let output = run_ducklake_sql_with_pause(
        store.path(),
        data.path(),
        &options,
        "SELECT 1;\n",
        std::time::Duration::from_millis(4_200),
        "SELECT 'PASS' AS marker, detail FROM moraine_maintenance_status('lake') \
           WHERE step = 'sweep_indexes' ORDER BY started_at;\n",
    );
    let passes = marked_passes(&output);
    assert!(
        passes.len() > 1,
        "too few passes to prove anything: {output}"
    );

    let counts: Vec<u64> = passes
        .iter()
        .map(|pass| {
            pass[1]
                .split_whitespace()
                .nth(1)
                .and_then(|count| count.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("unparseable detail: {}", pass[1]))
        })
        .collect();

    let claiming: Vec<u64> = counts.iter().copied().filter(|count| *count > 0).collect();
    assert_eq!(
        claiming,
        vec![ENTRIES],
        "exactly one pass must take the whole range; a split means two \
         passes ran over it concurrently: {passes:?}"
    );
}

/// Detaching *while a pass is running* stops and joins the thread before
/// the handle is released. The join blocks until the pass finishes,
/// which is what keeps a pass from ever touching a detached database —
/// this pins that it completes rather than deadlocking against whatever
/// detach itself holds.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn detach_during_a_running_pass_completes() {
    let store = TempDir::new("maint-detach-store");
    let data = TempDir::new("maint-detach-data");
    orphaned_range(&store, &data, 1_500);

    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_INTERVAL INTERVAL '100 milliseconds', \
         META_MAINTENANCE_BATCH_SIZE 1",
        data.path().display()
    );
    // The first tick fires at ~100ms and the pass then runs for about a
    // second, so detaching at 500ms lands squarely inside it.
    let output = run_ducklake_sql_with_pause(
        store.path(),
        data.path(),
        &options,
        "SELECT 1;\n",
        std::time::Duration::from_millis(500),
        "DETACH lake;\nSELECT 'SURVIVED' AS marker;\n",
    );

    assert!(
        csv_rows(&output)
            .iter()
            .any(|row| row.first().is_some_and(|marker| marker == "SURVIVED")),
        "detaching mid-pass must complete, not hang or fail: {output}"
    );
}

/// Graceful process shutdown takes the last-host-context path when no explicit
/// detach occurs. An active pass owns a connection that retains the database,
/// so waiting for database destruction to stop it is an ownership cycle. The
/// last host context must stop and join the pass before releasing its own
/// database reference.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn database_shutdown_during_a_running_pass_completes_without_detach() {
    let store = TempDir::new("maint-shutdown-store");
    let data = TempDir::new("maint-shutdown-data");
    // Ten times the range used by the detach test keeps this pass in flight
    // well beyond the short pre-shutdown pause, even on a release build.
    orphaned_range(&store, &data, 15_000);

    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_INTERVAL INTERVAL '100 milliseconds', \
         META_MAINTENANCE_BATCH_SIZE 1",
        data.path().display()
    );
    // The first tick starts at ~100ms. At 300ms stdin closes without a
    // DETACH statement, making last-host-context destruction own the join.
    run_ducklake_sql_with_pause(
        store.path(),
        data.path(),
        &options,
        "SELECT 1;\n",
        std::time::Duration::from_millis(300),
        "",
    );

    // A shutdown that merely killed the scheduler thread could leave the
    // range partially reclaimed. A new pass must find the joined pass's work
    // complete.
    let output = run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT 'PASS' AS marker, detail FROM moraine_maintenance('lake') \
           WHERE step = 'sweep_indexes';",
    );
    let sweep = csv_rows(&output)
        .into_iter()
        .find(|row| row.first().is_some_and(|marker| marker == "PASS"))
        .unwrap_or_else(|| panic!("no sweep row after shutdown: {output}"));
    assert!(
        sweep[1].contains("0 entries"),
        "last-host-context shutdown did not wait for the active pass: {sweep:?}"
    );
}

/// A lake attached with `METADATA_CATALOG` names its metadata catalog
/// itself, so the default `__ducklake_metadata_<lake>` naming does not
/// find it and stripping that prefix does not recover the lake name.
/// Both are resolved by matching attached databases on path instead: the
/// trigger accepts either name, and the pass still calls DuckLake's
/// functions with the *lake* name.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_resolves_a_custom_metadata_catalog_name() {
    let store = TempDir::new("maint-custom-meta-store");
    let data = TempDir::new("maint-custom-meta-data");
    let options = format!(
        ", META_DATA_PATH '{}', METADATA_CATALOG 'custom_meta', \
         META_MAINTENANCE_MERGE_ADJACENT_FILES true",
        data.path().display()
    );

    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(6) t(i);\
         SELECT 'LAKE' AS via, step, status FROM moraine_maintenance('lake') \
           WHERE step = 'merge_adjacent_files';\
         SELECT 'META' AS via, step, status FROM moraine_maintenance('custom_meta') \
           WHERE step = 'merge_adjacent_files';",
    ));

    // Both spellings reach the same scheduler, and the DuckLake step runs
    // rather than failing on a name DuckLake does not know.
    for via in ["LAKE", "META"] {
        let found = rows
            .iter()
            .find(|row| row.first().is_some_and(|marker| marker == via))
            .unwrap_or_else(|| panic!("no row for {via}: {rows:?}"));
        assert_eq!(found[2], "ran", "{via} did not run the step: {rows:?}");
    }

    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT count(*) FROM lake.main.t;"
        )),
        vec![vec!["6".to_string()]]
    );
}

/// `expire_snapshots`' `versions` parameter takes a list, which
/// DuckLake's `META_` passthrough cannot carry — a list-valued `META_`
/// option fails the attach outright, and not only for moraine's options.
/// DuckLake accepts the same value spelled as a string, so that is how
/// the parameter is reachable, and it passes through unaltered.
///
/// The versions it names are interior, so this also pins that expiry is
/// not tail-only: snapshots above the expired ones survive untouched.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_passes_a_list_parameter_spelled_as_a_string() {
    let store = TempDir::new("maint-versions-store");
    let data = TempDir::new("maint-versions-data");
    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_EXPIRE_SNAPSHOTS_VERSIONS '[1,2]'",
        data.path().display()
    );

    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(3) t(i);\
         INSERT INTO lake.main.t VALUES (9);\
         INSERT INTO lake.main.t VALUES (10);\
         SELECT 'STEP' AS marker, status FROM moraine_maintenance('lake') \
           WHERE step = 'expire_snapshots';\
         SELECT 'SNAP' AS marker, snapshot_id \
           FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id;",
    ));

    assert!(
        rows.iter().any(|row| row[0] == "STEP" && row[1] == "ran"),
        "the step must run with a string-spelled list: {rows:?}"
    );
    // Bootstrap plus four mutations gives snapshots 0..4; naming
    // versions 1 and 2 expires exactly those two and nothing else.
    let snapshots: Vec<&str> = rows
        .iter()
        .filter(|row| row[0] == "SNAP")
        .map(|row| row[1].as_str())
        .collect();
    assert_eq!(snapshots, vec!["0", "3", "4"], "{rows:?}");

    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT count(*) FROM lake.main.t;"
        )),
        vec![vec!["5".to_string()]]
    );
}

/// `older_than` given an interval becomes a rolling window rather than a
/// frozen instant. Attach options are evaluated once, so a timestamp
/// written as `now()` would render as a literal and a schedule would keep
/// expiring against its attach-time instant forever.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn maintenance_renders_an_interval_older_than_as_a_rolling_window() {
    let store = TempDir::new("maint-rolling-store");
    let data = TempDir::new("maint-rolling-data");
    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN INTERVAL '7 days', \
         META_MAINTENANCE_CLEANUP_OLD_FILES_OLDER_THAN INTERVAL '1 hour'",
        data.path().display()
    );

    // The rendered SQL contains commas, so the match runs in SQL and the
    // column comes back as a boolean rather than something to re-split.
    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(4) t(i);\
         SELECT step, status, \
                detail LIKE '%now()%' AND detail LIKE '%INTERVAL%' AS rolling \
           FROM moraine_maintenance('lake') \
           WHERE step IN ('expire_snapshots', 'cleanup_old_files') ORDER BY step;",
    ));

    assert_eq!(
        rows,
        vec![
            vec![
                "cleanup_old_files".to_string(),
                "ran".to_string(),
                "true".to_string()
            ],
            vec![
                "expire_snapshots".to_string(),
                "ran".to_string(),
                "true".to_string()
            ],
        ],
        "both steps must run and carry a window re-evaluated per pass"
    );

    // A rolling window this wide expires nothing, so the lake is intact.
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT count(*) FROM lake.main.t;"
        )),
        vec![vec!["4".to_string()]]
    );
}

/// Compaction after expiry, differential against a stock DuckLake
/// catalog: `ducklake_schema_versions` keeps the rows the expired
/// snapshots wrote, so `ducklake_merge_adjacent_files` still resolves
/// each data file's schema version and merges. Deriving those rows from
/// surviving snapshots instead leaves the compaction planner's join
/// with a NULL and aborts it at bind.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_merge_adjacent_files_survives_expiry() {
    let store = TempDir::new("merge-expire-store");
    let data = TempDir::new("merge-expire-data");
    let reference_meta = TempDir::new("merge-expire-ref-meta");
    let reference_data = TempDir::new("merge-expire-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };

    // Three small Parquet files, each from its own snapshot: merge input
    // once flushed. `ALTER TABLE` gives the table a second schema
    // version, so the rows being retained are more than one.
    apply("CREATE TABLE lake.main.t(a BIGINT);");
    for range in ["range(10)", "range(10, 20)", "range(20, 30)"] {
        apply(&format!(
            "INSERT INTO lake.main.t SELECT i FROM {range} t(i);\
             CALL ducklake_flush_inlined_data('lake');"
        ));
    }
    apply("ALTER TABLE lake.main.t ADD COLUMN b VARCHAR;");
    let recorded = probe(
        "SELECT begin_snapshot, schema_version, table_id \
         FROM __ducklake_metadata_lake.ducklake_schema_versions ORDER BY 1, 3;",
    );
    assert_eq!(recorded.len(), 2, "create and alter each record a row");

    // Expiry takes every snapshot below the head — including the ones
    // those rows were written in, and every one the three data files
    // were written in.
    apply("CALL ducklake_expire_snapshots('lake', older_than => now());");
    assert_eq!(
        probe("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot;"),
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        probe(
            "SELECT begin_snapshot, schema_version, table_id \
             FROM __ducklake_metadata_lake.ducklake_schema_versions ORDER BY 1, 3;"
        ),
        recorded
    );

    assert_eq!(
        probe("SELECT files_processed, files_created FROM ducklake_merge_adjacent_files('lake');"),
        vec![vec!["3".to_string(), "1".to_string()]]
    );
    assert_eq!(
        probe("SELECT count(*), sum(a) FROM lake.main.t;"),
        vec![vec!["30".to_string(), "435".to_string()]]
    );
}

/// Reads racing the scheduler's own expiry pass.
///
/// DuckLake resolves a transaction's snapshot by reading
/// `ducklake_snapshot` twice in one statement — the row whose id is the
/// maximum, and the maximum itself. Each scan is one dump and two dumps
/// observe two committed heads, so an expiry landing between them leaves
/// the maximum naming a row the other scan never saw. DuckLake reports
/// that as "No snapshot found" and cannot re-resolve: it was handed an
/// empty result, not a typed error. Materializing the table once per
/// DuckDB transaction is what makes both halves one row set.
///
/// `test/sql/metadata_read_pinning.test` pins the mechanism
/// deterministically, over two connections. This is the live shape it was
/// found in: an expiry pass ticking every 50 ms under a session that keeps
/// reading and committing, with `older_than => now()` so every pass has
/// the whole tail to reclaim. Any statement in the session that fails —
/// including on "No snapshot found" — fails the test.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn reads_survive_an_expiry_pass_running_under_them() {
    const ROUNDS: usize = 120;
    let store = TempDir::new("maint-race-store");
    let data = TempDir::new("maint-race-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    // A tail of snapshots for the first pass to find, minted in a session
    // with no scheduler so nothing reclaims them before the race starts.
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(20) t(i);\
         INSERT INTO lake.main.t SELECT i FROM range(20) t(i);\
         INSERT INTO lake.main.t SELECT i FROM range(20) t(i);",
    );

    // `older_than` as an interval renders as a rolling window, so every
    // tick expires everything below head rather than freezing on an
    // attach-time instant. The session's own commits keep refilling the
    // tail the passes reclaim, so the two overlap for the whole run.
    let scheduled = format!(
        "{meta}, META_MAINTENANCE_INTERVAL INTERVAL '50 milliseconds', \
         META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN INTERVAL '0 seconds'"
    );

    // Each round resolves a fresh snapshot (autocommit gives every
    // statement its own transaction), reads the metadata catalog the way
    // DuckLake's own resolution does, and commits one more snapshot for
    // the next pass to expire.
    let mut race = "INSERT INTO lake.main.t VALUES (1);\n\
                    SELECT count(*) FROM lake.main.t;\n\
                    SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot \
                      WHERE snapshot_id = (SELECT max(snapshot_id) \
                                             FROM __ducklake_metadata_lake.ducklake_snapshot);\n"
        .repeat(ROUNDS);
    race.push_str(
        "SELECT 'PASS' AS marker, status FROM moraine_maintenance_status('lake') \
           WHERE step = 'expire_snapshots';\n",
    );

    let output = run_ducklake_sql_with_pause(
        store.path(),
        data.path(),
        &scheduled,
        // Nothing but the attach; the pause lets the first ticks land so
        // the race is already running when the reads start.
        "SELECT 1;\n",
        std::time::Duration::from_millis(200),
        &race,
    );

    // The reads are only evidence if expiry was actually running under
    // them: a pass that never ran the step would make this vacuous.
    let ran = csv_rows(&output)
        .into_iter()
        .filter(|row| row.first().is_some_and(|marker| marker == "PASS"))
        .filter(|row| row[1] == "ran")
        .count();
    assert!(
        ran > 0,
        "no expiry pass ran under the reads, so they raced nothing: {output}"
    );

    // And the lake is whole: every round's row landed, and every surviving
    // snapshot still resolves.
    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "SELECT count(*) FROM lake.main.t;",
    ));
    assert_eq!(rows, vec![vec![(60 + ROUNDS).to_string()]]);
}

/// Expiring a dropped table takes its file column statistics with it,
/// exactly as a stock DuckLake catalog does — and the sweep reclaims
/// whatever an older catalog stranded before that held.
///
/// Differential because the failure was invisible without a reference:
/// moraine kept the statistics while stock deleted them, agreeing on every
/// other count, and the delete DuckLake issues is dropped silently. The
/// snapshot deletions are what made it reachable — they take the data file
/// out of both sides of the commit diff, which is the one arm that can
/// retire a statistics row.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn expiring_a_dropped_table_reclaims_its_file_column_stats_like_stock() {
    let dir = TempDir::new("sweep-stats-store");
    let data_dir = TempDir::new("sweep-stats-data");
    let reference_meta = TempDir::new("sweep-stats-ref-meta");
    let reference_data = TempDir::new("sweep-stats-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(dir.path(), data_dir.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    // Each read runs its own session: the sweep reclaims without stamping
    // the head, so a session that already materialized a table keeps
    // serving what it built.
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(dir.path(), data_dir.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };
    let counts = "SELECT (SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file), \
                         (SELECT count(*) FROM __ducklake_metadata_lake.ducklake_file_column_stats);";

    apply(
        "CREATE TABLE lake.main.t (a BIGINT, b BIGINT);\
         INSERT INTO lake.main.t SELECT range, range FROM range(0,32);\
         CALL ducklake_flush_inlined_data('lake');",
    );
    let seeded = probe(counts);
    assert_eq!(seeded[0][0], "1", "one flushed file");
    assert_ne!(seeded[0][1], "0", "which carries statistics");

    apply(
        "DROP TABLE lake.main.t;\
         CALL ducklake_expire_snapshots('lake', older_than => now());\
         CALL ducklake_cleanup_old_files('lake', cleanup_all => true);",
    );
    assert_eq!(
        probe(counts),
        vec![vec!["0".to_string(), "0".to_string()]],
        "expiry must take the statistics with the file, as stock does"
    );

    // And the sweep is a no-op once nothing was stranded.
    let swept = csv_rows(&run_ducklake_sql(
        dir.path(),
        data_dir.path(),
        "SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_file_stats';",
    ));
    assert_eq!(swept, vec![vec!["reclaimed 0 file column statistics"]]);
}

/// `moraine_raise_format` takes the newest additive format deliberately,
/// ahead of the commit that would otherwise take it. It reports the move
/// and is idempotent, and `dry_run` reads a store's format without
/// moving it. The store here writes no shape needing the newest format —
/// its inserts inline, but nothing deregisters a duplicate schema — so
/// it sits below one until the verb runs.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn raising_the_store_format_is_explicit_and_idempotent() {
    let dir = TempDir::new("raise-format-store");
    let data_dir = TempDir::new("raise-format-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        // Inlining is off: a locator carries the newest format, and this
        // test needs a store that sits below it.
        "SET ducklake_default_data_inlining_row_limit = 0;\n\
         CREATE TABLE lake.main.t (i BIGINT);\n\
         INSERT INTO lake.main.t VALUES (1);",
    );

    let raise_with = |label: &str, options: &str| -> (u64, u64) {
        let rows = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            &format!("SELECT from_format, to_format FROM moraine_raise_format('lake'{options});"),
        ));
        let row = rows.first().unwrap_or_else(|| panic!("{label}: a row"));
        let read = |cell: &String| cell.parse::<u64>().expect("a format is a number");
        (read(&row[0]), read(&row[1]))
    };

    // A dry run answers what a raise would do, twice over, without doing
    // it — the pre-flight a one-way door needs.
    let probed = raise_with("the dry run", ", dry_run := true");
    assert!(
        probed.0 < probed.1,
        "a fresh store must sit below this binary's newest additive format, got {probed:?}"
    );
    assert_eq!(
        raise_with("the second dry run", ", dry_run := true"),
        probed,
        "a dry run must leave the store where it found it"
    );

    let (from_format, to_format) = raise_with("the first raise", "");
    assert_eq!((from_format, to_format), probed);
    assert!(
        from_format < to_format,
        "a store writing no shape that needs the newest format must sit below it, \
         got {from_format} -> {to_format}"
    );

    let (again_from, again_to) = raise_with("the second raise", "");
    assert_eq!(
        (again_from, again_to),
        (to_format, to_format),
        "raising an already-raised store must be a no-op"
    );

    // The lake still reads, and reads the same rows.
    let rows = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT i FROM lake.main.t ORDER BY i;",
    ));
    assert_eq!(rows, vec![vec!["1"]]);
}
