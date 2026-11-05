use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, OptionScope};
use object_store::local::LocalFileSystem;

use crate::helpers::*;

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn options_stream_with_fixed_facts_under_a_small_memory_budget() {
    let directory = TempDir::new("paged-options");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let catalog = Catalog::open(
            Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let payload = "x".repeat(4096);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").unwrap().id;
                tx.set_option(OptionScope::Global, "created_by", "stored-shadow")?;
                for id in 0..512 {
                    let table = tx.create_table(
                        schema,
                        &format!("t_{id}"),
                        &[ColumnDef {
                            name: "i".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: true,
                            default_value: None,
                            children: Vec::new(),
                        }],
                    )?;
                    for option in 0..8 {
                        tx.set_option(
                            OptionScope::Table(table),
                            &format!("measure_{option}"),
                            &payload,
                        )?;
                    }
                    if id == 0 {
                        tx.set_option(OptionScope::Table(table), "version", "table-version")?;
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    });
    let output = run_session_with_env(
        &Attach::Standalone {
            store_dir: directory.path(),
            read_only: true,
        },
        "SELECT count(*) FROM m.ducklake_metadata WHERE key LIKE 'measure_%'; SELECT value FROM m.ducklake_metadata WHERE key = 'created_by' AND scope IS NULL; SELECT value FROM m.ducklake_metadata WHERE key = 'version' AND scope = 'table';",
        &[("MORAINE_WORKING_MEMORY_BYTES", "16777216")],
    );
    assert!(output.status.success(), "{}", combined_output(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("4096"), "{stdout}");
    assert!(stdout.contains("moraine"), "{stdout}");
    assert!(stdout.contains("table-version"), "{stdout}");
    assert!(!stdout.contains("stored-shadow"), "{stdout}");
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn staged_statistics_scan_stays_bounded_after_a_delete() {
    let directory = TempDir::new("paged-staged-statistics");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let first_file = std::cell::Cell::new(None);
    runtime.block_on(async {
        let catalog = Catalog::open(
            Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let payload = "x".repeat(4096);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").unwrap().id;
                let table = tx.create_table(
                    schema,
                    "files",
                    &[ColumnDef {
                        name: "i".into(),
                        column_type: "VARCHAR".into(),
                        nulls_allowed: true,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )?;
                let column = tx.columns_of(table)[0].id;
                for id in 0..2048 {
                    let file = tx.register_data_file(
                        table,
                        moraine::DataFile {
                            path: format!("file_{id}.parquet"),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 1,
                            file_size_bytes: 1024,
                            footer_size: 128,
                            encryption_key: None,
                            partition_values: Vec::new(),
                            column_stats: vec![moraine::FileColumnStats {
                                column_id: column,
                                column_size_bytes: 1024,
                                value_count: Some(1),
                                null_count: Some(0),
                                min_value: Some(payload.clone()),
                                max_value: Some(payload.clone()),
                                contains_nan: None,
                                extra_stats: None,
                            }],
                        },
                        &[],
                    )?;
                    if id == 0 {
                        first_file.set(Some(file.get()));
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    });
    let output = run_session_with_env(
        &Attach::Standalone {
            store_dir: directory.path(),
            read_only: false,
        },
        &format!(
            "BEGIN; DELETE FROM m.ducklake_file_column_stats WHERE data_file_id = {}; SELECT count(*) FROM m.ducklake_file_column_stats; ROLLBACK;",
            first_file.get().unwrap()
        ),
        &[("MORAINE_WORKING_MEMORY_BYTES", "16777216")],
    );
    assert!(output.status.success(), "{}", combined_output(&output));
    assert!(String::from_utf8(output.stdout).unwrap().contains("2047"));
}
