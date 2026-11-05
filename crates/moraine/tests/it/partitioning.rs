//! Partition specs through the verb surface: setting, replacing, clearing,
//! reading one back — at head and by time travel — and registering a data
//! file into the partition it falls in.

use moraine::{DataFile, Error, PartitionColumnDef, TableId};

use crate::fixtures::{col, datafile, seeded};

fn key(column: moraine::ColumnId, transform: &str) -> PartitionColumnDef {
    PartitionColumnDef {
        column,
        transform: transform.into(),
    }
}

#[tokio::test]
async fn a_spec_reads_back_with_its_columns_in_declared_order() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(async |tx| {
            tx.add_column(table, &col("y")).await?;
            Ok(())
        })
        .await
        .unwrap();

    let columns = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap();
    let (x, y) = (columns[0].id, columns[1].id);
    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(y, "bucket(16)"), key(x, "identity")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();

    let spec = catalog
        .read_scope()
        .await
        .unwrap()
        .partitioning_of(table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        spec.columns,
        vec![key(y, "bucket(16)"), key(x, "identity")],
        "keys keep their declared order and verbatim transforms"
    );
}

#[tokio::test]
async fn an_unpartitioned_table_has_no_spec() {
    let (catalog, _, table, _) = seeded().await;
    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .partitioning_of(table)
            .await
            .unwrap()
            .is_none()
    );
}

/// Repartitioning ends the old spec into history and mints a new one, so a
/// snapshot before the change still reconstructs the spec in force then —
/// the property files written under an older spec depend on.
#[tokio::test]
async fn repartitioning_leaves_the_old_spec_readable_by_time_travel() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;

    let first = catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    let second = catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "year")]).await?;
            Ok(())
        })
        .await
        .unwrap();

    let at_first = catalog.read_scope_at(first).await.unwrap();
    let spec_then = at_first.partitioning_of(table).await.unwrap().unwrap();
    assert_eq!(spec_then.columns, vec![key(x, "identity")]);

    let at_second = catalog.read_scope_at(second).await.unwrap();
    let spec_now = at_second.partitioning_of(table).await.unwrap().unwrap();
    assert_eq!(spec_now.columns, vec![key(x, "year")]);
    assert_ne!(spec_then.id, spec_now.id, "a new spec gets a fresh id");
}

#[tokio::test]
async fn clearing_unpartitions_the_table_but_keeps_the_past() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;

    let partitioned = catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async move |tx| tx.clear_partitioning(table).await)
        .await
        .unwrap();

    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .partitioning_of(table)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .read_scope_at(partitioned)
            .await
            .unwrap()
            .partitioning_of(table)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn clearing_an_unpartitioned_table_is_not_found() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(async move |tx| tx.clear_partitioning(table).await)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
}

#[tokio::test]
async fn a_spec_is_refused_when_it_is_empty_repeats_a_column_or_names_a_stranger() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;

    let empty = catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[]).await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(empty, Error::Constraint(_)), "{empty}");

    let repeated = catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity"), key(x, "year")])
                .await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(repeated, Error::Constraint(_)), "{repeated}");

    let stranger = catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(moraine::ColumnId::new(999), "identity")])
                .await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(stranger, Error::NotFound(_)), "{stranger}");

    let absent_table = catalog
        .commit(async |tx| {
            tx.set_partitioning(TableId::new(9999), &[key(x, "identity")])
                .await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(absent_table, Error::NotFound(_)), "{absent_table}");
}

/// Dropping the table takes its spec with it — the per-table cascade the
/// snapshot applies must not leave a spec addressable by a reused view.
#[tokio::test]
async fn dropping_the_table_drops_its_spec() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;
    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async move |tx| tx.drop_table(table).await)
        .await
        .unwrap();

    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .partitioning_of(table)
            .await
            .unwrap()
            .is_none()
    );
}

/// Partitioning is table DDL: it bumps the catalog schema version, so a
/// client keying a schema cache on it re-reads.
#[tokio::test]
async fn setting_a_spec_bumps_the_schema_version() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;
    let before = catalog
        .read_scope()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version;

    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")]).await?;
            Ok(())
        })
        .await
        .unwrap();

    let after = catalog
        .read_scope()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version;
    assert_eq!(after, before + 1);
}

/// A file registered into a partitioned table records the values it falls
/// under and the spec they belong to, and reads back in key order —
/// through the accessor and through the `ducklake_file_partition_value`
/// projection the extension serves.
#[tokio::test]
async fn a_registered_file_carries_the_partition_it_falls_in() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(async |tx| {
            tx.add_column(table, &col("y")).await?;
            Ok(())
        })
        .await
        .unwrap();
    let columns = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap();
    let (x, y) = (columns[0].id, columns[1].id);

    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity"), key(y, "year")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    let spec_id = catalog
        .read_scope()
        .await
        .unwrap()
        .partitioning_of(table)
        .await
        .unwrap()
        .unwrap()
        .id;

    catalog
        .commit(async move |tx| {
            tx.register_data_file(
                table,
                DataFile {
                    partition_values: vec!["eu-west".into(), "2026".into()],
                    ..datafile(3)
                },
                &[],
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();

    let files = catalog
        .read_scope()
        .await
        .unwrap()
        .data_files_of(table)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].partition_id, Some(spec_id));
    assert_eq!(files[0].partition_values, vec!["eu-west", "2026"]);

    let rows = moraine::ffi_support::dump_file_partition_value_rows(&catalog)
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.partition_key_index, row.partition_value.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "eu-west"), (1, "2026")]
    );
}

/// The values must match the live spec's keys one for one: too few or too
/// many would leave the file in a partition no reader can reconstruct.
#[tokio::test]
async fn a_file_must_carry_one_value_per_partition_key() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;
    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")]).await?;
            Ok(())
        })
        .await
        .unwrap();

    for values in [vec![], vec!["a".to_string(), "b".to_string()]] {
        let err = catalog
            .commit(async move |tx| {
                tx.register_data_file(
                    table,
                    DataFile {
                        partition_values: values.clone(),
                        ..datafile(1)
                    },
                    &[],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }
}

#[tokio::test]
async fn an_unpartitioned_table_refuses_a_file_carrying_values() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(async move |tx| {
            tx.register_data_file(
                table,
                DataFile {
                    partition_values: vec!["eu-west".into()],
                    ..datafile(1)
                },
                &[],
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

/// Repartitioning does not disturb files already registered: each keeps
/// the spec it was written under and the values it was written with, which
/// is what lets files under different specs coexist.
#[tokio::test]
async fn a_file_keeps_its_own_spec_across_a_repartition() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;
    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    let first_spec = catalog
        .read_scope()
        .await
        .unwrap()
        .partitioning_of(table)
        .await
        .unwrap()
        .unwrap()
        .id;
    catalog
        .commit(async move |tx| {
            tx.register_data_file(
                table,
                DataFile {
                    partition_values: vec!["old".into()],
                    ..datafile(1)
                },
                &[],
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(async move |tx| {
            tx.set_partitioning(table, &[key(x, "year")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async move |tx| {
            tx.register_data_file(
                table,
                DataFile {
                    partition_values: vec!["new".into()],
                    ..datafile(2)
                },
                &[],
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    let second_spec = head.partitioning_of(table).await.unwrap().unwrap().id;
    let files = head.data_files_of(table).await.unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(
        files
            .iter()
            .map(|file| (file.partition_id, file.partition_values.clone()))
            .collect::<Vec<_>>(),
        vec![
            (Some(first_spec), vec!["old".to_string()]),
            (Some(second_spec), vec!["new".to_string()]),
        ]
    );
}
