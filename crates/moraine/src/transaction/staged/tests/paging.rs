use super::*;

#[tokio::test]
async fn paged_staged_files_match_the_overlay_and_freeze_authored_rows() {
    let catalog = open().await;
    let write = catalog.begin_write_tx().unwrap();
    let mut setup = StagedTransaction::begin_detached(&catalog, write);
    setup.stage(insert_row(TableKind::Table, table_row(1, 0, "t", 1, None)));
    setup.stage(insert_row(TableKind::Column, column_row(1, 1, "a", 1)));
    for id in 10..140 {
        setup.stage(insert_row(TableKind::DataFile, data_file_row(id, 1, 1)));
    }
    setup.stage(insert_row(TableKind::Snapshot, snapshot_row(1, 1, 2)));
    setup.stage(insert_row(
        TableKind::SnapshotChanges,
        snapshot_changes_row(1, "inserted_into_table:1"),
    ));
    setup.commit().await.unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    tx.stage(insert_row(TableKind::DataFile, data_file_row(200, 1, 2)));
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(2)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(2)],
    });
    let expected = tx.visible_data_files().await.unwrap();
    let mut cursor = tx
        .metadata_cursor(TableKind::DataFile, Some(1), None)
        .await
        .unwrap();
    tx.stage(insert_row(TableKind::DataFile, data_file_row(201, 1, 2)));
    let mut actual = Vec::new();
    loop {
        let page = cursor.data_files().await.unwrap();
        assert!(page.records.len() <= 64);
        if page.records.is_empty() {
            break;
        }
        actual.extend(page.records);
    }
    assert_eq!(actual, expected);
    let mut replay = cursor.fork().await.unwrap();
    assert_eq!(replay.data_files().await.unwrap().records, actual[..64]);
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn paged_stats_replace_and_delete_without_retaining_the_directory() {
    let catalog = open().await;
    let write = catalog.begin_write_tx().unwrap();
    for data_file_id in 0..130 {
        write
            .put(
                Key::current(EntityKey::FileColumnStats {
                    table_id: 1,
                    data_file_id,
                    column_id: 1,
                })
                .encode(),
                value::encode_value(&proto::FileColumnStatsValue {
                    table_id: 1,
                    data_file_id,
                    column_id: 1,
                    min_value: Some("old".into()),
                    ..Default::default()
                }),
            )
            .unwrap();
    }
    catalog.commit_write_tx(write).await.unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    tx.stage(insert_row(
        TableKind::FileColumnStats,
        file_column_stats_row(0, 1, 1, "new", "new"),
    ));
    tx.stage(RowOperation::Delete {
        table: TableKind::FileColumnStats,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(1)],
    });
    tx.stage(insert_row(
        TableKind::FileColumnStats,
        file_column_stats_row(200, 2, 1, "other", "other"),
    ));
    let expected = tx.visible_file_column_stats_of(1).await.unwrap();
    let mut cursor = tx
        .metadata_cursor(TableKind::FileColumnStats, Some(1), None)
        .await
        .unwrap();
    tx.stage(insert_row(
        TableKind::FileColumnStats,
        file_column_stats_row(0, 1, 1, "later", "later"),
    ));
    let mut actual = Vec::new();
    loop {
        let page = cursor.file_column_stats().await.unwrap();
        assert!(page.records.len() <= 64);
        if page.records.is_empty() {
            break;
        }
        actual.extend(page.records);
    }
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 129);
    let mut replay = cursor.fork().await.unwrap();
    assert_eq!(
        replay.file_column_stats().await.unwrap().records,
        actual[..64]
    );
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn paged_table_inserts_inherit_only_surviving_counters() {
    let catalog = open().await;
    let write = catalog.begin_write_tx().unwrap();
    for (end_snapshot, next_column_id) in [(None, 137), (Some(1), 42)] {
        let entity = EntityKey::Table { table_id: 1 };
        let key =
            end_snapshot.map_or_else(|| Key::current(entity), |end| Key::history(entity, end));
        write
            .put(
                key.encode(),
                value::encode_value(&proto::TableValue {
                    table_id: 1,
                    table_name: "before".into(),
                    next_column_id,
                    end_snapshot,
                    ..Default::default()
                }),
            )
            .unwrap();
    }
    catalog.commit_write_tx(write).await.unwrap();
    for delete_current in [false, true] {
        let scope = catalog.index_read_scope().await.unwrap().unwrap();
        let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
            .with_read_scope(scope.reads().clone());
        tx.stage(RowOperation::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        });
        if delete_current {
            tx.stage(RowOperation::Delete {
                table: TableKind::Table,
                cells: vec![Cell::U64(1), Cell::U64(2)],
            });
        }
        tx.stage(insert_row(
            TableKind::Table,
            table_row(1, 0, "after", 2, None),
        ));
        let expected = tx.visible_tables().await.unwrap();
        let mut cursor = tx
            .metadata_cursor(TableKind::Table, None, None)
            .await
            .unwrap();
        assert_eq!(cursor.tables().await.unwrap().records, expected);
        tx.rollback();
    }
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn container_pages_merge_existing_and_new_scopes_once() {
    let catalog = open().await;
    let write = catalog.begin_write_tx().unwrap();
    for id in 0..130 {
        write
            .put(
                Key::current(EntityKey::Tag { object_id: id }).encode(),
                value::encode_value(&proto::TagValue {
                    object_id: id,
                    entries: vec![proto::TagEntry {
                        key: "comment".into(),
                        value: "old".into(),
                        begin_snapshot: 1,
                        end_snapshot: None,
                    }],
                }),
            )
            .unwrap();
        write
            .put(
                Key::current(EntityKey::Option {
                    scope_kind: 2,
                    scope_id: id,
                })
                .encode(),
                value::encode_value(&proto::OptionScopeValue {
                    options: std::collections::HashMap::from([("k".into(), "old".into())]),
                }),
            )
            .unwrap();
    }
    catalog.commit_write_tx(write).await.unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    for id in [0, 200] {
        tx.stage(insert_row(TableKind::Tag, tag_row(id, 2, "comment", "new")));
        tx.stage(insert_row(
            TableKind::Metadata,
            vec![
                Cell::Str("k".into()),
                Cell::Str("new".into()),
                Cell::Str("table".into()),
                Cell::U64(id),
            ],
        ));
    }
    tx.stage(RowOperation::Delete {
        table: TableKind::Tag,
        cells: vec![Cell::U64(1), Cell::Str("comment".into()), Cell::U64(1)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::Metadata,
        cells: vec![
            Cell::Str("k".into()),
            Cell::Str("table".into()),
            Cell::U64(1),
        ],
    });
    let expected_tags = crate::ffi_support::staged::visible_tag_rows(&tx)
        .await
        .unwrap();
    let expected_options = crate::ffi_support::staged::visible_option_rows(&tx)
        .await
        .unwrap();
    let mut tags = tx
        .metadata_cursor(TableKind::Tag, None, None)
        .await
        .unwrap();
    let mut options = tx
        .metadata_cursor(TableKind::Metadata, None, None)
        .await
        .unwrap();
    let mut actual_tags = Vec::new();
    let mut actual_options = Vec::new();
    loop {
        let page = tags.tags().await.unwrap();
        if page.records.is_empty() {
            break;
        }
        assert!(page.records.len() <= 64);
        actual_tags.extend(page.records);
    }
    loop {
        let page = options.options().await.unwrap();
        if page.records.is_empty() {
            break;
        }
        assert!(page.records.len() <= 64);
        actual_options.extend(page.records);
    }
    assert_eq!(actual_tags, expected_tags);
    assert_eq!(actual_options, expected_options);
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_pages_skip_deleted_runs_without_ending_the_scan() {
    let catalog = open().await;
    let write = catalog.begin_write_tx().unwrap();
    for snapshot_id in 1..140 {
        write
            .put(
                Key::Snapshot { snapshot_id }.encode(),
                value::encode_value(&proto::SnapshotValue {
                    snapshot_id,
                    ..Default::default()
                }),
            )
            .unwrap();
    }
    catalog.commit_write_tx(write).await.unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    for id in 0..130 {
        tx.stage(RowOperation::Delete {
            table: TableKind::Snapshot,
            cells: vec![Cell::U64(id)],
        });
    }
    let mut cursor = tx
        .metadata_cursor(TableKind::SnapshotChanges, None, None)
        .await
        .unwrap();
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(139)],
    });
    assert_eq!(
        cursor
            .snapshots()
            .await
            .unwrap()
            .records
            .iter()
            .map(|row| row.snapshot_id)
            .collect::<Vec<_>>(),
        (130..140).collect::<Vec<_>>()
    );
    assert!(cursor.snapshots().await.unwrap().records.is_empty());
    assert_eq!(
        cursor
            .fork()
            .await
            .unwrap()
            .snapshots()
            .await
            .unwrap()
            .records
            .len(),
        10
    );
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn child_pages_merge_parent_edits_and_authored_rows() {
    let catalog = open().await;
    seed_table(&catalog).await;
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    for id in 10..140 {
        tx.stage(insert_row(
            TableKind::PartitionInfo,
            partition_info_row(id, 1, 2),
        ));
        tx.stage(insert_row(
            TableKind::PartitionColumn,
            partition_column_row(id, 1, 0, 1),
        ));
        tx.stage(insert_row(
            TableKind::FilePartitionValue,
            file_partition_value_row(id, 1, 0, "one"),
        ));
    }
    tx.stage(RowOperation::Delete {
        table: TableKind::PartitionInfo,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::Null],
    });
    let expected = crate::ffi_support::staged::visible_partition_column_rows(&tx)
        .await
        .unwrap();
    let expected_files = crate::ffi_support::staged::visible_file_partition_value_rows(&tx)
        .await
        .unwrap();
    let mut cursor = tx
        .metadata_cursor(TableKind::PartitionColumn, None, None)
        .await
        .unwrap();
    let mut actual = Vec::new();
    loop {
        let page = cursor.partition_columns().await.unwrap();
        if page.records.is_empty() {
            break;
        }
        assert!(page.records.len() <= 64);
        actual.extend(page.records);
    }
    assert_eq!(actual, expected);
    let mut cursor = tx
        .metadata_cursor(TableKind::FilePartitionValue, None, None)
        .await
        .unwrap();
    let mut actual = Vec::new();
    loop {
        let page = cursor.file_partition_values().await.unwrap();
        if page.records.is_empty() {
            break;
        }
        actual.extend(page.records);
    }
    assert_eq!(actual, expected_files);
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn schema_version_pages_apply_deletes_inserts_and_file_floors() {
    let catalog = open().await;
    let write = catalog.begin_write_tx().unwrap();
    for table_id in 0..130 {
        write
            .put(
                Key::SchemaVersion {
                    table_id,
                    begin_snapshot: 5,
                }
                .encode(),
                value::encode_value(&proto::SchemaVersionValue { schema_version: 10 }),
            )
            .unwrap();
        write
            .put(
                Key::current(EntityKey::File {
                    table_id,
                    data_file_id: table_id,
                })
                .encode(),
                value::encode_value(&proto::DataFileValue {
                    table_id,
                    data_file_id: table_id,
                    begin_snapshot: 2,
                    ..Default::default()
                }),
            )
            .unwrap();
    }
    write
        .put(
            Key::Snapshot { snapshot_id: 4 }.encode(),
            value::encode_value(&proto::SnapshotValue {
                snapshot_id: 4,
                schema_version: 8,
                schema_changed_table_ids: vec![0, 1],
                ..Default::default()
            }),
        )
        .unwrap();
    catalog.commit_write_tx(write).await.unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    tx.stage(RowOperation::Delete {
        table: TableKind::SchemaVersions,
        cells: vec![Cell::U64(5), Cell::U64(10), Cell::U64(0)],
    });
    tx.stage(insert_row(
        TableKind::SchemaVersions,
        vec![Cell::U64(6), Cell::U64(11), Cell::U64(0)],
    ));
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(4)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::Null],
    });
    tx.stage(insert_row(TableKind::DataFile, data_file_row(200, 200, 1)));
    let expected = crate::ffi_support::staged::visible_schema_version_rows(&tx)
        .await
        .unwrap();
    let mut cursor = tx
        .metadata_cursor(TableKind::SchemaVersions, None, None)
        .await
        .unwrap();
    let mut actual = Vec::new();
    loop {
        let page = cursor.schema_versions().await.unwrap();
        if page.records.is_empty() {
            break;
        }
        assert!(page.records.len() <= 64);
        actual.extend(page.records);
    }
    actual.sort_by_key(|row| (row.table_id, row.begin_snapshot));
    assert_eq!(actual, expected);
    tx.rollback();
    catalog.close().await.unwrap();
}
