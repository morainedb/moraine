use super::*;

async fn seed_tag_versions(catalog: &Catalog) {
    let write = catalog.begin_write_tx().unwrap();
    for id in 0..130 {
        for (end_snapshot, label) in [(None, "current"), (Some(2), "recent"), (Some(1), "old")] {
            for (entity, encoded) in [
                (
                    EntityKey::Column {
                        table_id: 1,
                        column_id: id,
                    },
                    value::encode_value(&proto::ColumnValue {
                        table_id: 1,
                        column_id: id,
                        end_snapshot,
                        tags: vec![proto::ColumnTag {
                            key: "comment".into(),
                            value: label.into(),
                            begin_snapshot: 1,
                            end_snapshot: None,
                        }],
                        ..Default::default()
                    }),
                ),
                (
                    EntityKey::View { view_id: id },
                    value::encode_value(&proto::ViewValue {
                        view_id: id,
                        end_snapshot,
                        column_tags: vec![proto::ViewColumnTag {
                            column_name: "a".into(),
                            key: "comment".into(),
                            value: label.into(),
                            begin_snapshot: 1,
                            end_snapshot: None,
                        }],
                        ..Default::default()
                    }),
                ),
            ] {
                let key = end_snapshot
                    .map_or_else(|| Key::current(entity), |end| Key::history(entity, end));
                write.put(key.encode(), encoded).unwrap();
            }
        }
    }
    catalog.commit_write_tx(write).await.unwrap();
}

fn edit_parents(tx: &mut StagedTransaction) {
    for end in [Cell::Null, Cell::U64(2)] {
        tx.stage(RowOperation::Delete {
            table: TableKind::Column,
            cells: vec![Cell::U64(1), Cell::U64(0), end.clone()],
        });
        tx.stage(RowOperation::Delete {
            table: TableKind::View,
            cells: vec![Cell::U64(0), end],
        });
    }
    tx.stage(insert_row(
        TableKind::ColumnTag,
        column_tag_row(1, 0, 3, "unit", "cm"),
    ));
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::ViewColumnTag,
        cells: vec![
            Cell::U64(0),
            Cell::Str("a".into()),
            Cell::Str("comment".into()),
            Cell::U64(3),
        ],
    });
    for end in [Cell::Null, Cell::U64(1), Cell::U64(2)] {
        tx.stage(RowOperation::Delete {
            table: TableKind::Column,
            cells: vec![Cell::U64(1), Cell::U64(1), end.clone()],
        });
        tx.stage(RowOperation::Delete {
            table: TableKind::View,
            cells: vec![Cell::U64(1), end],
        });
    }
    tx.stage(insert_row(
        TableKind::Column,
        column_row(1, 2, "replacement", 3),
    ));
    tx.stage(insert_row(
        TableKind::ColumnTag,
        column_tag_row(1, 2, 3, "comment", "authored"),
    ));
    for begin in [3, 4] {
        tx.stage(insert_row(
            TableKind::ViewColumnTag,
            vec![
                Cell::U64(200),
                Cell::Str("a".into()),
                Cell::U64(begin),
                Cell::Null,
                Cell::Str("comment".into()),
                Cell::Str("orphan".into()),
            ],
        ));
    }
    tx.stage(RowOperation::Delete {
        table: TableKind::ViewColumnTag,
        cells: vec![
            Cell::U64(200),
            Cell::Str("a".into()),
            Cell::Str("comment".into()),
            Cell::U64(3),
        ],
    });
}

#[tokio::test]
async fn parent_tag_pages_select_surviving_versions_and_freeze_edits() {
    let catalog = open().await;
    seed_tag_versions(&catalog).await;
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap())
        .with_read_scope(scope.reads().clone());
    edit_parents(&mut tx);
    assert_eq!(tx.visible_view_column_tags().await.unwrap().len(), 130);
    let mut columns = tx
        .metadata_cursor(TableKind::ColumnTag, None, None)
        .await
        .unwrap();
    let mut views = tx
        .metadata_cursor(TableKind::ViewColumnTag, None, None)
        .await
        .unwrap();
    tx.stage(insert_row(
        TableKind::ColumnTag,
        column_tag_row(1, 0, 4, "later", "hidden"),
    ));
    let mut actual_columns = Vec::new();
    let mut actual_views = Vec::new();
    loop {
        let page = columns.column_tags().await.unwrap();
        assert!(page.records.len() <= 64);
        if page.records.is_empty() {
            break;
        }
        actual_columns.extend(page.records);
    }
    loop {
        let page = views.view_column_tags().await.unwrap();
        assert!(page.records.len() <= 64);
        if page.records.is_empty() {
            break;
        }
        actual_views.extend(page.records);
    }
    assert_eq!(actual_columns.len(), 130);
    assert_eq!(actual_views.len(), 130);
    assert!(!actual_columns.iter().any(|row| row.column_id == 1));
    assert!(!actual_views.iter().any(|row| row.view_id == 1));
    assert_eq!(
        actual_columns
            .iter()
            .find(|row| row.column_id == 2)
            .unwrap()
            .value,
        "authored"
    );
    assert_eq!(
        actual_views
            .iter()
            .find(|row| row.view_id == 200)
            .unwrap()
            .begin_snapshot,
        4
    );
    let selected: Vec<_> = actual_columns
        .iter()
        .filter(|row| row.column_id == 0)
        .collect();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].value, "old");
    assert_eq!(selected[1].value, "cm");
    let selected = actual_views.iter().find(|row| row.view_id == 0).unwrap();
    assert_eq!(selected.value, "old");
    assert_eq!(selected.end_snapshot, Some(3));
    assert!(
        actual_views
            .iter()
            .filter(|row| row.view_id != 0 && row.view_id != 200)
            .all(|row| row.value == "current")
    );
    assert_eq!(
        columns
            .fork()
            .await
            .unwrap()
            .column_tags()
            .await
            .unwrap()
            .records,
        actual_columns[..64]
    );
    assert_eq!(
        views
            .fork()
            .await
            .unwrap()
            .view_column_tags()
            .await
            .unwrap()
            .records,
        actual_views[..64]
    );
    tx.rollback();
    catalog.close().await.unwrap();
}
