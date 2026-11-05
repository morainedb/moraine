use std::sync::Arc;

use object_store::memory::InMemory;

use super::*;
use crate::{
    Catalog, CatalogOptions,
    store::key::{EntityKey, Key, NameKey, NameKind},
};

#[tokio::test]
async fn exact_name_lookup_does_not_decode_unrelated_tables() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            tx.create_table(
                schema,
                "chosen",
                &[crate::ColumnDef {
                    name: "id".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let write = catalog.begin_write_tx().unwrap();
    write
        .put(
            Key::current(EntityKey::Table { table_id: 999 }).encode(),
            b"invalid unrelated row",
        )
        .unwrap();
    catalog.commit_write_tx(write).await.unwrap();
    let selected = index_table(&catalog, "main", "chosen", false)
        .await
        .unwrap();
    assert_eq!(selected.tables.len(), 1);
    assert_eq!(
        selected.tables.values().next().unwrap().table_name,
        "chosen"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn name_entries_follow_create_rename_and_drop_atomically() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            tx.create_table(
                schema,
                "before",
                &[crate::ColumnDef {
                    name: "id".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let selected = index_table(&catalog, "main", "before", false)
        .await
        .unwrap();
    let row = selected.tables.values().next().unwrap();
    let key = |name: &str| {
        Key::Name(NameKey {
            kind: NameKind::Table,
            parent: row.schema_id,
            name: name.into(),
            entity_id: row.table_id,
        })
        .encode()
    };
    let read = catalog.begin_read().await.unwrap();
    assert!(read.handle().get(key("before")).await.unwrap().is_some());
    read.finish();
    catalog
        .commit(|tx| tx.rename_table(crate::TableId::new(row.table_id), "after"))
        .await
        .unwrap();
    let read = catalog.begin_read().await.unwrap();
    assert!(read.handle().get(key("before")).await.unwrap().is_none());
    assert!(read.handle().get(key("after")).await.unwrap().is_some());
    read.finish();
    assert!(
        index_table(&catalog, "main", "before", false)
            .await
            .is_err()
    );
    assert!(index_table(&catalog, "main", "after", false).await.is_ok());
    catalog
        .commit(|tx| tx.drop_table(crate::TableId::new(row.table_id)))
        .await
        .unwrap();
    let read = catalog.begin_read().await.unwrap();
    assert!(read.handle().get(key("after")).await.unwrap().is_none());
    read.finish();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn name_maintenance_preserves_exact_spelling_and_duplicate_order() {
    use crate::store::{proto, value};
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let writes = [(3, "same"), (7, "same"), (8, "Same")]
        .into_iter()
        .map(|(table_id, name)| {
            (
                Key::current(EntityKey::Table { table_id }).encode(),
                Some(value::encode_value(&proto::TableValue {
                    table_id,
                    schema_id: 0,
                    table_name: name.into(),
                    ..Default::default()
                })),
            )
        })
        .collect();
    catalog.commit_own_rows_durably(writes).await.unwrap();
    for (name, id) in [("same", 7), ("Same", 8)] {
        assert!(
            index_table(&catalog, "main", name, false)
                .await
                .unwrap()
                .tables
                .contains_key(&id)
        );
    }
    catalog
        .commit_own_rows_durably(vec![(
            Key::current(EntityKey::Table { table_id: 7 }).encode(),
            None,
        )])
        .await
        .unwrap();
    assert!(
        index_table(&catalog, "main", "same", false)
            .await
            .unwrap()
            .tables
            .contains_key(&3)
    );
    assert!(index_table(&catalog, "main", "SAME", false).await.is_err());
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn positioning_sources_do_not_decode_other_files_or_mappings() {
    use crate::store::{proto, value};
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            tx.create_table(
                schema,
                "chosen",
                &[crate::ColumnDef {
                    name: "id".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let definition = index_table(&catalog, "main", "chosen", false)
        .await
        .unwrap();
    let table_id = *definition.tables.keys().next().unwrap();
    let column_id = definition.columns[&table_id]
        .keys()
        .next()
        .copied()
        .unwrap();
    let file = proto::DataFileValue {
        table_id,
        data_file_id: 3,
        mapping_id: Some(1),
        ..Default::default()
    };
    let write = catalog.begin_write_tx().unwrap();
    for (key, bytes) in [
        (
            Key::current(EntityKey::File {
                table_id,
                data_file_id: 3,
            }),
            value::encode_value(&file),
        ),
        (
            Key::current(EntityKey::File {
                table_id,
                data_file_id: 999,
            }),
            b"corrupt unrelated file".to_vec(),
        ),
        (
            Key::current(EntityKey::Mapping {
                table_id,
                mapping_id: 1,
            }),
            value::encode_value(&proto::MappingValue {
                table_id,
                mapping_id: 1,
                map_type: "map_by_name".into(),
                name_mappings: vec![proto::NameMapping {
                    target_field_id: column_id,
                    source_name: "physical".into(),
                    ..Default::default()
                }],
            }),
        ),
        (
            Key::current(EntityKey::Mapping {
                table_id,
                mapping_id: 999,
            }),
            b"corrupt unrelated mapping".to_vec(),
        ),
    ] {
        write.put(key.encode(), bytes).unwrap();
    }
    catalog.commit_write_tx(write).await.unwrap();
    let selected = table_sources_for(&catalog, "main", "chosen", &[crate::DataFileId::new(3)])
        .await
        .unwrap();
    assert_eq!(selected.data_files[&table_id].len(), 1);
    assert!(selected.mappings.is_empty());
    let read = catalog.begin_read().await.unwrap();
    let columns = selected
        .file_read_columns_at(read.handle(), crate::TableId::new(table_id), &file)
        .await
        .unwrap();
    assert_eq!(columns[0].source_name.as_deref(), Some("physical"));
    read.finish();
    catalog.close().await.unwrap();
}
