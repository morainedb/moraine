use proptest::prelude::*;

use super::*;

fn nested_count<K: Ord, V>(rows: &OrdMap<u64, OrdMap<K, V>>) -> usize {
    rows.values().map(OrdMap::len).sum()
}

fn scanned_count(view: &CatalogSnapshot) -> usize {
    view.schemas.len()
        + view.tables.len()
        + view.views.len()
        + view.macros.len()
        + view.table_stats.len()
        + view.options.len()
        + view.tags.len()
        + view.gc_files.len()
        + nested_count(&view.columns)
        + nested_count(&view.data_files)
        + nested_count(&view.delete_files)
        + nested_count(&view.partitions)
        + nested_count(&view.sorts)
        + nested_count(&view.mappings)
        + nested_count(&view.indexes)
        + nested_count(&view.table_column_stats)
        + nested_count(&view.file_column_stats)
}

fn record(kind: u8, table_id: u64, id: u64) -> EntityRecord {
    match kind {
        0 => EntityRecord::Column(ColumnValue {
            table_id,
            column_id: id,
            ..Default::default()
        }),
        1 => EntityRecord::File(DataFileValue {
            table_id,
            data_file_id: id,
            ..Default::default()
        }),
        2 => EntityRecord::DeleteFile(DeleteFileValue {
            table_id,
            delete_file_id: id,
            ..Default::default()
        }),
        3 => EntityRecord::Partition(PartitionValue {
            table_id,
            partition_id: id,
            ..Default::default()
        }),
        4 => EntityRecord::Sort(SortValue {
            table_id,
            sort_id: id,
            ..Default::default()
        }),
        5 => EntityRecord::Mapping(MappingValue {
            table_id,
            mapping_id: id,
            ..Default::default()
        }),
        6 => EntityRecord::Index(IndexValue {
            table_id,
            index_id: id,
            ..Default::default()
        }),
        7 => EntityRecord::TableColumnStats(TableColumnStatsValue {
            table_id,
            column_id: id,
            ..Default::default()
        }),
        _ => EntityRecord::FileColumnStats(FileColumnStatsValue {
            table_id,
            data_file_id: id,
            column_id: id,
            ..Default::default()
        }),
    }
}

fn delete(view: &mut CatalogSnapshot, kind: u8, table: u64, id: u64) {
    match kind {
        0 => view.remove_column_only(table, id),
        1 => view.delete_data_file(table, id),
        2 => view.delete_delete_file(table, id),
        3 => view.delete_partition(table, id),
        4 => view.delete_sort(table, id),
        5 => view.remove_table_only(table),
        6 => view.delete_index(table, id),
        7 => view.remove_table_column_stats(table, id),
        _ => view.remove_file_column_stats(table, id, id),
    }
}

proptest! {
    #[test]
    fn counts_follow_replacements_deletions_and_cascades(
        edits in prop::collection::vec((0u8..9, 0u64..4, 0u64..8, 0u8..5), 1..160)
    ) {
        let mut view = CatalogSnapshot::default();
        for table in 0..4 {
            view.put_table(TableValue { table_id: table, ..Default::default() });
            view.put_table_stats(TableStatsValue { table_id: table, ..Default::default() });
            for kind in 0..9 {
                view.put_record(record(kind, table, table * 8));
            }
        }
        for (kind, table, local_id, operation) in edits {
            let held = view.clone();
            let held_count = held.live_entity_count();
            let id = table * 8 + local_id;
            match operation {
                0 | 1 => view.put_record(record(kind, table, id)),
                2 => delete(&mut view, kind, table, id),
                3 => view.delete_table(table),
                _ => view.delete_column(table, id),
            }
            prop_assert_eq!(view.live_entity_count(), scanned_count(&view));
            prop_assert_eq!(held.live_entity_count(), held_count);
            prop_assert_eq!(held.live_entity_count(), scanned_count(&held));
        }
    }
}

#[test]
fn table_cascades_keep_historical_mapping_and_file_statistics_counts() {
    let records: Vec<_> = (0..9).map(|kind| record(kind, 1, 8)).collect();
    let mut view = CatalogSnapshot::build(snap(1), &records, &[], None);
    assert_eq!(view.live_entity_count(), 9);
    for row in records {
        view.put_record(row);
    }
    assert_eq!(view.live_entity_count(), 9);
    view.delete_table(1);
    assert_eq!(view.live_entity_count(), 2);
    view.delete_table(1);
    assert_eq!(view.live_entity_count(), 2);
    view.remove_file_column_stats(1, 8, 8);
    assert_eq!(view.live_entity_count(), 1);
}
