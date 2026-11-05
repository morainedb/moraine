use std::sync::Arc;

use object_store::memory::InMemory;
use slatedb::{
    IsolationLevel,
    config::{FlushOptions, FlushType},
};

use super::*;
use crate::{
    DataFileId,
    store::{
        key::SysKey,
        open::StoreBuilder,
        proto::{DeleteFileValue, FormatValue},
        value::encode_value,
    },
};

#[tokio::test]
async fn legacy_selected_files_share_one_delete_scan() {
    let (database, counters) = StoreBuilder::new("selected-files", Arc::new(InMemory::new()))
        .cache_puts(false)
        .open_writer()
        .await
        .unwrap();
    database
        .put(
            Key::Sys(SysKey::Format).encode(),
            encode_value(&FormatValue {
                format_version: 11,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    for id in 0..512 {
        database
            .put(
                Key::current(EntityKey::DeleteFile {
                    table_id: 1,
                    delete_file_id: id,
                })
                .encode(),
                encode_value(&DeleteFileValue {
                    table_id: 1,
                    delete_file_id: id,
                    data_file_id: id % 16,
                    path: "x".repeat(2048),
                    end_snapshot: (id == 0).then_some(2),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }
    database
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    let transaction = database.begin(IsolationLevel::Snapshot).unwrap();
    let mut one_file_reads = 0;
    for file_count in [1, 1, 8] {
        let before = counters.tally();
        let mut view = CatalogSnapshot::default();
        let mut files: Vec<_> = (0..file_count).rev().map(DataFileId::new).collect();
        files.push(DataFileId::new(0));
        load_sources_for(
            ReadHandle::Tx(&transaction),
            &mut view,
            TableId::new(1),
            &files,
        )
        .await
        .unwrap();
        let actual: Vec<_> = view.delete_files[&1].keys().copied().collect();
        let expected: Vec<_> = (1..512).filter(|id| id % 16 < file_count).collect();
        assert_eq!(actual, expected);
        let reads = counters.tally().since(before);
        let blocks = reads.block_hits + reads.block_misses;
        if file_count == 1 {
            one_file_reads = blocks;
        } else {
            assert!(one_file_reads > 0);
            assert!(
                blocks <= 2 * one_file_reads + 16,
                "one file: {one_file_reads}, eight files: {blocks}"
            );
        }
    }
    transaction.rollback();
    database.close().await.unwrap();
}
