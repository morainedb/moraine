use super::*;

fn benchmark_setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Wall time of indexed inline commits whose bodies are encoded up front.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "timing benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn indexed_inline_commit_benchmark() {
    let commits = benchmark_setting("MORAINE_INLINE_COMMITS", 24);
    let chunks = benchmark_setting("MORAINE_INLINE_CHUNKS", 8);
    let rows = benchmark_setting("MORAINE_INLINE_ROWS", 32_768);
    let (catalog, index) = catalog_with_indexed_inline_table(false).await;
    let head = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();
    let mut next_row = 0u64;
    let mut elapsed = Vec::with_capacity(usize::try_from(commits).unwrap());

    for commit in 0..commits {
        let snapshot_id = head + 1 + commit;
        let mut schema_ipc = None;
        let bodies: Vec<_> = (0..chunks)
            .map(|_| {
                let values: Vec<i64> = (next_row..next_row + rows)
                    .map(|row| i64::try_from(row).unwrap())
                    .collect();
                let (schema, batch) = bigint_batch(&values);
                schema_ipc.get_or_insert_with(|| inline_schema_ipc(&schema));
                let row_id_start = next_row;
                next_row += rows;
                (row_id_start, inline_body(&batch))
            })
            .collect();

        let started = std::time::Instant::now();
        let mut tx =
            StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().await.unwrap());
        if commit == 0 {
            tx.stage(RowOperation::InlineSchema {
                table_id: 1,
                schema_version: 0,
                arrow_schema: schema_ipc.unwrap(),
            });
        }
        for (row_id_start, body) in bodies {
            tx.stage(RowOperation::InlineInsert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: snapshot_id,
                row_id_start,
                row_count: rows,
                arrow_body: body.into(),
            });
        }
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(snapshot_id, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(snapshot_id, "inlined_insert:1"),
        });
        tx.commit().await.unwrap();
        elapsed.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    assert_eq!(
        index_entry_count(&catalog, false, index).await,
        usize::try_from(commits * chunks * rows).unwrap()
    );
    let mut sorted = elapsed.clone();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    let mean = elapsed.iter().sum::<f64>() / f64::from(u32::try_from(elapsed.len()).unwrap());
    println!("BENCH,commits,chunks,rows_per_chunk,median_ms,mean_ms,min_ms,max_ms");
    println!(
        "BENCH,{commits},{chunks},{rows},{median:.3},{mean:.3},{:.3},{:.3}",
        sorted[0],
        sorted[sorted.len() - 1]
    );
    catalog.close().await.unwrap();
}
