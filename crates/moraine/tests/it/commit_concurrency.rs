//! Concurrent-commit behavior through the public API: benign races
//! retry internally; true conflicts surface typed.

use moraine::Error;

use crate::fixtures::{col, datafile, seeded};

#[tokio::test(flavor = "multi_thread")]
async fn disjoint_table_ddl_both_succeed() {
    let (catalog, _s, a, b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(async move |tx| tx.add_column(a, &col("a1")).await.map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(async move |tx| tx.add_column(b, &col("b1")).await.map(|_| ()))
            .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(head.columns_of(a).await.unwrap().len(), 2);
    assert_eq!(head.columns_of(b).await.unwrap().len(), 2);
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_table_ddl_races_serialize_or_conflict() {
    let (catalog, _s, a, _b) = seeded().await;
    // Whether a round serializes or conflicts depends on scheduling; the
    // classifier itself is pinned by the `transaction::operations` unit tests. This
    // test only requires that races never corrupt state.
    let mut added = Vec::new();
    for round in 0..20 {
        let c1 = catalog.clone();
        let c2 = catalog.clone();
        let n1 = format!("c1_{round}");
        let n2 = format!("c2_{round}");
        let t1 = tokio::spawn({
            let n1 = n1.clone();
            async move {
                c1.commit(async move |tx| tx.add_column(a, &col(&n1)).await.map(|_| ()))
                    .await
            }
        });
        let t2 = tokio::spawn({
            let n2 = n2.clone();
            async move {
                c2.commit(async move |tx| tx.add_column(a, &col(&n2)).await.map(|_| ()))
                    .await
            }
        });
        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();
        for (r, name) in [(r1, n1), (r2, n2)] {
            match r {
                Ok(_) => added.push(name),
                Err(Error::CommitConflict(_)) => {}
                Err(other) => panic!("unexpected error: {other}"),
            }
        }
    }
    let head = catalog.read_scope().await.unwrap();
    let names: std::collections::HashSet<String> = head
        .columns_of(a)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.name)
        .collect();
    for name in &added {
        assert!(
            names.contains(name),
            "successfully committed column {name} missing from table a"
        );
    }
    assert_eq!(
        names.len(),
        added.len() + 1,
        "every successful add present exactly once, plus the seeded column"
    );
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_name_create_race_yields_one_table() {
    let (catalog, s, _a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(async move |tx| tx.create_table(s, "orders", &[col("x")]).await.map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(async move |tx| tx.create_table(s, "orders", &[col("x")]).await.map(|_| ()))
            .await
    });
    let results = [t1.await.unwrap(), t2.await.unwrap()];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(oks, 1);
    assert!(
        results
            .iter()
            .any(|r| matches!(r, Err(Error::AlreadyExists(_) | Error::CommitConflict(_))))
    );
    // Exactly one live table named "orders" either way.
    let head = catalog.read_scope().await.unwrap();
    let orders: Vec<_> = head
        .tables_in(s)
        .await
        .unwrap()
        .into_iter()
        .filter(|t| t.name == "orders")
        .collect();
    assert_eq!(orders.len(), 1);
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn counters_never_regress_or_collide_under_concurrency() {
    let (catalog, s, _a, _b) = seeded().await;
    let mut handles = Vec::new();
    for i in 0..8 {
        let c = catalog.clone();
        let name = format!("t{i}");
        handles.push(tokio::spawn(async move {
            c.commit(async move |tx| tx.create_table(s, &name, &[col("x")]).await.map(|_| ()))
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    let head = catalog.read_scope().await.unwrap();
    let mut ids: Vec<u64> = head
        .tables_in(s)
        .await
        .unwrap()
        .iter()
        .map(|t| t.id.get())
        .collect();
    ids.sort_unstable();
    let before = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), before, "table ids must be unique");
    assert_eq!(before, 10, "2 seeded + 8 concurrent");
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_table_appends_both_land_with_dense_row_ids() {
    let (catalog, _s, a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(async move |tx| {
            tx.register_data_file(a, datafile(100), &[])
                .await
                .map(|_| ())
        })
        .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(async move |tx| {
            tx.register_data_file(a, datafile(50), &[])
                .await
                .map(|_| ())
        })
        .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let head = catalog.read_scope().await.unwrap();
    let files = head.data_files_of(a).await.unwrap();
    assert_eq!(files.len(), 2);
    let mut starts: Vec<(u64, u64)> = files
        .iter()
        .map(|f| {
            (
                f.row_id_start.expect("verb-registered files carry a start"),
                f.record_count,
            )
        })
        .collect();
    starts.sort_unstable();

    // Dense, disjoint ranges regardless of which commit won the race.
    assert_eq!(starts[0].0, 0);
    assert_eq!(starts[1].0, starts[0].1);
    assert_eq!(head.table_stats(a).await.unwrap().unwrap().next_row_id, 150);
    catalog.close().await.unwrap();
}

/// Two concurrent deletes from one table, targeting different data files,
/// both land through the public API.
///
/// This is the end-to-end shape, not the classifier: concurrent commits
/// coalesce into one batch far more often than they race, so what this
/// pins is that nothing on the way — the one-live-delete-file-per-data-file
/// rule above all — turns two disjoint deletes into a failure. The grain
/// the classifier applies when they *do* race is pinned directly, over the
/// whole matrix, in `transaction::operations`.
#[tokio::test(flavor = "multi_thread")]
async fn same_table_deletes_of_different_files_both_land() {
    use moraine::DeleteFile;

    let (catalog, _s, a, _b) = seeded().await;
    // Two data files to delete from, registered up front.
    catalog
        .commit(async move |tx| {
            tx.register_data_file(a, datafile(100), &[]).await?;
            tx.register_data_file(a, datafile(50), &[]).await?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    let mut ids: Vec<_> = head
        .data_files_of(a)
        .await
        .unwrap()
        .iter()
        .map(|f| f.id)
        .collect();
    ids.sort_unstable();
    let (first, second) = (ids[0], ids[1]);

    let delete_over = |data_file_id| DeleteFile {
        data_file_id,
        path: format!("d{data_file_id}.parquet"),
        path_is_relative: true,
        format: "parquet".into(),
        delete_count: 1,
        file_size_bytes: 10,
        footer_size: 4,
        encryption_key: None,
    };

    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(async move |tx| {
            tx.register_delete_file(a, delete_over(first), &[])
                .await
                .map(|_| ())
        })
        .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(async move |tx| {
            tx.register_delete_file(a, delete_over(second), &[])
                .await
                .map(|_| ())
        })
        .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.delete_files_of(a).await.unwrap().len(),
        2,
        "deletes of different files must both land, not conflict"
    );
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn append_vs_drop_is_a_real_error() {
    let (catalog, s, a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(async move |tx| {
            tx.register_data_file(a, datafile(10), &[])
                .await
                .map(|_| ())
        })
        .await
    });
    let t2 = tokio::spawn(async move { c2.commit(async move |tx| tx.drop_table(a).await).await });
    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();

    // Serialized is fine; a genuine race surfaces CommitConflict or
    // NotFound on the loser.
    for r in [&r1, &r2] {
        match r {
            Ok(_) | Err(Error::CommitConflict(_) | Error::NotFound(_)) => {}
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    // Whatever the outcome mix, the head must be consistent with it.
    let head = catalog.read_scope().await.unwrap();
    let live = head
        .tables_in(s)
        .await
        .unwrap()
        .into_iter()
        .any(|t| t.id == a);
    if r2.is_ok() {
        assert!(!live, "drop committed, so the table must be gone");
    } else {
        assert!(live, "drop failed, so the table must survive");
        if r1.is_ok() {
            assert_eq!(head.data_files_of(a).await.unwrap().len(), 1);
        }
    }
    catalog.close().await.unwrap();
}

/// An options-only commit re-validates its scope against a racing drop:
/// whichever order the two land in, a dropped table never keeps a live
/// option record (which nothing could ever remove again).
#[tokio::test(flavor = "multi_thread")]
async fn option_set_vs_drop_leaves_no_orphaned_option() {
    use moraine::OptionScope;

    for _ in 0..10 {
        let (catalog, s, a, _b) = seeded().await;
        let c1 = catalog.clone();
        let c2 = catalog.clone();
        let t1 = tokio::spawn(async move {
            c1.commit(async move |tx| tx.set_option(OptionScope::Table(a), "k", "v").await)
                .await
        });
        let t2 =
            tokio::spawn(async move { c2.commit(async move |tx| tx.drop_table(a).await).await });
        let set = t1.await.unwrap();
        let dropped = t2.await.unwrap();

        for r in [&set, &dropped] {
            match r {
                Ok(_) | Err(Error::CommitConflict(_) | Error::NotFound(_)) => {}
                Err(other) => panic!("unexpected error: {other}"),
            }
        }

        let head = catalog.read_scope().await.unwrap();
        let live = head
            .tables_in(s)
            .await
            .unwrap()
            .into_iter()
            .any(|t| t.id == a);
        if !live {
            assert_eq!(
                head.option(OptionScope::Table(a), "k").await.unwrap(),
                None,
                "dropped table kept an orphaned option record"
            );
        }
        catalog.close().await.unwrap();
    }
}
