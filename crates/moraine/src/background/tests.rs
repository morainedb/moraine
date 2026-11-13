use super::*;

fn request(table: u64, file: u64) -> BackgroundRequest {
    BackgroundRequest::commit(
        vec![(TableId::new(table), crate::DataFileId::new(file))],
        SnapshotId::new(file),
    )
    .unwrap()
}

#[tokio::test]
async fn coalescing_warming_preserves_every_publication() {
    let work = BackgroundWork::default();
    work.push(request(1, 1));
    work.push(request(1, 2));
    work.push(request(2, 3));
    let Job::Warm(warm) = work.warming.next().await else {
        panic!("warming job")
    };
    assert_eq!(warm.tables, Some([1, 2].map(TableId::new).into()));
    assert!(work.warming.jobs.lock().unwrap().is_empty());
    for file in 1..=3 {
        let Job::Publish(job) = work.commits.next().await else {
            panic!("publication job")
        };
        assert_eq!(job.files[0].1.get(), file);
    }
}

#[tokio::test]
async fn started_warming_cannot_absorb_new_work() {
    let work = BackgroundWork::default();
    work.push(request(1, 1));
    {
        let jobs = work.warming.jobs.lock().unwrap();
        let Job::Warm(job) = jobs.front().unwrap() else {
            panic!("warming job")
        };
        *job.progress.try_lock().unwrap() = Some(Warming::new(job.tables.as_ref()));
    }
    work.push(request(2, 2));
    assert_eq!(work.warming.jobs.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn attach_warming_dominates_only_unstarted_warming() {
    let work = BackgroundWork::default();
    work.push(request(1, 1));
    work.push(BackgroundRequest::attach(true));
    let Job::Warm(job) = work.warming.next().await else {
        panic!("warming job")
    };
    assert!(job.tables.is_none());
    assert!(matches!(work.backfill.next().await, Job::Backfill(_)));
    assert!(matches!(work.commits.next().await, Job::Publish(_)));
    let no_preload = BackgroundWork::default();
    no_preload.push(BackgroundRequest::attach(false));
    assert!(no_preload.warming.jobs.lock().unwrap().is_empty());
    assert!(matches!(no_preload.backfill.next().await, Job::Backfill(_)));
}

#[tokio::test(start_paused = true)]
async fn retries_rotate_without_losing_progress() {
    let work = Arc::new(BackgroundWork::default());
    work.push(request(1, 1));
    let queue = work.clone();
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = attempts.clone();
    let task = tokio::spawn(async move {
        queue
            .commits
            .run_with(
                &|job| {
                    let attempt = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    send.send(job).unwrap();
                    async move {
                        match attempt {
                            0 => false,
                            1 => std::future::pending().await,
                            _ => true,
                        }
                    }
                },
                &Semaphore::new(1),
                Policy {
                    deadline: Duration::from_secs(5),
                    retry_initial: Duration::from_secs(1),
                    retry_maximum: Duration::from_secs(2),
                },
            )
            .await;
    });
    let Job::Publish(first) = receive.recv().await.unwrap() else {
        panic!("publication")
    };
    work.push(request(2, 2));
    let Job::Publish(second) = receive.recv().await.unwrap() else {
        panic!("publication")
    };
    assert_eq!(second.files[0].0, TableId::new(2));
    let Job::Publish(retry) = receive.recv().await.unwrap() else {
        panic!("publication")
    };
    assert!(Arc::ptr_eq(&first.progress, &retry.progress));
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn stalled_warming_and_backfill_do_not_block_commits_and_abort_releases_work() {
    let work = Arc::new(BackgroundWork::default());
    work.push(BackgroundRequest::attach(true));
    work.push(request(1, 1));
    let queue = work.clone();
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        queue
            .run_with(|job| {
                let send = send.clone();
                async move {
                    send.send(job.kind()).unwrap();
                    std::future::pending::<bool>().await
                }
            })
            .await;
    });
    let mut kinds = BTreeSet::new();
    for _ in 0..3 {
        kinds.insert(
            tokio::time::timeout(Duration::from_secs(1), receive.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(kinds, ["warming", "publication", "backfill"].into());
    task.abort();
    let _ = task.await;
    assert_eq!(Arc::strong_count(&work), 1);
}

#[tokio::test]
async fn bounded_queue_refuses_overflow_and_wakes_for_new_work() {
    let work = BackgroundWork::default();
    for file in 1..=MAX_PENDING_JOBS as u64 {
        work.push(request(1, file));
    }
    work.push(request(1, 999));
    assert_eq!(work.commits.jobs.lock().unwrap().len(), MAX_PENDING_JOBS);
    for file in 1..=MAX_PENDING_JOBS as u64 {
        let Job::Publish(job) = work.commits.next().await else {
            panic!("publication")
        };
        assert_eq!(job.files[0].1.get(), file);
    }
    let next = tokio::time::timeout(Duration::from_secs(1), work.commits.next());
    let schedule = async {
        tokio::task::yield_now().await;
        work.push(request(2, 2));
    };
    let (job, ()) = tokio::join!(next, schedule);
    assert!(matches!(job.unwrap(), Job::Publish(_)));
}

#[tokio::test]
async fn attach_jobs_read_a_real_catalog_without_an_extension() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let catalog = crate::Catalog::open(objects.clone(), crate::CatalogOptions::default())
        .await
        .unwrap();
    let work = BackgroundWork::default();
    work.push(BackgroundRequest::attach(true));
    assert!(work.warming.next().await.run(&catalog, None, "").await);
    assert!(
        work.backfill
            .next()
            .await
            .run(&catalog, Some(&DataStore::new(objects)), "")
            .await
    );
    drop(work);
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_producers_preserve_every_file_batch() {
    let work = Arc::new(BackgroundWork::default());
    let mut producers = Vec::new();
    for table in 1..=64 {
        let work = work.clone();
        producers.push(tokio::spawn(async move {
            work.push(request(table, table));
        }));
    }
    for producer in producers {
        producer.await.unwrap();
    }
    let mut tables = BTreeSet::new();
    for _ in 1..=64 {
        let Job::Publish(job) = work.commits.next().await else {
            panic!("publication")
        };
        tables.insert(job.files[0].0);
    }
    assert_eq!(tables, (1..=64).map(TableId::new).collect());
}

#[tokio::test]
async fn admission_slots_are_released_on_cancellation() {
    let slots = Arc::new(Semaphore::new(1));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for table in [1, 2] {
        let slots = slots.clone();
        let send = send.clone();
        tasks.push(tokio::spawn(async move {
            let work = BackgroundWork::default();
            work.push(request(table, table));
            work.commits
                .run_with(
                    &|_| {
                        send.send(table).unwrap();
                        std::future::pending::<bool>()
                    },
                    &slots,
                    Policy::default(),
                )
                .await;
        }));
    }
    assert_eq!(receive.recv().await, Some(1));
    tokio::task::yield_now().await;
    assert!(receive.try_recv().is_err());
    tasks[0].abort();
    assert_eq!(receive.recv().await, Some(2));
    tasks[1].abort();
    for task in tasks {
        let _ = task.await;
    }
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
#[ignore = "reads MORAINE_MEMORY_FIXTURE with a small working-memory allowance"]
async fn preload_streams_a_persisted_catalog() {
    let root = std::env::var("MORAINE_MEMORY_FIXTURE").unwrap();
    let objects = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
    let catalog = crate::Catalog::open_read_only(
        objects,
        crate::CatalogOptions {
            cache_memory: Some(16 * 1024 * 1024),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let work = BackgroundWork::default();
    work.push(BackgroundRequest::attach(true));
    assert!(work.warming.next().await.run(&catalog, None, "").await);
    drop(work);
    catalog.close().await.unwrap();
}
