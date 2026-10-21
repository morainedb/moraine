use std::time::Duration;

use moraine::{SnapshotId, TableId};

use super::{MAX_PENDING_TABLES, Pass, Pending};

fn commit(table: u64, snapshot: u64) -> Pass {
    Pass::commit(vec![TableId::new(table)], SnapshotId::new(snapshot))
}

#[tokio::test]
async fn bursts_keep_every_table_and_the_earliest_snapshot() {
    let pending = Pending::default();
    for (table, snapshot) in [(1, 30), (2, 10), (1, 20)] {
        pending.push(commit(table, snapshot));
    }
    let pass = pending.next().await;
    assert_eq!(
        pass.tables.unwrap().into_iter().collect::<Vec<_>>(),
        vec![TableId::new(1), TableId::new(2)]
    );
    assert_eq!(pass.since, Some(SnapshotId::new(9)));
}

#[tokio::test]
async fn attach_and_commit_requests_stay_separate_in_either_order() {
    for (attach_first, preload) in [(false, false), (false, true), (true, false), (true, true)] {
        let schedule = super::Schedule::default();
        let mut passes = [Pass::attach(preload), commit(1, 20)];
        if !attach_first {
            passes.reverse();
        }
        for pass in passes {
            schedule.push(pass);
        }
        let backfill = schedule.backfill.next().await;
        assert_eq!(backfill.tables, (!preload).then(Default::default));
        assert!(backfill.since.is_none());
        let commit = schedule.commits.next().await;
        assert_eq!(commit.tables.unwrap(), [TableId::new(1)].into());
        assert_eq!(commit.since, Some(SnapshotId::new(19)));
    }
}

#[tokio::test]
async fn overflow_warms_all_tables_without_retaining_the_table_ids() {
    let pending = Pending::default();
    for table in 0..=MAX_PENDING_TABLES as u64 {
        pending.push(commit(table, table + 1));
    }
    let pass = pending.next().await;
    assert!(pass.tables.is_none());
    assert_eq!(pass.since, Some(SnapshotId::new(0)));
}

#[tokio::test]
async fn requests_during_a_pass_are_delivered_and_idle_workers_wake() {
    let pending = Pending::default();
    pending.push(commit(1, 1));
    let _running = pending.next().await;
    pending.push(commit(2, 2));
    assert_eq!(pending.next().await.since, Some(SnapshotId::new(1)));

    let next = tokio::time::timeout(Duration::from_secs(1), pending.next());
    let schedule = async {
        tokio::task::yield_now().await;
        pending.push(commit(3, 3));
    };
    let (pass, ()) = tokio::join!(next, schedule);
    assert_eq!(pass.unwrap().since, Some(SnapshotId::new(2)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_producers_do_not_lose_tables() {
    let pending = std::sync::Arc::new(Pending::default());
    let mut tasks = Vec::new();
    for table in 1..=64 {
        let pending = std::sync::Arc::clone(&pending);
        tasks.push(tokio::spawn(async move {
            pending.push(commit(table, table));
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let pass = pending.next().await;
    assert_eq!(pass.tables.unwrap(), (1..=64).map(TableId::new).collect());
    assert_eq!(pass.since, Some(SnapshotId::new(0)));
}

#[test]
fn commits_progress_during_backfill_and_shutdown_drops_both_passes() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Running(Arc<AtomicUsize>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let running = Arc::new(AtomicUsize::new(0));
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let backfill_permits = Arc::new(tokio::sync::Semaphore::new(0));
    let (finished, mut completions) = tokio::sync::mpsc::unbounded_channel();
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let pending = Arc::new(super::Schedule::default());
    pending.push(Pass::attach(false));
    let queue = Arc::clone(&pending);
    let active = Arc::clone(&running);
    let available = Arc::clone(&permits);
    let backfill_available = Arc::clone(&backfill_permits);
    let worker = super::Worker {
        pending,
        task: runtime.spawn(async move {
            queue
                .run(|pass| {
                    let active = Arc::clone(&active);
                    let available = Arc::clone(&available);
                    let backfill_available = Arc::clone(&backfill_available);
                    let started = started.clone();
                    let finished = finished.clone();
                    async move {
                        active.fetch_add(1, Ordering::SeqCst);
                        let _running = Running(active);
                        let backfill = pass.since.is_none();
                        started.send(pass).unwrap();
                        let gate = if backfill {
                            backfill_available
                        } else {
                            available
                        };
                        gate.acquire().await.unwrap().forget();
                        finished.send(backfill).unwrap();
                    }
                })
                .await;
        }),
    };
    runtime.block_on(async {
        assert!(starts.recv().await.unwrap().since.is_none());
        worker.push(commit(1, 20));
        let first = tokio::time::timeout(Duration::from_secs(1), starts.recv())
            .await
            .expect("commit must start while backfill is blocked")
            .unwrap();
        assert_eq!(first.since, Some(SnapshotId::new(19)));
        assert_eq!(running.load(Ordering::SeqCst), 2);
        backfill_permits.add_permits(1);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), completions.recv())
                .await
                .expect("backfill must progress while a commit is blocked")
                .unwrap()
        );
        assert_eq!(running.load(Ordering::SeqCst), 1);
        worker.push(Pass::attach(false));
        assert!(starts.recv().await.unwrap().since.is_none());

        worker.push(commit(2, 40));
        worker.push(commit(3, 30));
        tokio::task::yield_now().await;
        assert!(starts.try_recv().is_err(), "only one commit batch runs");
        permits.add_permits(1);
        let merged = tokio::time::timeout(Duration::from_secs(1), starts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(merged.since, Some(SnapshotId::new(29)));
        assert_eq!(merged.tables.unwrap(), [2, 3].map(TableId::new).into());
        assert_eq!(running.load(Ordering::SeqCst), 2);
    });
    worker.stop(&runtime);
    assert_eq!(running.load(Ordering::SeqCst), 0);
}
