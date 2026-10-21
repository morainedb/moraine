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
async fn attach_backfill_and_preload_cover_merged_commits_in_either_order() {
    for attach_first in [false, true] {
        let pending = Pending::default();
        let mut passes = [Pass::attach(true), commit(1, 20)];
        if !attach_first {
            passes.reverse();
        }
        for pass in passes {
            pending.push(pass);
        }
        let pass = pending.next().await;
        assert!(pass.tables.is_none());
        assert!(pass.since.is_none());
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
fn shutdown_waits_for_the_running_future_to_drop() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Running(Arc<AtomicBool>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let running = Arc::new(AtomicBool::new(true));
    let held = Running(Arc::clone(&running));
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker = super::Worker {
        pending: Arc::new(Pending::default()),
        task: runtime.spawn(async move {
            let _held = held;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        }),
    };
    runtime.block_on(ready).unwrap();
    worker.stop(&runtime);
    assert!(!running.load(Ordering::SeqCst));
}
