use std::sync::Arc;

use super::Admission;

#[tokio::test]
async fn background_io_leaves_room_for_foreground_and_cancel_releases_permits() {
    let admission = Arc::new(Admission::new(2));
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for request in 0..2 {
        let admission = Arc::clone(&admission);
        let started = started.clone();
        tasks.push(tokio::spawn(async move {
            admission
                .run(true, async {
                    started.send(request).unwrap();
                    std::future::pending::<object_store::Result<()>>().await
                })
                .await
        }));
    }
    assert_eq!(starts.recv().await, Some(0));
    tokio::task::yield_now().await;
    assert!(starts.try_recv().is_err());
    assert_eq!(admission.run(false, async { Ok(42) }).await.unwrap(), 42);
    tasks[0].abort();
    assert_eq!(starts.recv().await, Some(1));
    tasks[1].abort();
    for task in tasks {
        let _ = task.await;
    }
    assert_eq!(admission.all.available_permits(), 2);
    assert_eq!(admission.background.available_permits(), 1);
}
