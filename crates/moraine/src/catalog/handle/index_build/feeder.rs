//! Feeds a staged build's planned units into its running window.

use std::{collections::VecDeque, future::Future};

use futures::{StreamExt, stream::FuturesOrdered};
use tokio::sync::mpsc;

use crate::error::Result;

/// Launches plans while fewer than `budget` are outstanding, and sends
/// their units into `window` in plan order as it admits them. A plan is
/// outstanding from its launch until its last unit is sent. Every
/// in-flight plan is polled on every wait, full window or not: one left
/// unpolled would hold whatever it had acquired against the rest of the
/// process.
pub(super) async fn feed_window<L, P, T, E, U, S, R>(
    mut launch: L,
    mut expand: E,
    mut spawn: S,
    window: mpsc::Sender<R>,
    budget: usize,
) -> Result<()>
where
    L: FnMut() -> Option<P>,
    P: Future<Output = Result<T>>,
    E: FnMut(T) -> Vec<U>,
    S: FnMut(U) -> R,
{
    let mut plans = FuturesOrdered::new();
    let mut planned: VecDeque<(U, bool)> = VecDeque::new();
    let mut outstanding = 0usize;
    let mut exhausted = false;

    loop {
        // The budget bounds launches, never polls; one plan always runs.
        while !exhausted && (outstanding < budget || outstanding == 0) {
            match launch() {
                Some(plan) => {
                    plans.push_back(plan);
                    outstanding = outstanding.saturating_add(1);
                }
                None => exhausted = true,
            }
        }
        if plans.is_empty() && planned.is_empty() {
            return Ok(());
        }

        tokio::select! {
            biased;
            slot = window.reserve(), if !planned.is_empty() => {
                // A closed window means the consumer is gone.
                let Ok(slot) = slot else { return Ok(()) };
                let Some((unit, last)) = planned.pop_front() else { continue };
                slot.send(spawn(unit));
                if last {
                    outstanding = outstanding.saturating_sub(1);
                }
            }
            plan = plans.next(), if !plans.is_empty() => {
                let Some(plan) = plan else { continue };
                let units = expand(plan?);
                let count = units.len();
                if count == 0 {
                    outstanding = outstanding.saturating_sub(1);
                }
                planned.extend(
                    units
                        .into_iter()
                        .enumerate()
                        .map(|(index, unit)| (unit, index.saturating_add(1) == count)),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::sync::{mpsc, watch};

    use super::feed_window;
    use crate::error::Error;

    async fn settle(condition: impl Fn() -> bool) {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Plans launched behind a full window still complete while the feeder
    /// waits for a slot.
    #[tokio::test]
    async fn a_full_window_still_drives_every_in_flight_plan() {
        let (release, released) = watch::channel(false);
        let completed = Arc::new(AtomicUsize::new(0));
        let spawned = Arc::new(AtomicUsize::new(0));
        let (window, receiver) = mpsc::channel::<usize>(1);

        let feed = {
            let completed = Arc::clone(&completed);
            let spawned = Arc::clone(&spawned);
            let mut launched = 0usize;
            feed_window(
                move || {
                    let index = launched;
                    launched += 1;
                    (index < 4).then(|| {
                        let mut released = released.clone();
                        let completed = Arc::clone(&completed);
                        async move {
                            // The first plan is immediate and fills the window;
                            // the rest complete only once released.
                            if index > 0 {
                                released.wait_for(|released| *released).await.map_err(|_| {
                                    Error::Interrupted("release dropped".to_owned())
                                })?;
                                completed.fetch_add(1, Ordering::SeqCst);
                            }
                            Ok::<_, Error>(index)
                        }
                    })
                },
                |index| {
                    if index == 0 {
                        vec![0, 1]
                    } else {
                        vec![index * 10]
                    }
                },
                move |unit| {
                    spawned.fetch_add(1, Ordering::SeqCst);
                    unit
                },
                window,
                4,
            )
        };
        let feed = tokio::spawn(feed);

        // One unit fills the window; the feeder now waits for a slot.
        settle(|| spawned.load(Ordering::SeqCst) >= 1).await;
        release.send(true).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            settle(|| completed.load(Ordering::SeqCst) == 3),
        )
        .await
        .expect("plans behind a full window must still complete");

        drop(receiver);
        feed.await.unwrap().unwrap();
    }

    /// Units reach the window in plan order, and a plan launches only while
    /// fewer than the budget are outstanding.
    #[tokio::test]
    async fn units_arrive_in_plan_order_under_the_launch_budget() {
        let (window, mut receiver) = mpsc::channel::<usize>(1);
        let sent = Arc::new(AtomicUsize::new(0));

        let feed = {
            let sent_at_launch = Arc::clone(&sent);
            let sent_at_spawn = Arc::clone(&sent);
            let mut launched = 0usize;
            feed_window(
                move || {
                    // Two units per plan, so the plans fully sent bound what
                    // may still be outstanding at a launch.
                    let fully_sent = sent_at_launch.load(Ordering::SeqCst) / 2;
                    assert!(launched - fully_sent < 2, "a launch beyond the budget");
                    let index = launched;
                    launched += 1;
                    (index < 5).then_some(async move { Ok::<_, Error>(index) })
                },
                |index| vec![index * 10, index * 10 + 1],
                move |unit| {
                    sent_at_spawn.fetch_add(1, Ordering::SeqCst);
                    unit
                },
                window,
                2,
            )
        };
        let feed = tokio::spawn(feed);

        let mut arrived = Vec::new();
        while let Some(unit) = receiver.recv().await {
            arrived.push(unit);
        }
        feed.await.unwrap().unwrap();
        assert_eq!(arrived, vec![0, 1, 10, 11, 20, 21, 30, 31, 40, 41]);
    }

    /// A plan without units frees its budget at once, so the plans behind
    /// it still launch.
    #[tokio::test]
    async fn a_plan_without_units_frees_its_budget() {
        let (window, mut receiver) = mpsc::channel::<usize>(1);
        let mut launched = 0usize;
        let feed = feed_window(
            move || {
                let index = launched;
                launched += 1;
                (index < 3).then_some(async move { Ok::<_, Error>(index) })
            },
            |index| {
                if index < 2 {
                    Vec::new()
                } else {
                    vec![index * 10]
                }
            },
            |unit| unit,
            window,
            1,
        );
        let feed = tokio::spawn(feed);

        let mut arrived = Vec::new();
        while let Some(unit) = receiver.recv().await {
            arrived.push(unit);
        }
        feed.await.unwrap().unwrap();
        assert_eq!(arrived, vec![20]);
    }
}
