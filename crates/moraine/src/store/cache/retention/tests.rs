use super::*;

#[test]
fn readers_keep_their_charge_until_the_last_clone_drops() {
    let budget = Budget::new(100);
    let first = Shared::new(42);
    assert!(first.try_charge(&budget, 60));
    let reader = first.clone();
    drop(first);
    assert_eq!(budget.used(), 60);
    let second = Shared::new(7);
    assert!(!second.try_charge(&budget, 50));
    assert_eq!(*second, 7);
    drop(reader);
    assert_eq!(budget.used(), 0);
    assert!(second.try_charge(&budget, 50));
    assert!(second.try_charge(&budget, 50));
    assert_eq!(budget.used(), 50);
    drop(second);
    assert_eq!(budget.used(), 0);
}

#[test]
fn container_growth_is_atomic_and_releases_capacity() {
    let budget = Budget::new(100);
    let mut first = Charge::new(Arc::clone(&budget));
    let mut second = Charge::new(Arc::clone(&budget));
    assert!(first.resize(70));
    assert!(!second.resize(40));
    assert_eq!(budget.used(), 70);
    assert!(first.resize(20));
    assert!(second.resize(80));
    assert!(!first.resize(u64::MAX));
    assert_eq!(budget.used(), 100);
    drop(first);
    drop(second);
    assert_eq!(budget.used(), 0);
}

#[test]
fn simultaneous_catalogs_cannot_overdraw_the_budget() {
    let budget = Budget::new(100);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                let mut charge = Charge::new(Arc::clone(&budget));
                barrier.wait();
                charge.resize(30);
                barrier.wait();
                let used = budget.used();
                barrier.wait();
                assert!(used <= 100);
            });
        }
    });
    assert_eq!(budget.used(), 0);
}
