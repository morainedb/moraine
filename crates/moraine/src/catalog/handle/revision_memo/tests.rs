use proptest::prelude::*;

use super::*;

proptest! {
    #[test]
    fn mixed_operations_match_a_bounded_lru(
        operations in prop::collection::vec((any::<bool>(), 0_u8..3, 0_u8..2, 0_u8..8, any::<u64>()), 0..200),
    ) {
        let memo = RevisionMemo::new(4096, 4);
        let mut expected = std::collections::VecDeque::new();
        for (insert, revision, table, key, value) in operations {
            let identity = (revision, table, key);
            let position = expected.iter().position(|(held, _)| held == &identity);
            let previous = position.and_then(|position| expected.remove(position));
            let table = TableId::new(u64::from(table));
            if insert {
                expected.push_front((identity, value));
                expected.truncate(4);
                memo.put(u64::from(revision), table, CollidingKey(key), Shared::new(value), 0, 8);
            } else {
                let found = memo.get(u64::from(revision), table, &CollidingKey(key));
                prop_assert_eq!(found.as_deref(), previous.as_ref().map(|(_, value)| value));
                if let Some(previous) = previous {
                    expected.push_front(previous);
                }
            }
        }
    }
}

struct CountedKey<'a>(u64, &'a std::sync::atomic::AtomicUsize);

impl PartialEq for CountedKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.0 == other.0
    }
}

impl Eq for CountedKey<'_> {}

impl std::hash::Hash for CountedKey<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[test]
fn a_lookup_does_not_compare_every_retained_key() {
    let comparisons = std::sync::atomic::AtomicUsize::new(0);
    let memo = RevisionMemo::new(1024 * 1024, 4096);
    let table = TableId::new(1);
    for key in 0..4096 {
        memo.put(
            1,
            table,
            CountedKey(key, &comparisons),
            Shared::new(key),
            0,
            8,
        );
    }
    comparisons.store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        memo.get(1, table, &CountedKey(0, &comparisons)).as_deref(),
        Some(&0)
    );
    let compared = comparisons.load(std::sync::atomic::Ordering::Relaxed);
    assert!(compared < 32, "lookup compared {compared} keys");
}

#[derive(PartialEq, Eq)]
struct CollidingKey(u8);

impl std::hash::Hash for CollidingKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u8(0);
    }
}

#[test]
fn collisions_replacement_and_recency_preserve_the_right_values() {
    let memo = RevisionMemo::new(4096, 2);
    let table = TableId::new(1);
    memo.put(1, table, CollidingKey(1), Shared::new(10), 0, 8);
    memo.put(1, table, CollidingKey(2), Shared::new(20), 0, 8);
    let held = memo.get(1, table, &CollidingKey(1)).unwrap();
    memo.put(1, table, CollidingKey(3), Shared::new(30), 0, 8);
    assert!(memo.get(1, table, &CollidingKey(2)).is_none());
    memo.put(1, table, CollidingKey(1), Shared::new(11), 0, 8);
    assert_eq!(memo.get(1, table, &CollidingKey(1)).as_deref(), Some(&11));
    assert_eq!(memo.get(1, table, &CollidingKey(3)).as_deref(), Some(&30));
    assert_eq!(*held, 10);
}

#[test]
fn a_zero_entry_limit_disables_retention() {
    let memo = RevisionMemo::new(1024, 0);
    memo.put(1, TableId::new(1), 1, Shared::new(7), 0, 8);
    assert!(memo.get(1, TableId::new(1), &1).is_none());
}

#[test]
fn near_limit_weights_do_not_overflow_accounting() {
    let memo: RevisionMemo<u8, u64> = RevisionMemo::new(usize::MAX, 8);
    let weight = usize::MAX - RevisionMemo::<u8, u64>::entry_bytes();
    memo.put(1, TableId::new(1), 1, Shared::new(1), 0, weight);
    assert!(memo.get(1, TableId::new(1), &1).is_none());
    assert_eq!(memo.estimated_bytes(), 0);
}

#[test]
fn revisions_tables_and_keys_do_not_alias() {
    let memo: RevisionMemo<u8, u64> = RevisionMemo::new(1024, 8);
    let table = TableId::new(1);
    memo.put(1, table, 7, Shared::new(70), 0, 8);
    assert_eq!(memo.get(1, table, &7).as_deref(), Some(&70));
    assert_eq!(memo.get(2, table, &7), None);
    assert_eq!(memo.get(1, TableId::new(2), &7), None);
    assert_eq!(memo.get(1, table, &8), None);
}

#[test]
fn eviction_holds_the_byte_and_entry_ceilings() {
    let memo: RevisionMemo<u8, u64> = RevisionMemo::new(512, 2);
    let table = TableId::new(1);
    memo.put(1, table, 1, Shared::new(1), 0, 8);
    memo.put(1, table, 2, Shared::new(2), 0, 8);
    memo.put(1, table, 3, Shared::new(3), 0, 8);
    assert_eq!(
        memo.get(1, table, &1),
        None,
        "the count ceiling evicts the oldest"
    );
    assert!(memo.get(1, table, &3).is_some());

    // Below the ceiling with its header, so admitted; everything older
    // goes to make room.
    memo.put(1, table, 4, Shared::new(4), 0, 300);
    assert_eq!(memo.get(1, table, &4).as_deref(), Some(&4));
    assert_eq!(memo.get(1, table, &3), None);
    assert!(memo.entries.lock().unwrap().bytes <= 512);

    memo.put(1, table, 5, Shared::new(5), 0, 1024);
    assert_eq!(
        memo.get(1, table, &5),
        None,
        "an oversized value is not admitted"
    );
}

#[test]
fn multiple_memos_share_a_budget_and_charge_evicted_readers() {
    let budget = Budget::new(16_384);
    let first = RevisionMemo::with_budget(16_384, 8, Arc::clone(&budget));
    let second = RevisionMemo::with_budget(16_384, 8, Arc::clone(&budget));
    let table = TableId::new(1);
    first.put(1, table, 1, Shared::new(vec![0_u8; 4096]), 0, 4096);
    let reader = first.get(1, table, &1).unwrap();
    let occupied = budget.used();
    drop(first);
    assert!(budget.used() > 4096);
    assert!(budget.used() < occupied, "cache containers were released");
    second.put(1, table, 2, Shared::new(vec![0_u8; 12_000]), 0, 12_000);
    assert!(second.get(1, table, &2).is_none());
    assert!(budget.used() <= budget.capacity());
    drop(reader);
    second.put(1, table, 2, Shared::new(vec![0_u8; 12_000]), 0, 12_000);
    assert!(second.get(1, table, &2).is_some());
    drop(second);
    assert_eq!(budget.used(), 0);
}

#[test]
fn denied_retention_does_not_copy_query_results() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Counted(Arc<AtomicUsize>);
    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::Relaxed);
            Self(self.0.clone())
        }
    }
    let copies = Arc::new(AtomicUsize::new(0));
    let value = Counted(copies.clone());
    let budget = Budget::new(4096);
    let mut other = crate::store::cache::retention::Charge::new(budget.clone());
    assert!(other.resize(4096));
    let memo = RevisionMemo::with_budget(4096, 16, budget);
    memo.put_cloned(1, TableId::new(1), 1, &value, 0, 0);
    assert_eq!(copies.load(Ordering::Relaxed), 0);
    drop(other);
    memo.put_cloned(1, TableId::new(1), 1, &value, 0, 0);
    assert_eq!(copies.load(Ordering::Relaxed), 1);
    assert!(memo.get(1, TableId::new(1), &1).is_some());
}
