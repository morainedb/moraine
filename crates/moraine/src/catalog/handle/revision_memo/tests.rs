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
                memo.put(u64::from(revision), table, CollidingKey(key), Arc::new(value), 8);
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
        memo.put(1, table, CountedKey(key, &comparisons), Arc::new(key), 8);
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
    memo.put(1, table, CollidingKey(1), Arc::new(10), 8);
    memo.put(1, table, CollidingKey(2), Arc::new(20), 8);
    let held = memo.get(1, table, &CollidingKey(1)).unwrap();
    memo.put(1, table, CollidingKey(3), Arc::new(30), 8);
    assert!(memo.get(1, table, &CollidingKey(2)).is_none());
    memo.put(1, table, CollidingKey(1), Arc::new(11), 8);
    assert_eq!(memo.get(1, table, &CollidingKey(1)).as_deref(), Some(&11));
    assert_eq!(memo.get(1, table, &CollidingKey(3)).as_deref(), Some(&30));
    assert_eq!(*held, 10);
}

#[test]
fn a_zero_entry_limit_disables_retention() {
    let memo = RevisionMemo::new(1024, 0);
    memo.put(1, TableId::new(1), 1, Arc::new(7), 8);
    assert!(memo.get(1, TableId::new(1), &1).is_none());
}

#[test]
fn near_limit_weights_do_not_overflow_accounting() {
    let memo: RevisionMemo<u8, u64> = RevisionMemo::new(usize::MAX, 8);
    let weight = usize::MAX - RevisionMemo::<u8, u64>::entry_bytes();
    memo.put(1, TableId::new(1), 1, Arc::new(1), weight);
    assert_eq!(memo.estimated_bytes(), usize::MAX as u64);
    memo.put(1, TableId::new(1), 2, Arc::new(2), weight);
    assert!(memo.get(1, TableId::new(1), &1).is_none());
    assert!(memo.get(1, TableId::new(1), &2).is_some());
}

#[test]
fn revisions_tables_and_keys_do_not_alias() {
    let memo: RevisionMemo<u8, u64> = RevisionMemo::new(1024, 8);
    let table = TableId::new(1);
    memo.put(1, table, 7, Arc::new(70), 8);
    assert_eq!(memo.get(1, table, &7).as_deref(), Some(&70));
    assert_eq!(memo.get(2, table, &7), None);
    assert_eq!(memo.get(1, TableId::new(2), &7), None);
    assert_eq!(memo.get(1, table, &8), None);
}

#[test]
fn eviction_holds_the_byte_and_entry_ceilings() {
    let memo: RevisionMemo<u8, u64> = RevisionMemo::new(256, 2);
    let table = TableId::new(1);
    memo.put(1, table, 1, Arc::new(1), 8);
    memo.put(1, table, 2, Arc::new(2), 8);
    memo.put(1, table, 3, Arc::new(3), 8);
    assert_eq!(
        memo.get(1, table, &1),
        None,
        "the count ceiling evicts the oldest"
    );
    assert!(memo.get(1, table, &3).is_some());

    // Below the ceiling with its header, so admitted; everything older
    // goes to make room.
    memo.put(1, table, 4, Arc::new(4), 160);
    assert_eq!(memo.get(1, table, &4).as_deref(), Some(&4));
    assert_eq!(memo.get(1, table, &3), None);
    assert!(memo.entries.lock().unwrap().bytes <= 256);

    memo.put(1, table, 5, Arc::new(5), 1024);
    assert_eq!(
        memo.get(1, table, &5),
        None,
        "an oversized value is not admitted"
    );
}
