use super::row_set::{FileRowSet, FileRowSetKind};

#[test]
fn contiguous_rows_choose_a_range() {
    let rows = FileRowSet::from_sorted((10..1_010).collect()).unwrap();

    assert_eq!(rows.kind(), FileRowSetKind::Range);
    assert!(rows.contains(10));
    assert!(rows.contains(1_009));
    assert!(!rows.contains(1_010));
    assert!(rows.estimated_bytes() <= 32);
}

#[test]
fn moderately_sparse_rows_choose_roaring() {
    let rows = FileRowSet::from_sorted((0..100_000).map(|row| row * 10).collect()).unwrap();

    assert_eq!(rows.kind(), FileRowSetKind::Roaring);
    assert!(rows.contains(420));
    assert!(!rows.contains(421));
    assert!(rows.estimated_bytes() < 800_000);
}

#[test]
fn fragmented_rows_choose_the_predictable_sorted_form() {
    let rows = FileRowSet::from_sorted((0..10_000).map(|row| row << 16).collect()).unwrap();

    assert_eq!(rows.kind(), FileRowSetKind::Sorted);
    assert!(rows.contains(9_999 << 16));
    assert!(!rows.contains((9_999 << 16) + 1));
    assert_eq!(rows.estimated_bytes(), 80_000);
}

#[test]
fn matching_preserves_query_order_and_ignores_absent_rows() {
    let rows = FileRowSet::from_sorted(vec![3, 7, 11, 50]).unwrap();

    assert_eq!(rows.matching(&[1, 3, 7, 12, 50]), vec![3, 7, 50]);
}

#[test]
fn duplicate_or_unsorted_input_is_refused() {
    assert!(FileRowSet::from_sorted(vec![1, 1]).is_err());
    assert!(FileRowSet::from_sorted(vec![2, 1]).is_err());
}
