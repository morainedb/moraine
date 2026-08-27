use super::row_set::{FileRowSet, FileRowSetKind, PositionedRowSet};

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

#[test]
fn ascending_input_collapses_through_from_sorted_shapes() {
    let range = PositionedRowSet::from_file_order((10..1_010).collect()).unwrap();
    assert_eq!(range.rows.kind(), FileRowSetKind::Range);

    let roaring =
        PositionedRowSet::from_file_order((0..100_000).map(|row| row * 10).collect()).unwrap();
    assert_eq!(roaring.rows.kind(), FileRowSetKind::Roaring);

    let sorted =
        PositionedRowSet::from_file_order((0..10_000).map(|row| row << 16).collect()).unwrap();
    assert_eq!(sorted.rows.kind(), FileRowSetKind::Sorted);
}

#[test]
fn non_ascending_input_permutes_the_ascending_representation() {
    // Two ascending runs, as an UPDATE rewriting rows from two source files
    // would emit: file order is [30, 40, 10, 20], ascending order is
    // [10, 20, 30, 40] at file positions [2, 3, 0, 1].
    let rows = PositionedRowSet::from_file_order(vec![30, 40, 10, 20]).unwrap();

    assert_eq!(rows.rows.kind(), FileRowSetKind::Sorted);
    assert_eq!(
        rows.positions_of(&[30, 40, 10, 20, 999]),
        vec![Some(0), Some(1), Some(2), Some(3), None]
    );
}

#[test]
fn non_ascending_duplicate_ids_are_refused() {
    assert!(PositionedRowSet::from_file_order(vec![30, 10, 30]).is_err());
}

#[test]
fn positions_of_range_is_arithmetic() {
    let rows = PositionedRowSet::from_file_order((100..110).collect()).unwrap();
    assert_eq!(rows.rows.kind(), FileRowSetKind::Range);

    assert_eq!(
        rows.positions_of(&[100, 105, 109, 110]),
        vec![Some(0), Some(5), Some(9), None]
    );
}

#[test]
fn positions_of_ascending_roaring_uses_rank() {
    let rows =
        PositionedRowSet::from_file_order((0..100_000).map(|row| row * 10).collect()).unwrap();
    assert_eq!(rows.rows.kind(), FileRowSetKind::Roaring);

    assert_eq!(
        rows.positions_of(&[0, 10, 420, 421]),
        vec![Some(0), Some(1), Some(42), None]
    );
}

#[test]
fn positions_of_ascending_sorted_uses_binary_search() {
    let rows =
        PositionedRowSet::from_file_order((0..10_000).map(|row| row << 16).collect()).unwrap();
    assert_eq!(rows.rows.kind(), FileRowSetKind::Sorted);

    assert_eq!(
        rows.positions_of(&[0, 1 << 16, (9_999 << 16) + 1]),
        vec![Some(0), Some(1), None]
    );
}

/// A permutation over a Roaring-shaped set: sparse ascending ids rewritten
/// out of file order still answer positions correctly.
#[test]
fn positions_of_permuted_roaring_uses_rank_then_permutation() {
    let sorted_sparse: Vec<u64> = (0..100_000).map(|row| row * 10).collect();
    // Swap the first two ids' file positions: file order now disagrees with
    // ascending order only at the front.
    let mut file_order = sorted_sparse.clone();
    file_order.swap(0, 1);

    let rows = PositionedRowSet::from_file_order(file_order).unwrap();
    assert_eq!(rows.rows.kind(), FileRowSetKind::Roaring);

    assert_eq!(
        rows.positions_of(&[0, 10, 20]),
        vec![Some(1), Some(0), Some(2)]
    );
}

#[test]
fn positions_of_absent_row_is_none() {
    let rows = PositionedRowSet::from_file_order(vec![30, 10, 20]).unwrap();

    assert_eq!(rows.positions_of(&[999]), vec![None]);
}
