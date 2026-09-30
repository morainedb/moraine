use object_store::path::Path;

use super::{
    row_set::{FileRowSet, PositionedRowSet, RowOrder},
    sidecar::{self, Rejected, SidecarIdentity},
};

fn identity() -> SidecarIdentity<'static> {
    SidecarIdentity {
        table_id: 7,
        data_file_id: 42,
        file_path: "table/data-0.parquet",
        file_size: 1024,
    }
}

fn permuted() -> PositionedRowSet {
    PositionedRowSet::from_file_order(vec![70, 10, 30]).unwrap()
}

fn positions(rows: &PositionedRowSet, row_id: u64) -> Vec<u64> {
    let mut found = Vec::new();
    rows.visit_positions(row_id, |position| found.push(position));
    found
}

#[test]
fn a_sidecar_sits_beside_the_file_it_describes() {
    let data_file = Path::parse("lake/table/data-0.parquet").unwrap();
    assert_eq!(
        sidecar::path_for(&data_file).unwrap().as_ref(),
        "lake/table/data-0.parquet.rowsum"
    );
}

#[test]
fn a_summary_round_trips_through_its_published_form() {
    let rows = permuted();
    let bytes = sidecar::encode(identity(), &rows).unwrap();
    let decoded = sidecar::summary(identity(), &bytes).unwrap();

    for row_id in [10, 30, 70] {
        assert_eq!(positions(&decoded, row_id), positions(&rows, row_id));
    }
    assert!(!decoded.rows.contains(11));
}

#[test]
fn a_header_names_where_each_half_ends() {
    let rows = permuted();
    let bytes = sidecar::encode(identity(), &rows).unwrap();
    let layout = sidecar::layout_of(identity(), &bytes).unwrap();

    assert_eq!(
        layout.membership_end() + layout.order_len,
        bytes.len(),
        "the two halves account for everything after the header"
    );
    assert!(layout.order_len > 0, "a permuted summary carries an order");
}

#[test]
fn a_sidecar_describing_another_file_is_refused() {
    let bytes = sidecar::encode(identity(), &permuted()).unwrap();

    for wrong in [
        SidecarIdentity {
            data_file_id: 43,
            ..identity()
        },
        SidecarIdentity {
            file_size: 2048,
            ..identity()
        },
        SidecarIdentity {
            table_id: 8,
            ..identity()
        },
        SidecarIdentity {
            file_path: "table/data-1.parquet",
            ..identity()
        },
    ] {
        assert_eq!(
            sidecar::summary(wrong, &bytes).err(),
            Some(Rejected::Identity),
            "a mismatched header answered anyway"
        );
    }
}

#[test]
fn an_unreadable_sidecar_is_absent_rather_than_wrong() {
    let bytes = sidecar::encode(identity(), &permuted()).unwrap();

    let mut foreign = bytes.clone();
    foreign[0] = b'X';
    assert_eq!(
        sidecar::summary(identity(), &foreign).err(),
        Some(Rejected::Malformed)
    );

    let mut newer = bytes.clone();
    newer[8] = 2;
    assert_eq!(
        sidecar::summary(identity(), &newer).err(),
        Some(Rejected::Version)
    );

    assert_eq!(
        sidecar::summary(identity(), &bytes[..bytes.len() - 1]).err(),
        Some(Rejected::Malformed)
    );
    assert_eq!(
        sidecar::summary(identity(), &[]).err(),
        Some(Rejected::Malformed)
    );
}

#[test]
fn an_ascending_summary_publishes_no_order() {
    let rows = PositionedRowSet::from_file_order(vec![10, 30, 70]).unwrap();
    assert!(matches!(rows.order, RowOrder::Ascending));

    let bytes = sidecar::encode(identity(), &rows).unwrap();
    let layout = sidecar::layout_of(identity(), &bytes).unwrap();
    assert_eq!(layout.order_len, 0);

    let decoded = sidecar::summary(identity(), &bytes).unwrap();
    assert_eq!(positions(&decoded, 70), vec![2]);
}

#[test]
fn a_repeated_summary_keeps_every_physical_position() {
    let rows = PositionedRowSet::from_file_order(vec![30, 10, 30]).unwrap();
    let bytes = sidecar::encode(identity(), &rows).unwrap();
    let decoded = sidecar::summary(identity(), &bytes).unwrap();

    assert_eq!(positions(&decoded, 30), vec![0, 2]);
    assert_eq!(positions(&decoded, 10), vec![1]);
}

#[test]
fn a_dense_summary_publishes_its_range() {
    let rows = PositionedRowSet {
        rows: FileRowSet::range(100, 3).unwrap(),
        order: RowOrder::Ascending,
    };
    let bytes = sidecar::encode(identity(), &rows).unwrap();
    let decoded = sidecar::summary(identity(), &bytes).unwrap();

    assert_eq!(positions(&decoded, 101), vec![1]);
    assert!(!decoded.rows.contains(103));
}

proptest::proptest! {
    /// Any file order publishes and reads back with the positions it had.
    #[test]
    fn any_file_order_round_trips_through_its_published_form(
        file_order in proptest::collection::vec(0_u64..64, 1..48),
    ) {
        let rows = PositionedRowSet::from_file_order(file_order.clone()).unwrap();
        let bytes = sidecar::encode(identity(), &rows).unwrap();
        let decoded = sidecar::summary(identity(), &bytes).unwrap();

        for row_id in &file_order {
            proptest::prop_assert_eq!(positions(&decoded, *row_id), positions(&rows, *row_id));
        }
    }
}
