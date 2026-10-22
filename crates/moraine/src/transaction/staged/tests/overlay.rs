use super::{super::overlay::overlay_versioned, *};

#[test]
fn table_replacement_preserves_the_field_id_counter() {
    let prior = proto::TableValue {
        table_id: 1,
        next_column_id: 42,
        ..Default::default()
    };
    let operations = [
        RowOperation::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        },
        RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 0, "renamed", 2, None),
        },
        RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(2, 0, "new", 2, None),
        },
    ];
    let rows = overlay_versioned(&operations, vec![prior]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].end_snapshot, Some(2));
    assert_eq!(rows[1].table_name, "renamed");
    assert_eq!(rows[1].next_column_id, 42);
    assert_eq!(rows[2].next_column_id, 1);
}
