//! Column evolution, reconstructed at every snapshot that produced it.
//!
//! The catalog stores a column's history as begin/end-versioned records, so
//! the column set at a past snapshot is *rebuilt*, never stored. This
//! module generates an arbitrary sequence of column operations, replays it
//! through an independent model that knows only the documented rules, and
//! requires the rebuild at each snapshot to equal the model at that point.
//!
//! The model is deliberately not a second call into moraine: it is the
//! rules restated — ids from a per-table counter floored above every live
//! id and never reused, positions continuing past the highest live one and
//! never renumbered, and rename and promotion leaving both alone.

use std::sync::Arc;

use moraine::{
    Catalog, CatalogOptions, ColumnAlteration, ColumnDef, ColumnId, SnapshotId, TableId,
};
use object_store::memory::InMemory;
use proptest::prelude::*;

/// The widening ladder the generator promotes along. DuckLake owns which
/// promotions are legal and moraine does not validate them, so what matters
/// here is that a type change is a version transition the rebuild honours —
/// but generating only widenings keeps the sequences ones DuckLake would
/// actually emit.
const TYPES: [&str; 4] = ["TINYINT", "SMALLINT", "INTEGER", "BIGINT"];

/// How many columns the table starts with.
const INITIAL_COLUMNS: usize = 3;

/// One column as the model tracks it: the fields a column operation can
/// move, in the shape `columns_of` reports them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelColumn {
    id: u64,
    name: String,
    column_type: String,
    position: u64,
}

/// One generated operation. `which` selects a live column modulo the live
/// count, so the generator stays independent of the state it will meet.
#[derive(Debug, Clone, Copy)]
enum ColumnOp {
    Add { column_type: usize },
    Drop { which: usize },
    Rename { which: usize },
    Promote { which: usize, column_type: usize },
}

fn column_op() -> impl Strategy<Value = ColumnOp> {
    prop_oneof![
        (0..TYPES.len()).prop_map(|column_type| ColumnOp::Add { column_type }),
        (0..16_usize).prop_map(|which| ColumnOp::Drop { which }),
        (0..16_usize).prop_map(|which| ColumnOp::Rename { which }),
        (0..16_usize, 0..TYPES.len())
            .prop_map(|(which, column_type)| ColumnOp::Promote { which, column_type }),
    ]
}

/// The model's own state: the live columns, and the counter ids come from.
struct Model {
    columns: Vec<ModelColumn>,
    next_column_id: u64,
}

impl Model {
    /// The shape `create_table` lays down: ids and positions both from 1 in
    /// declaration order, and the counter one past the last.
    fn new() -> Self {
        let columns = (1..=INITIAL_COLUMNS)
            .map(|index| ModelColumn {
                id: index as u64,
                name: format!("c{index}"),
                column_type: TYPES[0].to_string(),
                position: index as u64,
            })
            .collect();
        Self {
            columns,
            next_column_id: INITIAL_COLUMNS as u64 + 1,
        }
    }

    /// The id `add_column` would mint: the counter, floored above every live
    /// id so a resurrected table cannot collide with a survivor.
    fn next_id(&self) -> u64 {
        let live_max = self.columns.iter().map(|c| c.id).max().unwrap_or(0);
        self.next_column_id.max(live_max + 1)
    }

    /// The position `add_column` would assign: past the highest live one,
    /// never renumbering, so a dropped column leaves a gap. Positions start
    /// at 1, so an emptied table restarts there.
    fn next_position(&self) -> u64 {
        self.columns
            .iter()
            .map(|c| c.position)
            .max()
            .map_or(1, |max| max + 1)
    }

    /// The live column `which` selects, or `None` when there is nothing to
    /// select from.
    fn select(&self, which: usize) -> Option<usize> {
        (!self.columns.is_empty()).then(|| which % self.columns.len())
    }

    /// The columns in the order `columns_of` reports them.
    fn expected(&self) -> Vec<ModelColumn> {
        let mut sorted = self.columns.clone();
        sorted.sort_by_key(|c| c.position);
        sorted
    }
}

/// The catalog's rebuild at `snapshot`, in the model's shape.
#[allow(clippy::unwrap_used)]
async fn rebuilt_at(catalog: &Catalog, snapshot: SnapshotId, table: TableId) -> Vec<ModelColumn> {
    let view = catalog.read_scope_at(snapshot).await.unwrap();
    view.columns_of(table)
        .await
        .unwrap()
        .into_iter()
        .map(|column| {
            assert!(
                column.parent_column.is_none(),
                "this generator emits scalar columns only; a nested one would \
                 bring fields the model does not track"
            );
            ModelColumn {
                id: column.id.get(),
                name: column.name,
                column_type: column.column_type,
                position: column.position,
            }
        })
        .collect()
}

/// Applies one generated operation through the verb path and advances the
/// model with it, or reports `None` when the op has nothing to act on —
/// there is no column to select, or the one selected is the table's last
/// and cannot be dropped. A skipped op mints no snapshot, so the history
/// and the model stay in step.
///
/// `index` names the op's position in the sequence, and every fresh name is
/// built from it: no sequence can then generate a name collision, and the
/// model never has to predict one.
#[allow(clippy::unwrap_used)]
async fn apply_op(
    catalog: &Catalog,
    table: TableId,
    model: &mut Model,
    index: usize,
    op: ColumnOp,
) -> Option<SnapshotId> {
    match op {
        ColumnOp::Add { column_type } => {
            let def = ColumnDef {
                name: format!("a{index}"),
                column_type: TYPES[column_type].to_string(),
                nulls_allowed: true,
                default_value: None,
                children: Vec::new(),
            };
            let landed = catalog
                .commit(async |tx| tx.add_column(table, &def).await.map(|_| ()))
                .await
                .unwrap();
            model.columns.push(ModelColumn {
                id: model.next_id(),
                name: def.name,
                column_type: def.column_type,
                position: model.next_position(),
            });
            model.next_column_id = model.columns.last().unwrap().id + 1;
            Some(landed)
        }
        ColumnOp::Drop { which } => {
            let at = model.select(which).filter(|_| model.columns.len() > 1)?;
            let column = ColumnId::new(model.columns[at].id);
            let landed = catalog
                .commit(async |tx| tx.drop_column(table, column).await)
                .await
                .unwrap();
            model.columns.remove(at);
            Some(landed)
        }
        ColumnOp::Rename { which } => {
            let at = model.select(which)?;
            let column = ColumnId::new(model.columns[at].id);
            let name = format!("r{index}");
            let landed = catalog
                .commit(async |tx| tx.rename_column(table, column, &name).await)
                .await
                .unwrap();
            model.columns[at].name = name;
            Some(landed)
        }
        ColumnOp::Promote {
            which,
            column_type: to,
        } => {
            let at = model.select(which)?;
            // Widening only: the wider of where the column is and where the
            // generator points.
            let from = TYPES
                .iter()
                .position(|t| *t == model.columns[at].column_type)
                .unwrap();
            let widened = TYPES[from.max(to)].to_string();
            let column = ColumnId::new(model.columns[at].id);
            let promoted = widened.clone();
            let landed = catalog
                .commit(async |tx| {
                    tx.alter_column(
                        table,
                        column,
                        ColumnAlteration {
                            column_type: Some(promoted.clone()),
                            nulls_allowed: None,
                            default_value: None,
                        },
                    )
                    .await
                })
                .await
                .unwrap();
            model.columns[at].column_type = widened;
            Some(landed)
        }
    }
}

/// Applies `ops` one commit at a time, recording what the model says the
/// table looked like at each snapshot, then checks every one of those
/// snapshots rebuilds to exactly that.
#[allow(clippy::unwrap_used)]
async fn replay_and_compare(ops: &[ColumnOp]) {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();

    let initial: Vec<ColumnDef> = (1..=INITIAL_COLUMNS)
        .map(|index| ColumnDef {
            name: format!("c{index}"),
            column_type: TYPES[0].to_string(),
            nulls_allowed: true,
            default_value: None,
            children: Vec::new(),
        })
        .collect();
    let created = std::cell::Cell::new(None);
    let snapshot = catalog
        .commit(async |tx| {
            let schema = tx.create_schema("s").await?;
            created.set(Some(tx.create_table(schema, "t", &initial).await?));
            Ok(())
        })
        .await
        .unwrap();
    let table = created.get().unwrap();

    let mut model = Model::new();
    let mut history = vec![(snapshot, model.expected())];

    for (index, op) in ops.iter().enumerate() {
        if let Some(snapshot) = apply_op(&catalog, table, &mut model, index, *op).await {
            history.push((snapshot, model.expected()));
        }
    }

    for (snapshot, expected) in history {
        assert_eq!(
            rebuilt_at(&catalog, snapshot, table).await,
            expected,
            "rebuild at snapshot {snapshot} disagrees with the replay"
        );
    }

    catalog.close().await.unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// For an arbitrary sequence of column operations and every snapshot it
    /// produced, the reconstructed column set, its order, its field ids, and
    /// its types equal what the rules say they should be.
    ///
    /// Type promotion is in the generator, so this also carries the
    /// promotion obligation: reconstruction before a widening yields the old
    /// type and after it the new, for every widening in the sequence rather
    /// than for one hand-written pair.
    #[test]
    fn columns_rebuild_at_every_snapshot_the_sequence_produced(
        ops in prop::collection::vec(column_op(), 0..10),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(replay_and_compare(&ops));
    }
}
