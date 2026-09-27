//! Row ids a flush or a compaction must keep. The commit that drains
//! inline chunks registers the files DuckLake wrote from them, and the
//! commit that compacts files registers what it wrote from its sources;
//! either way the files must name the row ids they re-home, one per
//! physical row, or the commit is refused.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use futures::{StreamExt, TryStreamExt, stream};
use tracing::debug;

use super::{
    RowOperation, TableKind,
    decode::{Cursor, decode_data_file, decode_hard_delete},
    index_upkeep::{data_file_object_path, ended_data_file},
};
use crate::{
    catalog::CatalogSnapshot,
    data_file::{self, DataStore, RowIdSource, ScopedRows},
    error::{Error, Result},
    store::{key::EntityKey, proto},
    transaction::operations::ChangeSet,
};

/// Files read for their row ids at once.
const ROW_ID_READ_CONCURRENCY: usize = 8;

/// The row ids one commit drains from each `(table_id, schema_version)`'s
/// inline chunks, one per physical row: a row id repeats once per version
/// drained. Keyed so a drain the commit names twice counts once.
pub(super) type DrainedRows = HashMap<(u64, u64), Vec<u64>>;

/// Refuses a flush whose registered files carry row ids other than the
/// drained chunks'. A table with drained rows but no registered file is
/// left alone, and so is a commit with no data store to read from.
pub(super) async fn verify_flushed_row_ids(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    drained: &DrainedRows,
    store: Option<&DataStore>,
    data_prefix: &str,
) -> Result<()> {
    if drained.is_empty() {
        return Ok(());
    }
    let Some(store) = store else {
        debug!(
            "a flush registered files this commit has no data store to read, so its row ids go \
             unverified"
        );
        return Ok(());
    };

    let mut drained_by_table: HashMap<u64, Vec<u64>> = HashMap::new();
    for ((table_id, _), rows) in drained {
        drained_by_table
            .entry(*table_id)
            .or_default()
            .extend_from_slice(rows);
    }

    let files = flushed_files(ops, &drained_by_table)?;
    for (table_id, mut expected) in drained_by_table {
        let Some(files) = files.get(&table_id) else {
            continue;
        };
        expected.sort_unstable();

        let mut found = Vec::with_capacity(expected.len());
        for file in files {
            found.extend(read_row_ids(base, store, data_prefix, file).await?);
        }
        found.sort_unstable();

        if found != expected {
            return Err(mismatch(table_id, files, &expected, &found));
        }
    }
    Ok(())
}

/// Refuses a compaction whose registered files carry row ids other than
/// its sources': a merge names every source row once, and a rewrite may
/// drop rows but never renames one. A table whose sources this commit
/// cannot find is left alone, and so is a commit with no data store.
pub(super) async fn verify_compacted_row_ids(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    store: Option<&DataStore>,
    data_prefix: &str,
) -> Result<()> {
    let Some(changes) = change_set(ops)? else {
        return Ok(());
    };
    let rewritten: BTreeSet<u64> = changes
        .rewrite_delete_tables
        .iter()
        .chain(&changes.compacted_tables)
        .copied()
        .collect();
    let compacted: BTreeSet<u64> = changes
        .merge_adjacent_tables
        .iter()
        .chain(&rewritten)
        .copied()
        .collect();
    if compacted.is_empty() {
        return Ok(());
    }
    let Some(store) = store else {
        debug!(
            "a compaction registered files this commit has no data store to read, so its row ids \
             go unverified"
        );
        return Ok(());
    };

    let (sources, mut outputs) = compaction_files(ops, &compacted)?;
    for table_id in compacted {
        let source_ids = sources.get(&table_id).cloned().unwrap_or_default();
        let outputs = outputs.remove(&table_id).unwrap_or_default();
        if source_ids.is_empty() && outputs.is_empty() {
            continue;
        }
        let Some(records) = source_records(base, table_id, &source_ids) else {
            debug!(
                table_id,
                "a compaction ends files this commit cannot find, so its row ids go unverified"
            );
            continue;
        };

        let mut held = read_all_row_ids(base, store, data_prefix, &records).await?;
        held.sort_unstable();
        let mut carried = read_all_row_ids(base, store, data_prefix, &outputs).await?;
        carried.sort_unstable();

        // A merge of a table also rewriting it is a shape DuckLake never
        // commits; the looser rule stands for it.
        let every_row = !rewritten.contains(&table_id);
        let detail = if every_row {
            (carried != held).then(|| first_divergence(&held, &carried, "its sources"))
        } else {
            first_unheld(&held, &carried)
                .map(|row_id| format!("row id {row_id} is not one its sources hold"))
        };
        if let Some(detail) = detail {
            return Err(compaction_mismatch(
                table_id,
                &outputs,
                &source_ids,
                held.len(),
                carried.len(),
                &detail,
            ));
        }
    }
    Ok(())
}

/// What the commit's `ducklake_snapshot_changes` row says it did.
fn change_set(ops: &[RowOperation]) -> Result<Option<ChangeSet>> {
    for op in ops {
        if let RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells,
        } = op
        {
            // The row's full shape is validated when the snapshot record
            // is built.
            let mut cursor = Cursor::new(TableKind::SnapshotChanges, cells);
            cursor.u64()?;
            return Ok(Some(ChangeSet::parse(&cursor.string()?)));
        }
    }
    Ok(None)
}

type SourceIds = BTreeMap<u64, BTreeSet<u64>>;
type Outputs = BTreeMap<u64, Vec<proto::DataFileValue>>;

/// The files a compaction consumes and the files it registers, per
/// compacted table. A source is a data file the commit hard-deletes or
/// ends; an output is one it registers backdated below its own snapshot
/// (a merge) or rebases onto it (a rewrite). A file registered at the
/// commit's snapshot and left there is an ordinary append.
fn compaction_files(
    ops: &[RowOperation],
    compacted: &BTreeSet<u64>,
) -> Result<(SourceIds, Outputs)> {
    let minted = minted_snapshot(ops)?;
    let mut rebased: HashSet<(u64, u64)> = HashSet::new();
    for op in ops {
        if let RowOperation::UpdateSetBegin {
            table: TableKind::DataFile,
            cells,
        } = op
        {
            rebased.insert(ended_data_file(cells)?);
        }
    }

    let mut sources: SourceIds = BTreeMap::new();
    let mut outputs: Outputs = BTreeMap::new();
    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DataFile,
                cells,
            } => {
                let file = decode_data_file(cells)?;
                if !compacted.contains(&file.table_id) {
                    continue;
                }
                let backdated = minted.is_some_and(|snapshot| file.begin_snapshot < snapshot);
                if backdated || rebased.contains(&(file.table_id, file.data_file_id)) {
                    outputs.entry(file.table_id).or_default().push(file);
                }
            }
            RowOperation::Delete {
                table: TableKind::DataFile,
                cells,
            } => {
                if let (
                    EntityKey::File {
                        table_id,
                        data_file_id,
                    },
                    None,
                ) = decode_hard_delete(TableKind::DataFile, cells)?
                {
                    sources.entry(table_id).or_default().insert(data_file_id);
                }
            }
            RowOperation::UpdateSetEnd {
                table: TableKind::DataFile,
                cells,
            } => {
                let (table_id, data_file_id) = ended_data_file(cells)?;
                sources.entry(table_id).or_default().insert(data_file_id);
            }
            _ => {}
        }
    }
    Ok((sources, outputs))
}

/// The committed records of `file_ids` in `table_id`, or `None` when any
/// is missing from the base snapshot.
fn source_records(
    base: &CatalogSnapshot,
    table_id: u64,
    file_ids: &BTreeSet<u64>,
) -> Option<Vec<proto::DataFileValue>> {
    let files = base.data_files.get(&table_id)?;
    file_ids
        .iter()
        .map(|file_id| files.get(file_id).cloned())
        .collect()
}

/// Every row id `files` carry, in no particular order.
async fn read_all_row_ids(
    base: &CatalogSnapshot,
    store: &DataStore,
    data_prefix: &str,
    files: &[proto::DataFileValue],
) -> Result<Vec<u64>> {
    stream::iter(files)
        .map(|file| read_row_ids(base, store, data_prefix, file))
        .buffered(ROW_ID_READ_CONCURRENCY)
        .try_fold(Vec::new(), |mut row_ids, file_row_ids| async move {
            row_ids.extend(file_row_ids);
            Ok(row_ids)
        })
        .await
}

/// The first of `carried` that `held` does not cover, both sorted, each
/// occurrence of an id consuming one of `held`'s.
fn first_unheld(held: &[u64], carried: &[u64]) -> Option<u64> {
    let mut held = held.iter().peekable();
    for row_id in carried {
        while held.peek().is_some_and(|candidate| *candidate < row_id) {
            held.next();
        }
        if held.next_if_eq(&row_id).is_none() {
            return Some(*row_id);
        }
    }
    None
}

/// The data files `ops` register for the drained tables, backdated below
/// the commit's own snapshot: a file registered at that snapshot is an
/// ordinary append riding the same transaction, not a flush output.
fn flushed_files(
    ops: &[RowOperation],
    drained: &HashMap<u64, Vec<u64>>,
) -> Result<HashMap<u64, Vec<proto::DataFileValue>>> {
    let minted = minted_snapshot(ops)?;
    let mut files: HashMap<u64, Vec<proto::DataFileValue>> = HashMap::new();
    for op in ops {
        let RowOperation::Insert {
            table: TableKind::DataFile,
            cells,
        } = op
        else {
            continue;
        };
        let file = decode_data_file(cells)?;
        if !drained.contains_key(&file.table_id)
            || minted.is_some_and(|snapshot| file.begin_snapshot >= snapshot)
        {
            continue;
        }
        files.entry(file.table_id).or_default().push(file);
    }
    Ok(files)
}

/// The snapshot id this commit mints, if it mints one.
fn minted_snapshot(ops: &[RowOperation]) -> Result<Option<u64>> {
    for op in ops {
        if let RowOperation::Insert {
            table: TableKind::Snapshot,
            cells,
        } = op
        {
            let mut cursor = Cursor::new(TableKind::Snapshot, cells);
            return Ok(Some(cursor.u64()?));
        }
    }
    Ok(None)
}

/// Every row id `file` carries, in file order.
async fn read_row_ids(
    base: &CatalogSnapshot,
    store: &DataStore,
    data_prefix: &str,
    file: &proto::DataFileValue,
) -> Result<Vec<u64>> {
    let path = data_file_object_path(base, file, data_prefix)?;
    let parquet =
        data_file::ParquetFile::new(store.clone(), path, file.file_size_bytes, file.footer_size);
    data_file::scoped_read_entry_batches(
        parquet,
        &[],
        ScopedRows::All,
        RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
    )
    .await?
    .try_fold(Vec::new(), |mut row_ids, batch| async move {
        row_ids.extend(batch.iter().map(|entry| entry.row_id));
        Ok(row_ids)
    })
    .await
}

/// Names the first row id at which the files and the chunks part, as a
/// corruption the commit refuses. The text avoids the substrings DuckLake
/// retries on.
fn mismatch(
    table_id: u64,
    files: &[proto::DataFileValue],
    expected: &[u64],
    found: &[u64],
) -> Error {
    let file_ids: Vec<u64> = files.iter().map(|file| file.data_file_id).collect();
    let detail = first_divergence(expected, found, "the chunks");
    Error::Corruption(format!(
        "flush of table {table_id} registered data files {file_ids:?} carrying {} row ids where \
         the drained inline chunks hold {}: {detail}; the flush is refused so the index keeps \
         naming the rows it did",
        found.len(),
        expected.len()
    ))
}

/// As [`mismatch`], for a compaction.
fn compaction_mismatch(
    table_id: u64,
    outputs: &[proto::DataFileValue],
    source_ids: &BTreeSet<u64>,
    held: usize,
    carried: usize,
    detail: &str,
) -> Error {
    let output_ids: Vec<u64> = outputs.iter().map(|file| file.data_file_id).collect();
    Error::Corruption(format!(
        "compaction of table {table_id} registered data files {output_ids:?} carrying {carried} \
         row ids where its sources {source_ids:?} hold {held}: {detail}; the compaction is \
         refused so the index keeps naming the rows it did"
    ))
}

/// Where two sorted id lists first part, naming `holder` as the owner of
/// `expected`.
fn first_divergence(expected: &[u64], found: &[u64], holder: &str) -> String {
    let divergence = expected
        .iter()
        .zip(found)
        .position(|(expected, found)| expected != found)
        .unwrap_or_else(|| expected.len().min(found.len()));
    match (expected.get(divergence), found.get(divergence)) {
        (Some(expected), Some(found)) => {
            format!(
                "sorted position {divergence} holds row id {found} where {holder} hold {expected}"
            )
        }
        (Some(expected), None) => format!("the files stop before row id {expected}"),
        (None, Some(found)) => format!("the files go on with row id {found}"),
        (None, None) => "the counts differ".to_owned(),
    }
}
