//! Positional deletions must name data files still present at publication.

use super::*;

pub(super) fn validate(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    rebase: &Rebase,
) -> Result<()> {
    let mut inserted = BTreeSet::new();
    let mut targets = BTreeSet::new();
    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DataFile,
                cells,
            } => {
                let mut file = decode::decode_data_file(cells)?;
                rebase.data_file(&mut file);
                inserted.insert((file.table_id, file.data_file_id));
            }
            RowOperation::Insert {
                table: TableKind::DeleteFile,
                cells,
            } => {
                let mut file = decode::decode_delete_file(cells)?;
                rebase.delete_file(&mut file);
                targets.insert((file.table_id, file.data_file_id));
            }
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                ..
            } => {
                targets.insert((*table_id, *data_file_id));
            }
            _ => {}
        }
    }
    for (table, file) in targets {
        if !inserted.contains(&(table, file))
            && !base
                .data_files
                .get(&table)
                .is_some_and(|files| files.contains_key(&file))
        {
            return Err(Error::CommitConflict(format!(
                "data file {file} in table {table} is no longer live; retry the deletion"
            )));
        }
    }
    Ok(())
}
