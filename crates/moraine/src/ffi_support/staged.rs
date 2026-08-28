//! The staged-row transaction seam: DuckLake authors rows over the ABI
//! instead of `moraine`'s own verb API. Re-exports `transaction::staged`'s
//! types and adds the one entry point that needs a [`Catalog`].
//! `#[doc(hidden)]`, unstable, as with all of [`crate::ffi_support`].

#[doc(hidden)]
pub use crate::transaction::staged::{
    Cell, CommitReport, RowOperation, StagedTransaction, TableKind,
};
use crate::{catalog::Catalog, data_file::DataStore, error::Result};

/// Begins a staged-row transaction at the current head. `data_store` (with
/// its bucket-relative `data_prefix`) is the `DATA_PATH` object store the
/// commit scoped-reads registered files from to maintain equality indexes;
/// `None` disables that maintenance.
///
/// # Errors
///
/// Returns an error if the underlying store transaction cannot be opened.
#[doc(hidden)]
pub async fn staged_begin(
    catalog: &Catalog,
    data_store: Option<DataStore>,
    data_prefix: String,
) -> Result<StagedTransaction> {
    let db_tx = catalog.begin_write_tx().await?;
    Ok(StagedTransaction::begin(
        db_tx,
        catalog.projections().clone(),
        Some(catalog.store()),
        data_store,
        data_prefix,
        catalog.data_reads(),
        catalog.cache_counters(),
    ))
}

// Each child projection below is its parents' rows plus the rows the
// transaction staged for parents it is inserting. A staged child naming a
// parent the transaction is not inserting is dropped here; commit refuses
// it.

/// `ducklake_partition_column` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_partition_column_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::PartitionColumnRow>> {
    let specs = tx.visible_partition_info().await?;
    let table_of: std::collections::HashMap<u64, u64> = specs
        .iter()
        .map(|spec| (spec.partition_id, spec.table_id))
        .collect();
    let mut rows = crate::ffi_support::partition_column_rows_from(specs);
    rows.extend(
        tx.staged_partition_columns()?
            .into_iter()
            .filter_map(|(partition_id, column)| {
                let table_id = table_of.get(&partition_id).copied()?;
                Some(crate::ffi_support::PartitionColumnRow {
                    partition_id,
                    table_id,
                    partition_key_index: column.partition_key_index,
                    column_id: column.column_id,
                    transform: column.transform,
                })
            }),
    );

    Ok(rows)
}

/// `ducklake_file_partition_value` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_file_partition_value_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::FilePartitionValueRow>> {
    let files = tx.visible_data_files().await?;
    let mut rows = crate::ffi_support::file_partition_value_rows_from(files);
    rows.extend(tx.staged_file_partition_values()?.into_iter().map(
        |((table_id, data_file_id), value)| crate::ffi_support::FilePartitionValueRow {
            data_file_id,
            table_id,
            partition_key_index: value.partition_key_index,
            partition_value: value.partition_value,
        },
    ));

    Ok(rows)
}

/// `ducklake_sort_expression` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_sort_expression_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::SortExpressionRow>> {
    let specs = tx.visible_sort_info().await?;
    let table_of: std::collections::HashMap<u64, u64> = specs
        .iter()
        .map(|spec| (spec.sort_id, spec.table_id))
        .collect();
    let mut rows = crate::ffi_support::sort_expression_rows_from(specs);
    rows.extend(
        tx.staged_sort_expressions()?
            .into_iter()
            .filter_map(|(sort_id, expression)| {
                let table_id = table_of.get(&sort_id).copied()?;
                Some(crate::ffi_support::SortExpressionRow {
                    sort_id,
                    table_id,
                    sort_key_index: expression.sort_key_index,
                    expression: expression.expression,
                    dialect: expression.dialect,
                    sort_direction: expression.sort_direction,
                    null_order: expression.null_order,
                })
            }),
    );

    Ok(rows)
}

/// `ducklake_column_tag` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_column_tag_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::ColumnTagRow>> {
    let mut rows = crate::ffi_support::column_tag_rows_from(
        tx.visible_columns()
            .await?
            .iter()
            .map(crate::ffi_support::ColumnTags::from),
    );
    rows.extend(
        tx.staged_column_tags()?
            .into_iter()
            .map(
                |((table_id, column_id), tag)| crate::ffi_support::ColumnTagRow {
                    table_id,
                    column_id,
                    begin_snapshot: tag.begin_snapshot,
                    end_snapshot: tag.end_snapshot,
                    key: tag.key,
                    value: tag.value,
                },
            ),
    );

    Ok(rows)
}

/// `ducklake_macro_impl` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_macro_impl_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::MacroImplRow>> {
    let mut rows = crate::ffi_support::macro_impl_rows_from(tx.visible_macros().await?);
    rows.extend(
        tx.staged_macro_impls()?
            .into_iter()
            .map(
                |(macro_id, implementation)| crate::ffi_support::MacroImplRow {
                    macro_id,
                    impl_id: implementation.impl_id,
                    dialect: implementation.dialect,
                    sql: implementation.sql,
                    macro_type: implementation.macro_type,
                },
            ),
    );

    Ok(rows)
}

/// `ducklake_macro_parameters` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_macro_parameter_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::MacroParameterRow>> {
    let mut rows = crate::ffi_support::macro_parameter_rows_from(tx.visible_macros().await?);
    rows.extend(tx.staged_macro_parameters()?.into_iter().map(
        |((macro_id, impl_id), parameter)| crate::ffi_support::MacroParameterRow {
            macro_id,
            impl_id,
            column_id: parameter.column_id,
            parameter_name: parameter.parameter_name,
            parameter_type: parameter.parameter_type,
            default_value: parameter.default_value,
            default_value_type: parameter.default_value_type,
        },
    ));

    Ok(rows)
}

/// `ducklake_name_mapping` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_name_mapping_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::NameMappingRow>> {
    let mut rows = crate::ffi_support::name_mapping_rows_from(tx.visible_mappings().await?);
    rows.extend(
        tx.staged_name_mappings()?
            .into_iter()
            .map(|(mapping_id, mapping)| crate::ffi_support::NameMappingRow {
                mapping_id,
                column_id: mapping.column_id,
                source_name: mapping.source_name,
                target_field_id: mapping.target_field_id,
                parent_column: mapping.parent_column,
                is_partition: mapping.is_partition,
            }),
    );

    Ok(rows)
}

/// `ducklake_tag` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_tag_rows(tx: &StagedTransaction) -> Result<Vec<crate::ffi_support::TagRow>> {
    Ok(crate::ffi_support::tag_rows_from(
        tx.visible_tag_containers().await?,
    ))
}

/// `ducklake_metadata` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_option_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::OptionRow>> {
    Ok(crate::ffi_support::option_rows_from(
        tx.visible_option_scopes().await?,
    ))
}

/// `ducklake_schema_versions` rows as `tx` sees them.
///
/// # Errors
///
/// Returns an error if the scan fails or a staged row is malformed.
#[doc(hidden)]
pub async fn visible_schema_version_rows(
    tx: &StagedTransaction,
) -> Result<Vec<crate::ffi_support::SchemaVersionRow>> {
    let (records, snapshots, data_files) = futures::try_join!(
        tx.visible_schema_version_records(),
        tx.visible_snapshots(),
        tx.visible_data_files(),
    )?;
    let file_snapshots: Vec<(u64, u64)> = data_files
        .iter()
        .map(|file| (file.table_id, file.begin_snapshot))
        .collect();

    Ok(crate::ffi_support::schema_version_rows_from(
        records,
        &snapshots,
        &file_snapshots,
    ))
}
