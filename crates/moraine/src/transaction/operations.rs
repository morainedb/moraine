//! The typed mutation log and conflict classification. Ops are recorded
//! at classification grain: which schema-list entries and which tables a
//! commit touched.

use std::collections::{BTreeMap, BTreeSet};

/// One staged mutation, at the grain conflict classification needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Operation {
    /// A schema was created.
    CreateSchema {
        /// The new schema's id.
        schema_id: u64,
        /// The new schema's name.
        name: String,
    },
    /// A schema was dropped.
    DropSchema {
        /// The dropped schema's id.
        schema_id: u64,
    },
    /// An existing schema's tags changed. Mints a snapshot and bumps the
    /// schema version, but feeds no change-set entry.
    AlterSchema {
        /// The mutated schema's id.
        schema_id: u64,
    },
    /// A table was created.
    CreateTable {
        /// The schema the table was created in.
        schema_id: u64,
        /// The new table's id.
        table_id: u64,
        /// The owning schema's name, serialized as `"schema"."table"`.
        schema_name: String,
        /// The new table's name.
        table_name: String,
    },
    /// An existing table was mutated (rename, move, or column DDL).
    AlterTable {
        /// The mutated table's id.
        table_id: u64,
    },
    /// A table's sort spec was set, changed, or cleared. Classifies as an
    /// alter but is not schema-changing.
    AlterTableSorting {
        /// The re-sorted table's id.
        table_id: u64,
    },
    /// An intermediate staged index-build cursor advance. Classifies as an
    /// insert.
    AdvanceIndexBuild {
        /// The table whose index build advanced.
        table_id: u64,
    },
    /// A deferred repair published complete coverage. Classifies as an
    /// alter but is not schema-changing.
    FinishIndexMaintenance {
        /// The table whose repaired index became ready.
        table_id: u64,
    },
    /// A table was dropped.
    DropTable {
        /// The dropped table's id.
        table_id: u64,
    },
    /// Data was appended to a table.
    RegisterDataFile {
        /// The table rows were inserted into.
        table_id: u64,
    },
    /// Delete markers were appended to a table.
    RegisterDeleteFile {
        /// The table delete markers were appended to.
        table_id: u64,
        /// The data file whose rows they remove.
        data_file_id: u64,
    },
    /// Data file(s) became eligible for garbage collection via merge.
    ExpireDataFile {
        /// The table whose data files were merged.
        table_id: u64,
    },
    /// Delete marker file(s) became eligible for garbage collection.
    ExpireDeleteFile {
        /// The table whose delete marker files were cleaned up.
        table_id: u64,
    },
    /// Table statistics were updated.
    UpdateStats {
        /// The table whose statistics changed.
        table_id: u64,
    },
    /// A view was created.
    CreateView {
        /// The schema the view was created in.
        schema_id: u64,
        /// The new view's id.
        view_id: u64,
        /// The owning schema's name, serialized as `"schema"."view"`.
        schema_name: String,
        /// The new view's name.
        view_name: String,
    },
    /// An existing view was mutated.
    AlterView {
        /// The mutated view's id.
        view_id: u64,
    },
    /// A view was dropped.
    DropView {
        /// The dropped view's id.
        view_id: u64,
    },
    /// A macro was created.
    CreateMacro {
        /// The schema the macro was created in.
        schema_id: u64,
        /// The new macro's id.
        macro_id: u64,
        /// The owning schema's name, serialized as `"schema"."macro"`.
        schema_name: String,
        /// The new macro's name.
        macro_name: String,
        /// `"scalar"` or `"table"` — selects the change-set entry kind.
        macro_type: String,
    },
    /// A macro was dropped.
    DropMacro {
        /// The dropped macro's id.
        macro_id: u64,
        /// `"scalar"` or `"table"` — selects the change-set entry kind.
        macro_type: String,
    },
    /// Rows were inlined into a table. Classifies as an append.
    InlineInsert {
        /// The table rows were inlined into.
        table_id: u64,
    },
    /// Inlined rows were tombstoned. Classifies as a delete.
    InlineDelete {
        /// The table rows were tombstoned in.
        table_id: u64,
    },
    /// Inlined rows were drained to a data file. Classifies as compaction.
    FlushInlinedData {
        /// The table whose inlined rows were drained.
        table_id: u64,
    },
}

impl Operation {
    /// Whether this op changes the catalog's shape and bumps the schema
    /// version.
    pub(crate) fn is_schema_changing(&self) -> bool {
        match self {
            Operation::CreateSchema { .. }
            | Operation::DropSchema { .. }
            | Operation::AlterSchema { .. }
            | Operation::CreateTable { .. }
            | Operation::AlterTable { .. }
            | Operation::DropTable { .. }
            | Operation::CreateView { .. }
            | Operation::AlterView { .. }
            | Operation::DropView { .. }
            | Operation::CreateMacro { .. }
            | Operation::DropMacro { .. } => true,
            Operation::AlterTableSorting { .. }
            | Operation::AdvanceIndexBuild { .. }
            | Operation::FinishIndexMaintenance { .. }
            | Operation::RegisterDataFile { .. }
            | Operation::RegisterDeleteFile { .. }
            | Operation::ExpireDataFile { .. }
            | Operation::ExpireDeleteFile { .. }
            | Operation::UpdateStats { .. }
            | Operation::InlineInsert { .. }
            | Operation::InlineDelete { .. }
            | Operation::FlushInlinedData { .. } => false,
        }
    }

    /// The table or view whose shape this op changes, if any: the ids a
    /// snapshot records as its `ducklake_schema_versions` rows.
    pub(crate) fn schema_changed_table_id(&self) -> Option<u64> {
        match self {
            Operation::CreateTable { table_id, .. } | Operation::AlterTable { table_id } => {
                Some(*table_id)
            }
            Operation::CreateView { view_id, .. } | Operation::AlterView { view_id } => {
                Some(*view_id)
            }
            Operation::CreateSchema { .. }
            | Operation::DropSchema { .. }
            | Operation::AlterSchema { .. }
            | Operation::AlterTableSorting { .. }
            | Operation::AdvanceIndexBuild { .. }
            | Operation::FinishIndexMaintenance { .. }
            | Operation::DropTable { .. }
            | Operation::DropView { .. }
            | Operation::CreateMacro { .. }
            | Operation::DropMacro { .. }
            | Operation::RegisterDataFile { .. }
            | Operation::RegisterDeleteFile { .. }
            | Operation::ExpireDataFile { .. }
            | Operation::ExpireDeleteFile { .. }
            | Operation::UpdateStats { .. }
            | Operation::InlineInsert { .. }
            | Operation::InlineDelete { .. }
            | Operation::FlushInlinedData { .. } => None,
        }
    }
}

/// Wraps `s` in double quotes, doubling any embedded quote.
fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');

    out
}

/// Parses one SQL-quoted identifier at the start of `s`, undoubling
/// embedded quotes; returns the value and the remainder, or `None` if
/// unterminated.
fn parse_quoted(s: &str) -> Option<(String, &str)> {
    let rest = s.strip_prefix('"')?;
    let mut value = String::new();
    let mut chars = rest.char_indices();
    while let Some((i, c)) = chars.next() {
        if c != '"' {
            value.push(c);
            continue;
        }
        if let Some(b'"') = rest.as_bytes().get(i + 1) {
            value.push('"');
            chars.next();
        } else {
            return Some((value, &rest[i + 1..]));
        }
    }

    None
}

/// Parses a fully quoted `created_schema` payload: a single quoted name
/// consuming the entire payload.
fn parse_created_schema_payload(payload: &str) -> Option<String> {
    let (name, rest) = parse_quoted(payload)?;
    rest.is_empty().then_some(name)
}

/// Parses a `created_table` payload: `"schema"."table"`, each name
/// independently quoted and joined by a bare dot.
fn parse_created_table_payload(payload: &str) -> Option<(String, String)> {
    let (schema, rest) = parse_quoted(payload)?;
    let rest = rest.strip_prefix('.')?;
    let (table, rest) = parse_quoted(rest)?;
    rest.is_empty().then_some((schema, table))
}

/// Splits `changes_made` on top-level commas: a `"` toggles an in-quotes
/// flag, and a comma is only an entry separator while the flag is clear.
fn split_entries(changes_made: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (i, c) in changes_made.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                entries.push(&changes_made[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    entries.push(&changes_made[start..]);
    entries.retain(|e| !e.is_empty());

    entries
}

/// What one commit touched, comparable against another commit's set.
/// Serialized into the snapshot record's `changes_made` field in
/// DuckLake's wire grammar: comma-joined `kind:payload` entries, created
/// entries carrying SQL-quoted names and all others numeric ids, e.g.
/// `dropped_schema:5,created_schema:"s1",created_table:"s1"."orders",
/// altered_table:3`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChangeSet {
    /// Names of schemas created by this commit, unquoted.
    pub(crate) created_schemas: BTreeSet<String>,
    pub(crate) dropped_schemas: BTreeSet<u64>,
    /// `(schema name, table name)` pairs, unquoted.
    pub(crate) created_tables: BTreeSet<(String, String)>,
    /// Schema ids a table was created in. Populated only by
    /// [`Self::from_operations`]; the wire grammar carries no ids for
    /// created entries.
    pub(crate) created_table_schema_ids: BTreeSet<u64>,
    pub(crate) altered_tables: BTreeSet<u64>,
    pub(crate) dropped_tables: BTreeSet<u64>,
    /// `(schema name, view name)` pairs, unquoted.
    pub(crate) created_views: BTreeSet<(String, String)>,
    /// Schema ids a view was created in; as
    /// [`Self::created_table_schema_ids`].
    pub(crate) created_view_schema_ids: BTreeSet<u64>,
    pub(crate) altered_views: BTreeSet<u64>,
    pub(crate) dropped_views: BTreeSet<u64>,
    /// `(schema name, macro name)` pairs, unquoted.
    pub(crate) created_scalar_macros: BTreeSet<(String, String)>,
    pub(crate) created_table_macros: BTreeSet<(String, String)>,
    /// Schema ids a macro was created in; as
    /// [`Self::created_table_schema_ids`].
    pub(crate) created_macro_schema_ids: BTreeSet<u64>,
    pub(crate) dropped_scalar_macros: BTreeSet<u64>,
    pub(crate) dropped_table_macros: BTreeSet<u64>,
    /// Tables data was appended to.
    pub(crate) inserted_tables: BTreeSet<u64>,
    /// Tables delete markers were appended to.
    pub(crate) deleted_from_tables: BTreeSet<u64>,
    /// The data files those delete markers target; ids are global.
    pub(crate) deleted_data_file_ids: BTreeSet<u64>,
    /// Set when this commit deletes without naming the file it deletes
    /// from (an inlined delete, or a set parsed from the wire grammar), so
    /// delete-delete falls back to table grain.
    pub(crate) deletes_untargeted_files: bool,
    /// Tables whose data files were merged away.
    pub(crate) merge_adjacent_tables: BTreeSet<u64>,
    /// Tables whose delete files were rewritten away.
    pub(crate) rewrite_delete_tables: BTreeSet<u64>,
    /// Parse-only legacy `compacted_table` kind; classifies as compaction.
    pub(crate) compacted_tables: BTreeSet<u64>,
    /// Parse-only DuckLake inline-flush kind.
    pub(crate) inline_flush_tables: BTreeSet<u64>,
    /// Set when parsing met a kind or payload this binary does not model;
    /// classifies as conflicting.
    pub(crate) has_unknown: bool,
}

impl ChangeSet {
    pub(crate) fn from_operations(operations: &[Operation]) -> Self {
        let mut set = Self::default();
        for op in operations {
            match op {
                Operation::CreateSchema { name, .. } => {
                    set.created_schemas.insert(name.clone());
                }
                Operation::DropSchema { schema_id } => {
                    set.dropped_schemas.insert(*schema_id);
                }
                Operation::CreateTable {
                    schema_id,
                    schema_name,
                    table_name,
                    ..
                } => {
                    set.created_tables
                        .insert((schema_name.clone(), table_name.clone()));
                    set.created_table_schema_ids.insert(*schema_id);
                }
                Operation::AlterTable { table_id }
                | Operation::AlterTableSorting { table_id }
                | Operation::FinishIndexMaintenance { table_id } => {
                    set.altered_tables.insert(*table_id);
                }
                Operation::DropTable { table_id } => {
                    set.dropped_tables.insert(*table_id);
                }
                Operation::RegisterDataFile { table_id }
                | Operation::InlineInsert { table_id }
                | Operation::AdvanceIndexBuild { table_id } => {
                    set.inserted_tables.insert(*table_id);
                }
                Operation::RegisterDeleteFile {
                    table_id,
                    data_file_id,
                } => {
                    set.deleted_from_tables.insert(*table_id);
                    set.deleted_data_file_ids.insert(*data_file_id);
                }
                // An inlined delete names a row, not a file.
                Operation::InlineDelete { table_id } => {
                    set.deleted_from_tables.insert(*table_id);
                    set.deletes_untargeted_files = true;
                }
                Operation::ExpireDataFile { table_id }
                | Operation::FlushInlinedData { table_id } => {
                    set.merge_adjacent_tables.insert(*table_id);
                }
                Operation::ExpireDeleteFile { table_id } => {
                    set.rewrite_delete_tables.insert(*table_id);
                }
                // Neither has a change-set kind; each exists so its commit
                // mints a snapshot.
                Operation::UpdateStats { .. } | Operation::AlterSchema { .. } => {}
                Operation::CreateView {
                    schema_id,
                    schema_name,
                    view_name,
                    ..
                } => {
                    set.created_views
                        .insert((schema_name.clone(), view_name.clone()));
                    set.created_view_schema_ids.insert(*schema_id);
                }
                Operation::AlterView { view_id } => {
                    set.altered_views.insert(*view_id);
                }
                Operation::DropView { view_id } => {
                    set.dropped_views.insert(*view_id);
                }
                Operation::CreateMacro {
                    schema_id,
                    schema_name,
                    macro_name,
                    macro_type,
                    ..
                } => {
                    let pair = (schema_name.clone(), macro_name.clone());
                    if macro_type == "table" {
                        set.created_table_macros.insert(pair);
                    } else {
                        set.created_scalar_macros.insert(pair);
                    }
                    set.created_macro_schema_ids.insert(*schema_id);
                }
                Operation::DropMacro {
                    macro_id,
                    macro_type,
                } => {
                    if macro_type == "table" {
                        set.dropped_table_macros.insert(*macro_id);
                    } else {
                        set.dropped_scalar_macros.insert(*macro_id);
                    }
                }
            }
        }
        set
    }

    /// Emits entries in DuckLake's writer order.
    pub(crate) fn to_changes_made(&self) -> String {
        fn ids(entries: &mut Vec<String>, kind: &str, set: &BTreeSet<u64>) {
            entries.extend(set.iter().map(|id| format!("{kind}:{id}")));
        }
        fn pairs(entries: &mut Vec<String>, kind: &str, set: &BTreeSet<(String, String)>) {
            entries.extend(set.iter().map(|(scope, name)| {
                format!("{kind}:{}.{}", quote_ident(scope), quote_ident(name))
            }));
        }

        let mut entries = Vec::new();
        ids(&mut entries, "dropped_schema", &self.dropped_schemas);
        ids(&mut entries, "dropped_table", &self.dropped_tables);
        ids(&mut entries, "dropped_view", &self.dropped_views);
        entries.extend(
            self.created_schemas
                .iter()
                .map(|name| format!("created_schema:{}", quote_ident(name))),
        );
        pairs(&mut entries, "created_table", &self.created_tables);
        pairs(&mut entries, "created_view", &self.created_views);
        pairs(
            &mut entries,
            "created_scalar_macro",
            &self.created_scalar_macros,
        );
        pairs(
            &mut entries,
            "created_table_macro",
            &self.created_table_macros,
        );
        ids(
            &mut entries,
            "dropped_scalar_macro",
            &self.dropped_scalar_macros,
        );
        ids(
            &mut entries,
            "dropped_table_macro",
            &self.dropped_table_macros,
        );
        ids(&mut entries, "inserted_into_table", &self.inserted_tables);
        ids(
            &mut entries,
            "deleted_from_table",
            &self.deleted_from_tables,
        );
        ids(&mut entries, "altered_table", &self.altered_tables);
        ids(&mut entries, "altered_view", &self.altered_views);
        ids(&mut entries, "merge_adjacent", &self.merge_adjacent_tables);
        ids(&mut entries, "rewrite_delete", &self.rewrite_delete_tables);
        entries.join(",")
    }

    /// Parses a stored `changes_made` string. Kind matching is
    /// case-insensitive.
    pub(crate) fn parse(changes_made: &str) -> Self {
        let mut set = Self::default();
        for entry in split_entries(changes_made) {
            let Some((kind, payload)) = entry.split_once(':') else {
                set.has_unknown = true;
                continue;
            };
            let id = |ids: &mut BTreeSet<u64>| payload.parse().map(|id| ids.insert(id)).is_ok();
            let pair = |pairs: &mut BTreeSet<(String, String)>| {
                parse_created_table_payload(payload)
                    .map(|parsed| pairs.insert(parsed))
                    .is_some()
            };
            let known = match kind.to_ascii_lowercase().as_str() {
                "created_schema" => parse_created_schema_payload(payload)
                    .map(|name| set.created_schemas.insert(name))
                    .is_some(),
                "dropped_schema" => id(&mut set.dropped_schemas),
                "created_table" => pair(&mut set.created_tables),
                "altered_table" => id(&mut set.altered_tables),
                "dropped_table" => id(&mut set.dropped_tables),
                "created_view" => pair(&mut set.created_views),
                "altered_view" => id(&mut set.altered_views),
                "dropped_view" => id(&mut set.dropped_views),
                "created_scalar_macro" => pair(&mut set.created_scalar_macros),
                "created_table_macro" => pair(&mut set.created_table_macros),
                "dropped_scalar_macro" => id(&mut set.dropped_scalar_macros),
                "dropped_table_macro" => id(&mut set.dropped_table_macros),
                "inserted_into_table" => id(&mut set.inserted_tables),
                // The grammar names the table, never the files; the caller
                // may supply file ids after parsing.
                "deleted_from_table" => {
                    set.deletes_untargeted_files = true;
                    id(&mut set.deleted_from_tables)
                }
                "merge_adjacent" => id(&mut set.merge_adjacent_tables),
                "rewrite_delete" => id(&mut set.rewrite_delete_tables),
                "compacted_table" => id(&mut set.compacted_tables),
                "inline_flush" | "flushed_inlined" => id(&mut set.inline_flush_tables),
                _ => false,
            };

            if !known {
                set.has_unknown = true;
            }
        }

        set
    }

    /// True when the set records no changes at all.
    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The tables this commit compacts, when compaction is all it does:
    /// such a commit only re-homes rows. Empty when the set carries
    /// anything else, including a kind this binary does not model.
    pub(crate) fn compaction_only_tables(&self) -> BTreeSet<u64> {
        // Taken out of a clone so whatever remains must equal the default.
        let mut rest = self.clone();
        let compacted: BTreeSet<u64> = std::mem::take(&mut rest.merge_adjacent_tables)
            .into_iter()
            .chain(std::mem::take(&mut rest.rewrite_delete_tables))
            .chain(std::mem::take(&mut rest.compacted_tables))
            .collect();

        let inline_flushed = std::mem::take(&mut rest.inline_flush_tables);
        let flush_deletes_are_mechanical = !inline_flushed.is_empty()
            && rest.deleted_from_tables.is_subset(&inline_flushed)
            && rest.deleted_data_file_ids.is_empty();
        let compacted = if flush_deletes_are_mechanical {
            rest.deleted_from_tables.clear();
            rest.deletes_untargeted_files = false;
            compacted.into_iter().chain(inline_flushed).collect()
        } else {
            rest.inline_flush_tables = inline_flushed;
            compacted
        };

        if rest.is_empty() {
            compacted
        } else {
            BTreeSet::new()
        }
    }

    fn touches_schema_list(&self) -> bool {
        !self.created_schemas.is_empty() || !self.dropped_schemas.is_empty()
    }

    fn creates_table_in(&self, schema_id: u64) -> bool {
        self.created_table_schema_ids.contains(&schema_id)
            || self.created_view_schema_ids.contains(&schema_id)
            || self.created_macro_schema_ids.contains(&schema_id)
    }

    fn table_kinds(&self) -> BTreeMap<u64, TableKinds> {
        let mut kinds: BTreeMap<u64, TableKinds> = BTreeMap::new();
        for &table_id in &self.inserted_tables {
            kinds.entry(table_id).or_default().inserted = true;
        }
        for &table_id in &self.deleted_from_tables {
            kinds.entry(table_id).or_default().deleted = true;
        }
        for &table_id in self.altered_tables.iter().chain(self.altered_views.iter()) {
            kinds.entry(table_id).or_default().altered = true;
        }
        for &table_id in self.dropped_tables.iter().chain(self.dropped_views.iter()) {
            kinds.entry(table_id).or_default().dropped = true;
        }
        for &table_id in self
            .merge_adjacent_tables
            .iter()
            .chain(self.rewrite_delete_tables.iter())
            .chain(self.compacted_tables.iter())
            .chain(self.inline_flush_tables.iter())
        {
            kinds.entry(table_id).or_default().compacted = true;
        }

        kinds
    }
}

#[derive(Default, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct TableKinds {
    inserted: bool,
    deleted: bool,
    altered: bool,
    dropped: bool,
    compacted: bool,
}

/// DuckLake's per-table conflict matrix, symmetric closure.
/// Delete-versus-delete is decided by [`delete_delete_conflicts`].
fn kinds_conflict(a: TableKinds, b: TableKinds) -> bool {
    let one_way = |x: TableKinds, y: TableKinds| {
        (x.inserted && (y.altered || y.deleted || y.dropped))
            || (x.deleted && (y.altered || y.compacted || y.dropped || y.inserted))
            || (x.altered && (y.altered || y.dropped))
            || (x.compacted && (y.deleted || y.dropped || y.compacted))
    };

    one_way(a, b) || one_way(b, a)
}

/// Whether two concurrent deletes of one table conflict: at data-file
/// grain when both sides name their files, else at table grain.
fn delete_delete_conflicts(ours: &ChangeSet, theirs: &ChangeSet) -> bool {
    ours.deletes_untargeted_files
        || theirs.deletes_untargeted_files
        || !ours
            .deleted_data_file_ids
            .is_disjoint(&theirs.deleted_data_file_ids)
}

/// Whether two concurrent commits are a true conflict. Symmetric. Benign
/// unless: either side has unknown changes; both touch the schema list; a
/// common table has incompatible kinds, or deletes on both sides that meet
/// on a file; or one created a relation inside a schema the other dropped.
pub(crate) fn conflicts(ours: &ChangeSet, theirs: &ChangeSet) -> bool {
    if ours.has_unknown || theirs.has_unknown {
        return true;
    }
    if ours.touches_schema_list() && theirs.touches_schema_list() {
        return true;
    }

    let our_kinds = ours.table_kinds();
    let their_kinds = theirs.table_kinds();
    for (&table_id, &our_table_kinds) in &our_kinds {
        if let Some(&their_table_kinds) = their_kinds.get(&table_id) {
            if kinds_conflict(our_table_kinds, their_table_kinds) {
                return true;
            }
            if our_table_kinds.deleted
                && their_table_kinds.deleted
                && delete_delete_conflicts(ours, theirs)
            {
                return true;
            }
        }
    }

    let created_in_dropped =
        |a: &ChangeSet, b: &ChangeSet| b.dropped_schemas.iter().any(|s| a.creates_table_in(*s));
    created_in_dropped(ours, theirs) || created_in_dropped(theirs, ours)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{ObjectStore, memory::InMemory};
    use proptest::prelude::*;

    use super::*;
    use crate::{
        catalog::{Catalog, CatalogOptions, CatalogSnapshot, ColumnDef, SchemaId, TableId},
        error::Error,
        transaction::verbs::Transaction,
    };

    fn create_schema(schema_id: u64, name: &str) -> Operation {
        Operation::CreateSchema {
            schema_id,
            name: name.to_owned(),
        }
    }

    fn create_table(
        schema_id: u64,
        table_id: u64,
        schema_name: &str,
        table_name: &str,
    ) -> Operation {
        Operation::CreateTable {
            schema_id,
            table_id,
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
        }
    }

    #[test]
    fn data_plane_ops_serialize_in_ducklake_order() {
        let ops = [
            create_schema(1, "s1"),
            Operation::RegisterDataFile { table_id: 7 },
            Operation::RegisterDeleteFile {
                table_id: 8,
                data_file_id: 80,
            },
            Operation::ExpireDataFile { table_id: 9 },
            Operation::ExpireDeleteFile { table_id: 9 },
            Operation::AlterTable { table_id: 3 },
            Operation::UpdateStats { table_id: 7 },
        ];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(
            set.to_changes_made(),
            r#"created_schema:"s1",inserted_into_table:7,deleted_from_table:8,altered_table:3,merge_adjacent:9,rewrite_delete:9"#
        );
        // The round trip is through DuckLake's grammar, which carries
        // neither the schema a table was created in nor the file a delete
        // targeted — so both come back absent, and the delete comes back
        // untargeted.
        assert_eq!(ChangeSet::parse(&set.to_changes_made()), {
            let mut expect = set.clone();
            expect.created_table_schema_ids.clear();
            expect.deleted_data_file_ids.clear();
            expect.deletes_untargeted_files = true;
            expect
        });
    }

    #[test]
    fn compaction_only_tables_names_a_pure_compaction() {
        let set = ChangeSet::parse("merge_adjacent:9,rewrite_delete:4");
        assert_eq!(set.compaction_only_tables(), BTreeSet::from([4, 9]));
        // The legacy kind classifies with them.
        let legacy = ChangeSet::parse("compacted_table:9");
        assert_eq!(legacy.compaction_only_tables(), BTreeSet::from([9]));
    }

    /// Moving inline rows and their existing tombstones into files preserves
    /// every indexed row id and value. DuckLake reports the tombstone move as
    /// an ordinary table delete beside the flush marker; together they are
    /// still re-homing only.
    #[test]
    fn compaction_only_tables_names_an_inline_flush() {
        let set = ChangeSet::parse(
            "deleted_from_table:41,deleted_from_table:43,deleted_from_table:55,\
             inline_flush:19,inline_flush:41,inline_flush:43,inline_flush:55",
        );

        assert_eq!(
            set.compaction_only_tables(),
            BTreeSet::from([19, 41, 43, 55])
        );
    }

    /// DuckLake never mixes the two, so a set that does is drift — and
    /// the caller must do the work it would otherwise skip.
    #[test]
    fn compaction_only_tables_is_empty_when_anything_else_changed() {
        for changes in [
            "merge_adjacent:9,inserted_into_table:9",
            "merge_adjacent:9,deleted_from_table:4",
            "merge_adjacent:9,altered_table:9",
            "inline_flush:9,inserted_into_table:9",
            "inline_flush:9,deleted_from_table:4",
            "",
        ] {
            assert!(
                ChangeSet::parse(changes)
                    .compaction_only_tables()
                    .is_empty(),
                "{changes:?} must not name a compaction-only table"
            );
        }
    }

    #[test]
    fn stats_ops_emit_nothing_but_are_ops() {
        let ops = [Operation::UpdateStats { table_id: 7 }];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(set.to_changes_made(), "");
        assert!(!ops[0].is_schema_changing());
        // An empty change set conflicts with nothing.
        let drop = ChangeSet::from_operations(&[Operation::DropTable { table_id: 7 }]);
        assert!(!conflicts(&set, &drop));
    }

    #[test]
    fn append_append_is_benign() {
        let a = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        let b = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        assert!(!conflicts(&a, &b));
        // Appends are also benign against compactions of the same table.
        let c = ChangeSet::from_operations(&[Operation::ExpireDataFile { table_id: 1 }]);
        assert!(!conflicts(&a, &c));
    }

    /// Delete-versus-delete is the one pair DuckLake checks below table
    /// grain, and moraine matches it: two commits deleting from the same
    /// table are benign when they targeted different data files, and a
    /// conflict when they targeted the same one.
    #[test]
    fn delete_delete_is_checked_at_file_grain() {
        let delete = |table_id, data_file_id| {
            ChangeSet::from_operations(&[Operation::RegisterDeleteFile {
                table_id,
                data_file_id,
            }])
        };

        assert!(!conflicts(&delete(1, 10), &delete(1, 11)));
        assert!(conflicts(&delete(1, 10), &delete(1, 10)));

        // A commit deleting from several files conflicts on any shared one.
        let several = ChangeSet::from_operations(&[
            Operation::RegisterDeleteFile {
                table_id: 1,
                data_file_id: 10,
            },
            Operation::RegisterDeleteFile {
                table_id: 1,
                data_file_id: 11,
            },
        ]);
        assert!(conflicts(&several, &delete(1, 11)));
        assert!(!conflicts(&several, &delete(1, 12)));

        // An inlined delete names a row, not a file, so it has no grain to
        // be refined at and falls back to the whole table.
        let inlined = ChangeSet::from_operations(&[Operation::InlineDelete { table_id: 1 }]);
        assert!(conflicts(&inlined, &delete(1, 10)));
        assert!(conflicts(&inlined, &inlined));

        // A DuckLake-authored delete arrives through the grammar, which
        // names the table only — so it conflicts with any delete of it,
        // however the file sets actually stand.
        let theirs = ChangeSet::parse("deleted_from_table:1");
        assert!(!theirs.has_unknown);
        assert!(conflicts(&delete(1, 10), &theirs));
        // Still only that table, though.
        assert!(!conflicts(&delete(2, 10), &theirs));
    }

    #[test]
    fn the_conflict_matrix() {
        let insert = |t| ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: t }]);
        // One file per table here, so same-table deletes meet on it and the
        // matrix's table-grain expectations hold as written.
        let delete = |t| {
            ChangeSet::from_operations(&[Operation::RegisterDeleteFile {
                table_id: t,
                data_file_id: t * 100,
            }])
        };
        let alter = |t| ChangeSet::from_operations(&[Operation::AlterTable { table_id: t }]);
        let drop = |t| ChangeSet::from_operations(&[Operation::DropTable { table_id: t }]);
        let compact =
            |t| ChangeSet::from_operations(&[Operation::ExpireDeleteFile { table_id: t }]);

        // Conflicting pairs.
        assert!(conflicts(&insert(1), &alter(1)));
        assert!(conflicts(&insert(1), &delete(1)));
        assert!(conflicts(&insert(1), &drop(1)));
        assert!(conflicts(&delete(1), &delete(1)));
        assert!(conflicts(&delete(1), &compact(1)));
        assert!(conflicts(&delete(1), &alter(1)));
        assert!(conflicts(&delete(1), &drop(1)));
        assert!(conflicts(&alter(1), &alter(1)));
        assert!(conflicts(&alter(1), &drop(1)));
        assert!(conflicts(&compact(1), &compact(1)));
        assert!(conflicts(&compact(1), &drop(1)));
        // Benign pairs.
        assert!(!conflicts(&insert(1), &insert(1)));
        assert!(!conflicts(&insert(1), &compact(1)));
        assert!(!conflicts(&alter(1), &compact(1)));
        assert!(!conflicts(&drop(1), &drop(1)));
        // Different tables never conflict.
        assert!(!conflicts(&delete(1), &delete(2)));
    }

    #[test]
    fn parsed_compaction_kinds_classify_as_compaction() {
        let theirs = ChangeSet::parse("compacted_table:1");
        assert!(!theirs.has_unknown);
        let ours = ChangeSet::from_operations(&[Operation::RegisterDeleteFile {
            table_id: 1,
            data_file_id: 100,
        }]);
        assert!(conflicts(&ours, &theirs));
        let benign = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        assert!(!conflicts(&benign, &theirs));
    }

    /// A DuckLake-authored append — an ordinary insert, or the inline
    /// flush that turns staged rows into a file — reaches the classifier
    /// only as a parsed `inserted_into_table` entry. It must classify like
    /// one of ours: benign against our appends and against a compaction of
    /// the same table, a conflict against a delete, alter, or drop of it.
    #[test]
    fn a_parsed_insert_classifies_like_an_append() {
        let theirs = ChangeSet::parse("inserted_into_table:1");
        assert!(!theirs.has_unknown);

        let append = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        let compaction = ChangeSet::from_operations(&[Operation::ExpireDataFile { table_id: 1 }]);
        assert!(!conflicts(&append, &theirs));
        assert!(!conflicts(&compaction, &theirs));

        for ours in [
            ChangeSet::from_operations(&[Operation::RegisterDeleteFile {
                table_id: 1,
                data_file_id: 100,
            }]),
            ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]),
            ChangeSet::from_operations(&[Operation::DropTable { table_id: 1 }]),
        ] {
            assert!(conflicts(&ours, &theirs));
        }
    }

    #[test]
    fn changes_made_exact_serialization() {
        let ops = [
            create_schema(1, "s1"),
            create_table(1, 2, "s1", "orders"),
            Operation::AlterTable { table_id: 3 },
            Operation::DropTable { table_id: 4 },
            Operation::DropSchema { schema_id: 5 },
        ];
        let set = ChangeSet::from_operations(&ops);
        let text = set.to_changes_made();
        assert_eq!(
            text,
            r#"dropped_schema:5,dropped_table:4,created_schema:"s1",created_table:"s1"."orders",altered_table:3"#
        );
    }

    #[test]
    fn round_trip_clears_created_table_schema_ids() {
        let ops = [
            create_schema(1, "s1"),
            create_table(1, 2, "s1", "orders"),
            Operation::AlterTable { table_id: 3 },
            Operation::DropTable { table_id: 4 },
            Operation::DropSchema { schema_id: 5 },
        ];
        let set = ChangeSet::from_operations(&ops);
        let text = set.to_changes_made();
        let parsed = ChangeSet::parse(&text);
        let expected = ChangeSet {
            created_table_schema_ids: BTreeSet::new(),
            ..set
        };
        assert_eq!(parsed, expected);
        assert_eq!(ChangeSet::parse(""), ChangeSet::default());
    }

    #[test]
    fn quoting_edges_round_trip() {
        // Names containing a comma, a dot, and an embedded quote must all
        // round-trip through the quoted grammar unchanged.
        let ops = [create_schema(1, "s,1"), create_table(2, 3, "a.b", r#"c"d"#)];
        let set = ChangeSet::from_operations(&ops);
        let text = set.to_changes_made();
        assert_eq!(text, r#"created_schema:"s,1",created_table:"a.b"."c""d""#);
        let parsed = ChangeSet::parse(&text);
        assert_eq!(parsed.created_schemas, set.created_schemas);
        assert_eq!(parsed.created_tables, set.created_tables);
        assert!(!parsed.has_unknown);
    }

    #[test]
    fn kind_matching_is_case_insensitive() {
        let parsed = ChangeSet::parse(r#"CREATED_SCHEMA:"x""#);
        assert!(!parsed.has_unknown);
        assert!(parsed.created_schemas.contains("x"));
    }

    #[test]
    fn inline_flush_is_a_known_ducklake_kind() {
        let parsed = ChangeSet::parse("inline_flush:7");
        assert!(!parsed.has_unknown);
        assert_eq!(parsed.compaction_only_tables(), BTreeSet::from([7]));
    }

    #[test]
    fn unknown_entries_are_conservative() {
        let parsed = ChangeSet::parse("flushed_inline:7");
        assert!(parsed.has_unknown);
        assert!(conflicts(&ChangeSet::default(), &parsed));
    }

    #[test]
    fn malformed_payload_is_unknown() {
        // A created_schema payload missing the closing quote is malformed.
        let parsed = ChangeSet::parse(r#"created_schema:"unterminated"#);
        assert!(parsed.has_unknown);
        // A dropped_table payload that is not numeric is malformed.
        let parsed = ChangeSet::parse("dropped_table:not_a_number");
        assert!(parsed.has_unknown);
    }

    #[test]
    fn disjoint_tables_are_benign() {
        let ours = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        let theirs = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 2 }]);
        assert!(!conflicts(&ours, &theirs));
    }

    #[test]
    fn overlapping_tables_conflict() {
        let ours = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        let dropped = ChangeSet::from_operations(&[Operation::DropTable { table_id: 1 }]);
        assert!(conflicts(&ours, &dropped));
        assert!(conflicts(&dropped, &ours));
    }

    #[test]
    fn schema_list_is_coarse_grained() {
        let create = ChangeSet::from_operations(&[create_schema(1, "s1")]);
        let drop = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 9 }]);
        assert!(conflicts(&create, &drop));
        // A table-only commit does not touch the schema list.
        let alter = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        assert!(!conflicts(&alter, &drop));
    }

    #[test]
    fn create_inside_dropped_schema_conflicts() {
        let ours = ChangeSet::from_operations(&[create_table(3, 8, "s3", "t8")]);
        let theirs = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        assert!(conflicts(&ours, &theirs));
        assert!(conflicts(&theirs, &ours));
        // Creation in a surviving schema is benign.
        let elsewhere = ChangeSet::from_operations(&[create_table(4, 9, "s4", "t9")]);
        assert!(!conflicts(&elsewhere, &theirs));
    }

    #[test]
    fn parsed_created_table_schema_ids_stay_empty() {
        // The wire grammar carries names, not schema ids, for created
        // entries, so a parsed ChangeSet can never populate
        // created_table_schema_ids — the create-inside-dropped-schema
        // check is a from_ops-only capability for our own side.
        let ours = ChangeSet::from_operations(&[create_table(3, 8, "s3", "t8")]);
        let parsed = ChangeSet::parse(&ours.to_changes_made());
        assert!(parsed.created_table_schema_ids.is_empty());
        let theirs = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        // The parsed side can no longer detect its own creation inside
        // the dropped schema by this mechanism; the closure re-run on
        // retry covers that risk instead (see the field's doc comment).
        assert!(!conflicts(&parsed, &theirs));
    }

    #[test]
    fn fresh_created_tables_never_conflict_by_id() {
        let a = ChangeSet::from_operations(&[create_table(1, 7, "s1", "t7")]);
        let b = ChangeSet::from_operations(&[create_table(1, 7, "s1", "t7")]);
        // Same ids cannot happen for real (ids are allocated above head),
        // but creation is not a mutation of existing state either way.
        assert!(!conflicts(&a, &b));
    }

    #[test]
    fn view_ops_serialize_and_round_trip() {
        let ops = [
            Operation::CreateView {
                schema_id: 1,
                view_id: 4,
                schema_name: "s".into(),
                view_name: "v".into(),
            },
            Operation::DropView { view_id: 5 },
            Operation::AlterView { view_id: 6 },
        ];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(
            set.to_changes_made(),
            r#"dropped_view:5,created_view:"s"."v",altered_view:6"#
        );
        assert_eq!(ChangeSet::parse(&set.to_changes_made()), {
            let mut e = set.clone();
            e.created_view_schema_ids.clear();
            e.created_table_schema_ids.clear();
            e
        });
        assert!(ops.iter().all(Operation::is_schema_changing));
    }

    #[test]
    fn macro_ops_serialize_and_round_trip() {
        let ops = [
            Operation::CreateMacro {
                schema_id: 1,
                macro_id: 4,
                schema_name: "s".into(),
                macro_name: "m".into(),
                macro_type: "scalar".into(),
            },
            Operation::CreateMacro {
                schema_id: 1,
                macro_id: 5,
                schema_name: "s".into(),
                macro_name: "tm".into(),
                macro_type: "table".into(),
            },
            Operation::DropMacro {
                macro_id: 6,
                macro_type: "scalar".into(),
            },
            Operation::DropMacro {
                macro_id: 7,
                macro_type: "table".into(),
            },
        ];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(
            set.to_changes_made(),
            r#"created_scalar_macro:"s"."m",created_table_macro:"s"."tm",dropped_scalar_macro:6,dropped_table_macro:7"#
        );
        assert_eq!(ChangeSet::parse(&set.to_changes_made()), {
            let mut e = set.clone();
            e.created_macro_schema_ids.clear();
            e
        });
        assert!(ops.iter().all(Operation::is_schema_changing));
    }

    #[test]
    fn macro_conflicts_classify_like_views() {
        // Two drops of one macro classify benign: like tables and views,
        // the loser's closure re-run sees the macro gone and surfaces
        // NotFound — set comparison never has to catch it.
        let drop = ChangeSet::from_operations(&[Operation::DropMacro {
            macro_id: 9,
            macro_type: "scalar".into(),
        }]);
        assert!(!conflicts(&drop, &drop));
        // Creating a macro inside a schema another commit dropped conflicts.
        let create = ChangeSet::from_operations(&[Operation::CreateMacro {
            schema_id: 3,
            macro_id: 7,
            schema_name: "s".into(),
            macro_name: "m".into(),
            macro_type: "scalar".into(),
        }]);
        let drop_schema = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        assert!(conflicts(&create, &drop_schema));
        assert!(conflicts(&drop_schema, &create));
    }

    #[test]
    fn view_conflicts_classify_at_id_grain() {
        let alter = ChangeSet::from_operations(&[Operation::AlterView { view_id: 9 }]);
        let drop = ChangeSet::from_operations(&[Operation::DropView { view_id: 9 }]);
        assert!(conflicts(&alter, &drop));
        assert!(conflicts(&alter, &alter));
        let other = ChangeSet::from_operations(&[Operation::AlterView { view_id: 8 }]);
        assert!(!conflicts(&alter, &other));
        // Creating a view inside a schema another commit dropped conflicts.
        let create = ChangeSet::from_operations(&[Operation::CreateView {
            schema_id: 3,
            view_id: 7,
            schema_name: "s".into(),
            view_name: "v".into(),
        }]);
        let drop_schema = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        assert!(conflicts(&create, &drop_schema));
    }

    #[test]
    fn ddl_ops_are_schema_changing() {
        assert!(create_schema(0, "s").is_schema_changing());
        assert!(Operation::AlterTable { table_id: 0 }.is_schema_changing());
        assert!(Operation::DropTable { table_id: 0 }.is_schema_changing());
    }

    #[test]
    fn data_plane_ops_are_not_schema_changing() {
        assert!(!Operation::RegisterDataFile { table_id: 0 }.is_schema_changing());
        assert!(
            !Operation::RegisterDeleteFile {
                table_id: 0,
                data_file_id: 0
            }
            .is_schema_changing()
        );
        assert!(!Operation::ExpireDataFile { table_id: 0 }.is_schema_changing());
        assert!(!Operation::ExpireDeleteFile { table_id: 0 }.is_schema_changing());
        assert!(!Operation::UpdateStats { table_id: 0 }.is_schema_changing());
    }

    fn any_change_set() -> impl Strategy<Value = ChangeSet> {
        let names = || proptest::collection::btree_set("[a-c]", 0..3);
        let ids = || proptest::collection::btree_set(0u64..4, 0..3);
        (
            names(),
            ids(),
            ids(),
            ids(),
            ids(),
            ids(),
            ids(),
            ids(),
            any::<bool>(),
        )
            .prop_map(
                |(
                    created_schemas,
                    dropped_schemas,
                    altered,
                    dropped,
                    inserted,
                    deleted,
                    merged,
                    created_in,
                    unknown,
                )| {
                    ChangeSet {
                        created_schemas,
                        dropped_schemas,
                        altered_tables: altered,
                        dropped_tables: dropped,
                        inserted_tables: inserted,
                        deleted_from_tables: deleted,
                        merge_adjacent_tables: merged,
                        created_table_schema_ids: created_in,
                        has_unknown: unknown,
                        ..ChangeSet::default()
                    }
                },
            )
    }

    proptest! {
        /// The conflict relation is symmetric: an asymmetric classifier would
        /// make a race's outcome depend on who lost it.
        #[test]
        fn conflicts_is_symmetric(a in any_change_set(), b in any_change_set()) {
            prop_assert_eq!(conflicts(&a, &b), conflicts(&b, &a));
        }
    }

    /// One catalog action, applied inside a commit closure and checked for its
    /// effect afterwards. Names carry a side tag so the two commits of a pair
    /// never collide on a name — a same-name collision is closure-re-validated,
    /// not a classifier concern.
    #[derive(Debug, Clone)]
    enum Action {
        NewSchema(u16),
        NewTable(u16),
        AddColumnA(u16),
        AddColumnB(u16),
        DropTableA,
        DropTableB,
    }

    struct Ids {
        schema: SchemaId,
        a: TableId,
        b: TableId,
    }

    fn a_column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: "BIGINT".into(),
            nulls_allowed: true,
            default_value: None,
            children: Vec::new(),
        }
    }

    impl Action {
        fn apply(&self, tx: &mut Transaction, ids: &Ids, side: &str) -> crate::error::Result<()> {
            match self {
                Action::NewSchema(k) => tx.create_schema(&format!("{side}s{k}")).map(|_| ()),
                Action::NewTable(k) => tx
                    .create_table(ids.schema, &format!("{side}t{k}"), &[a_column("x")])
                    .map(|_| ()),
                Action::AddColumnA(k) => tx
                    .add_column(ids.a, &a_column(&format!("{side}ac{k}")))
                    .map(|_| ()),
                Action::AddColumnB(k) => tx
                    .add_column(ids.b, &a_column(&format!("{side}bc{k}")))
                    .map(|_| ()),
                Action::DropTableA => tx.drop_table(ids.a),
                Action::DropTableB => tx.drop_table(ids.b),
            }
        }

        fn effect_present(&self, head: &CatalogSnapshot, ids: &Ids, side: &str) -> bool {
            let table_live = |id: TableId| head.tables_in(ids.schema).iter().any(|t| t.id == id);
            match self {
                Action::NewSchema(k) => head.schema_by_name(&format!("{side}s{k}")).is_some(),
                Action::NewTable(k) => head
                    .table_by_name(ids.schema, &format!("{side}t{k}"))
                    .is_some(),
                Action::AddColumnA(k) => {
                    table_live(ids.a)
                        && head
                            .columns_of(ids.a)
                            .iter()
                            .any(|c| c.name == format!("{side}ac{k}"))
                }
                Action::AddColumnB(k) => {
                    table_live(ids.b)
                        && head
                            .columns_of(ids.b)
                            .iter()
                            .any(|c| c.name == format!("{side}bc{k}"))
                }
                Action::DropTableA => !table_live(ids.a),
                Action::DropTableB => !table_live(ids.b),
            }
        }
    }

    fn any_action() -> impl Strategy<Value = Action> {
        prop_oneof![
            (0u16..3).prop_map(Action::NewSchema),
            (0u16..3).prop_map(Action::NewTable),
            (0u16..3).prop_map(Action::AddColumnA),
            (0u16..3).prop_map(Action::AddColumnB),
            Just(Action::DropTableA),
            Just(Action::DropTableB),
        ]
    }

    #[allow(clippy::unwrap_used)]
    async fn seeded() -> (Catalog, Ids) {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let options = CatalogOptions::default();
        let catalog = Catalog::open(store, options).await.unwrap();
        catalog
            .commit(|tx| {
                let schema = tx.create_schema("s")?;
                tx.create_table(schema, "a", &[a_column("x")])?;
                tx.create_table(schema, "b", &[a_column("x")])?;
                Ok(())
            })
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        let schema = snapshot.schema_by_name("s").unwrap().id;
        let a = snapshot.table_by_name(schema, "a").unwrap().id;
        let b = snapshot.table_by_name(schema, "b").unwrap().id;
        (catalog, Ids { schema, a, b })
    }

    /// The change set an action stages against the seeded head, read from the
    /// operations it records in a throwaway transaction.
    #[allow(clippy::unwrap_used)]
    async fn change_set_of(catalog: &Catalog, action: &Action, ids: &Ids) -> ChangeSet {
        let base = catalog.snapshot().await.unwrap();
        let next = base.snapshot.snapshot_id + 1;
        let mut tx = Transaction::new((*base).clone(), next);
        let _ = action.apply(&mut tx, ids, "probe");
        ChangeSet::from_operations(&tx.into_parts().operations)
    }

    /// The judgment the protocol defers to moraine, proven where its violation
    /// silently loses writes: a pair the classifier calls benign must commute
    /// through two real slot commits — the second re-assembles against a head
    /// already holding the first, and no committed effect is ever lost. A
    /// wrongly *permissive* classifier that waves through an interfering pair
    /// (say alter-then-drop of one table) makes the winner's effect vanish
    /// while both commits report success — which this assertion catches, and no
    /// racing test can.
    #[allow(clippy::unwrap_used)]
    async fn benign_pair_commutes(x: &Action, y: &Action) -> Result<(), TestCaseError> {
        let (catalog, ids) = seeded().await;
        let cs_x = change_set_of(&catalog, x, &ids).await;
        let cs_y = change_set_of(&catalog, y, &ids).await;

        prop_assert_eq!(conflicts(&cs_x, &cs_y), conflicts(&cs_y, &cs_x));
        prop_assume!(!conflicts(&cs_x, &cs_y));

        // The first commit always lands against the seeded head.
        let landed = catalog.commit(|tx| x.apply(tx, &ids, "x")).await;
        prop_assert!(landed.is_ok(), "first commit failed: {:?}", landed);

        // The second re-assembles against a head that already contains the
        // first, and rebases if it lost the slot.
        let rebased = catalog.commit(|tx| y.apply(tx, &ids, "y")).await;

        let head = catalog.snapshot().await.unwrap();
        prop_assert!(
            x.effect_present(&head, &ids, "x"),
            "the first commit's effect was silently lost: x={:?} y={:?}",
            x,
            y
        );
        match rebased {
            Ok(_) => prop_assert!(
                y.effect_present(&head, &ids, "y"),
                "the second commit reported success but its effect is absent: x={:?} y={:?}",
                x,
                y
            ),
            // A benign loser may still fail its own re-validation typed (a drop
            // of an entity the winner already removed surfaces NotFound).
            Err(
                Error::CommitConflict(_)
                | Error::NotFound(_)
                | Error::AlreadyExists(_)
                | Error::Constraint(_),
            ) => {}
            Err(other) => prop_assert!(false, "loser failed untyped: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn benign_pairs_commute_through_two_slot_commits() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        proptest!(
            ProptestConfig::with_cases(48),
            |(x in any_action(), y in any_action())| {
                runtime.block_on(benign_pair_commutes(&x, &y))?;
            }
        );
    }

    /// A sort change is the one table alter that is not schema-changing,
    /// and it still conflicts with everything an alter conflicts with.
    #[test]
    fn a_sort_change_alters_the_table_without_changing_its_schema() {
        let sorting = Operation::AlterTableSorting { table_id: 1 };
        assert!(!sorting.is_schema_changing());
        assert_eq!(sorting.schema_changed_table_id(), None);

        let sorted = ChangeSet::from_operations(&[sorting]);
        let dropped = ChangeSet::from_operations(&[Operation::DropTable { table_id: 1 }]);
        let altered = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        assert!(conflicts(&sorted, &dropped));
        assert!(conflicts(&sorted, &altered));
        assert!(conflicts(&sorted, &sorted));

        // An append conflicts with it, as with any alter — DuckLake's
        // matrix, not a rule sorting gets to soften.
        let appended = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        assert!(conflicts(&sorted, &appended));

        let elsewhere = ChangeSet::from_operations(&[Operation::AlterTableSorting { table_id: 2 }]);
        assert!(!conflicts(&sorted, &elsewhere));
    }

    /// A staged index cursor advance classifies like an append: another
    /// append is benign, while deletes, alters, and drops still conflict.
    #[test]
    fn an_index_build_advance_inserts_without_changing_the_schema() {
        let advance = Operation::AdvanceIndexBuild { table_id: 1 };
        assert!(!advance.is_schema_changing());
        assert_eq!(advance.schema_changed_table_id(), None);

        let advanced = ChangeSet::from_operations(&[advance]);
        assert_eq!(advanced.to_changes_made(), "inserted_into_table:1");
        let dropped = ChangeSet::from_operations(&[Operation::DropTable { table_id: 1 }]);
        let altered = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        let appended = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        let deleted = ChangeSet::from_operations(&[Operation::RegisterDeleteFile {
            table_id: 1,
            data_file_id: 10,
        }]);
        assert!(conflicts(&advanced, &dropped));
        assert!(conflicts(&advanced, &altered));
        assert!(conflicts(&advanced, &deleted));
        assert!(!conflicts(&advanced, &advanced));
        assert!(!conflicts(&advanced, &appended));

        let elsewhere = ChangeSet::from_operations(&[Operation::AdvanceIndexBuild { table_id: 2 }]);
        assert!(!conflicts(&advanced, &elsewhere));
    }

    /// Publishing a deferred repair remains an alter-classified fence: a
    /// writer that began while the index was maintaining must re-run and add
    /// its entries before the definition can stay ready.
    #[test]
    fn finishing_index_maintenance_alters_without_changing_the_schema() {
        let finish = Operation::FinishIndexMaintenance { table_id: 1 };
        assert!(!finish.is_schema_changing());
        assert_eq!(finish.schema_changed_table_id(), None);

        let finished = ChangeSet::from_operations(&[finish]);
        assert_eq!(finished.to_changes_made(), "altered_table:1");
        for racing in [
            ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]),
            ChangeSet::from_operations(&[Operation::RegisterDeleteFile {
                table_id: 1,
                data_file_id: 10,
            }]),
            ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]),
            ChangeSet::from_operations(&[Operation::DropTable { table_id: 1 }]),
        ] {
            assert!(conflicts(&finished, &racing));
        }
    }
}
