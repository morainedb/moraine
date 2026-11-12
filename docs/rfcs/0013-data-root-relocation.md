# Data root relocation

A copied lake retains its original global `data_path`. Relocation changes
that root explicitly, after the operator has copied the data and stopped
all access to the destination catalog.

## API

`Catalog::rewrite_data_path(old_root, new_root)` checks the recorded global
root and replaces it in one scoped option commit. The comparison ignores
trailing slashes. Missing or mismatched old roots and empty roots fail without
changing metadata. The method returns the committed snapshot ID.

Relative schema, table, data-file, and delete-file paths continue to resolve
under the global root. Absolute paths remain unchanged, including imported
files outside the data root. Historical snapshots use the relocated root too;
relocation does not rewrite file records or snapshot history.

The caller must quiesce readers and writers: existing handles may cache the
old root. The operation neither copies nor verifies data objects. Copies must
include historical data and delete files needed by retained snapshots.

## SQL

```sql
CALL moraine_rewrite_data_path(
    's3://destination/catalog',
    's3://source/data/',
    's3://destination/data/'
);
```

The SQL function opens the catalog independently and closes it on either
success or failure. It requires autocommit and a detached catalog. Opening
the writer fences any other writer, as other offline maintenance commands do.
An optional `wal_path` names the catalog's current separate WAL store and
resolves its own credentials. Catalog credentials use DuckDB secrets or the
existing environment fallback. Data credentials are unnecessary because the
command accesses metadata only.

A read-only open first rejects a missing catalog. Before opening a writer,
the command rejects a destination data root that overlaps the catalog or WAL
store. The normal attach path continues to reject mismatched roots.

## Copy procedure

1. Stop writers, compactors, garbage collectors, and maintenance before
   copying catalog objects. Close the source catalog cleanly and keep it
   quiescent throughout the copy; copying a live bucket is not a backup.
2. For an independent catalog copy, first use `moraine_move_wal` to bring
   any separate WAL into the source catalog store. Then copy the catalog
   and data prefixes, preserving relative object keys. Do not open two
   independent catalog copies against the same separate WAL prefix.
3. Run the rewrite command against the destination catalog, naming its old
   and new data roots. Keep destination readers and writers detached.
4. Attach the destination with the new `DATA_PATH` and `META_DATA_PATH`.

This API does not implement a bucket copier, rewrite absolute paths, change
WAL identity, or turn arbitrary partial copies into consistent catalogs.
