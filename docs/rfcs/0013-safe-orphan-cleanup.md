# Safe orphan cleanup

Orphan cleanup must commit deletion intent before removing a data object. File
publication and that intent use the same catalog commit order. Age windows
remain policy; they no longer decide whether a missing file can be published.

## Ordering

The extension lists candidates using DuckLake's existing filters. Immediately
before deletion it asks the core to reserve those paths. The core acquires a
fresh commit session, removes paths referenced by any retained data-file,
delete-file, or scheduled-deletion record, then commits the remaining paths as
one head-preserving batch. A lost slot repeats the reference check against the
winner. Physical deletion starts only after the plan is durably acknowledged.
An error or unknown acknowledgement deletes nothing.

Every catalog publication validates its resolved data-file and delete-file
paths against deletion records before submitting its batch. Publication that
wins first makes the next plan exclude its file. A plan that wins first makes
the next publication refuse the path. Validation also covers path-changing
metadata writes and grouped commits. Existing snapshots and the retention
window for retired inputs are unchanged.

Deletion records are permanent path tombstones. Retrying cleanup is harmless,
and a crash before physical deletion leaves the object eligible for a later
pass. Clearing a record after deletion would allow an indefinitely delayed
publisher to register the missing object, so this design does not clear them.
Object paths reserved for deletion cannot be reused for publication.

## Boundary

Core owns path normalization, reference validation, tombstones, and publication
checks. The DuckDB shim only passes candidates to core and deletes the approved
subset. A default no-op DuckLake metadata-manager hook preserves other
backends' cleanup behavior. Dry runs create no plan and remain advisory.

Core format 13 adds the deletion keyspace. Existing lakes must upgrade with
writers quiesced; older binaries cannot open format 13. All writers must use
the upgraded implementation before safe cleanup is enabled. Cleanup refuses
an older format rather than deleting without protection.

Local paths reserve both their addressed name and every symlink target,
including dangling targets. Parent directories are canonicalized, so removing
a file or symlink cannot erase the identity publication must check.
Remote paths use authority and object key, ignoring scheme aliases and query
credentials, matching DuckLake's reference comparison. References in retained
history and scheduled deletions protect objects as well as current files.

## Tests

Real SlateDB tests cover both publication orders, durable refusal after reopen,
path aliases, relative paths, history, delete files, and path changes. The
enterprise two-process suite exercises both race orders through the actual
DuckDB cleanup function, including an explicitly held stale read transaction.
Dry runs do not reserve paths; physical-delete failures and retries cannot
undo publication fencing. New key encodings have property round-trip tests.

## Key layout and API

Format 13 appends `OrphanDeletion { path: String }` to the `Key` enum, at
subspace byte 12. Values are empty. The empty path is a presence marker;
nonempty paths reserve canonical object identities permanently. This is
additive: no existing records require rewriting.

`Catalog::plan_orphan_deletion(&[String]) -> Result<Vec<String>>` returns
only paths whose reservations are durable and whose reference check won the
catalog commit order. The caller may delete precisely that returned subset.
The method does not perform object-store deletion. An empty result is a
successful no-op; an error authorizes no deletion.

The shared batch submission path checks file and delete-file publications,
including grouped and staged transactions. Path-changing metadata updates
also check the files they would redirect. A deletion plan advances the batch
count without creating a catalog snapshot.
