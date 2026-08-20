---
title: Operating a lake
description: The retention and cleanup safety contract, and the at-rest encryption posture.
sidebar:
  order: 5
---

Two things about a moraine-backed lake are the operator's to get right,
because no setting inside moraine can compensate for either: how long
expired state survives before its bytes are reclaimed, and how the bucket
holding the catalog is encrypted.

## The retention safety contract

Expiry and cleanup are DuckLake's, driven by `ducklake_expire_snapshots`
and `ducklake_cleanup_old_files`. Moraine reclaims what they schedule and
adds no policy of its own, so DuckLake's safety contract is the whole
contract — and it is two inequalities:

- **The retention window must exceed the maximum read and attach
  duration.** A read that outlives the window finds the snapshot it was
  resolved against gone.
- **The cleanup grace period must exceed the maximum reader and scan
  duration.** That is `ducklake_cleanup_old_files`' `older_than`, and it
  governs bytes rather than catalog rows.

The two are separate because expiry and cleanup do different things.
Expiry is *logical*: it deletes snapshot records and prunes the history
rows only those snapshots could see, and it deletes no data. A reader
holding a view of an expired snapshot keeps scanning happily, because the
Parquet it names is still there — scheduled for deletion, but not yet
deleted. Cleanup is what removes the bytes, and the grace period is the
only thing standing between a running scan and the files under it.

Size both windows from *observed* durations, not intended ones. The
duration that matters is the longest a reader actually holds a view: an
attach that stays open across an idle session still holds one, and a scan
over a large table is bounded by the data, not by the query's timeout.

### When a reader outlives the window

A reader whose snapshot has been expired gets `SnapshotExpired` rather than
wrong answers or a partial view. The contract there is to re-resolve from
head and retry — the failure is a signal to re-read, not corruption, and
moraine classifies it that way everywhere it can arise, including a commit
whose base predates a concurrent expiry.

Time travel to an expired snapshot fails the same way and permanently:
below the horizon, the snapshot is gone. If a workload needs to reach back
a fixed distance, that distance *is* the retention window.

## At-rest encryption

There are two independent layers, and they are configured in different
places by different people.

**Data files** are DuckLake's. When a lake is attached `ENCRYPTED`,
DuckLake generates a key per Parquet file and stores it in the catalog;
moraine carries those keys verbatim, as a faithful conduit. No key
material is moraine's to manage, and there is no KMS indirection in that
path.

**The catalog** is the bucket's. Moraine's catalog is SlateDB objects —
SSTs and WAL — in object storage, and protecting them at rest means
configuring server-side encryption with KMS-managed keys (SSE-KMS) on that
bucket. Moraine reads and writes plaintext through the object-store
client; the object store encrypts on the way down. The entire cost is
bucket configuration, and no key material enters moraine at all.

This is the same trust model a Postgres-backed DuckLake has. There, the
catalog is protected by the database's authentication and its host's
at-rest encryption. Moraine's catalog database *is* the bucket, so the
bucket's access control and SSE are the exact equivalent.

### Key policy, grants, and rotation

- **Key policy.** A writer needs to mint data keys and decrypt them —
  `kms:GenerateDataKey` plus `kms:Decrypt`, in AWS terms — while a
  read-only attach needs only `kms:Decrypt`. Scope the grant to the
  principals that attach the lake, and prefer a key dedicated to the
  catalog bucket over a shared account-wide one — the blast radius of the
  grant is every object in the bucket.
- **Grants.** Bucket read access alone buys an adversary nothing without
  the KMS grant: SSE ciphertext is inert. That is what makes the grant,
  rather than the bucket policy, the thing to audit. A mis-scoped IAM
  read, a leaked backup, or a stolen storage credential is covered; a
  compromised writer process is not, since it legitimately holds what it
  uses.
- **Rotation.** Rotate on whatever schedule your compliance posture
  requires; it is transparent to moraine, which never sees key material.
  Note what rotation does *not* do: existing objects are not re-encrypted.
  New writes use the current key material and old objects stay readable
  only while the material that wrote them is retained — automatic KMS
  rotation keeps prior versions for exactly this reason. Rotating by
  switching to a *different* key is the case that bites: the old key must
  stay enabled as long as any object written under it survives, including
  objects an unexpired snapshot or a checkpoint still pins. Disabling or
  deleting key material whose objects are still live makes the catalog
  unreadable, and no moraine-side setting recovers from it.

### What encryption does not hide

A party that can read the catalog plaintext can read the statistics.
Column min/max values are stored verbatim, because DuckLake reads them to
prune files — so the shape of your data is visible to anyone holding the
catalog grant, however the files themselves are encrypted. That is the
price of usable pruning, and no at-rest scheme changes it.

Inlined rows live inside catalog values rather than as Parquet, so they
are protected exactly as well as the catalog is, and *not* by DuckLake's
data-file encryption — there is no separate file to encrypt. A deployment
that relies on data-file encryption and also inlines data should size its
catalog protection accordingly.
