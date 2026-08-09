# RFC 0014: Catalog and data encryption

- **Date:** 2026-07-13

## Summary

Encryption in a DuckLake-on-moraine deployment has two layers.
**Data-file encryption** is DuckLake's: it encrypts its Parquet files and
records the key material in catalog rows; moraine's job is to be a faithful
conduit — store and return that material verbatim, decrypt nothing, touch no
data-file bytes. **Catalog-at-rest confidentiality** is moraine's to answer
for: the SlateDB objects (SSTs, WAL) that hold the catalog live in object
storage and are, by default, plaintext.

The catalog is at least as sensitive as the data it describes: it holds the
data-file keys themselves, the plaintext column min/max in
`file_column_stats` (RFC 0002 stores them verbatim), row counts, and the full
schema. Encrypting data files is pointless while the catalog is plaintext, so
catalog-at-rest is the load-bearing decision. **Decision: delegate it to
object-store server-side encryption (SSE-KMS).** This is not a stopgap — it
is what every surveyed catalog does with its own metadata (see Prior art) —
and it means no crypto machinery, no on-disk format change, and no key
material in the moraine core.

An untrusted catalog bucket is not a supported deployment. moraine does not
offer client-side catalog encryption; deployments that cannot trust the
object store must place the catalog behind a trusted, encrypted storage
boundary.

| Layer | Owner | Protects | moraine's role |
|---|---|---|---|
| Data-file encryption | DuckLake | Parquet data and delete files | Faithful conduit: round-trip key rows verbatim, decrypt nothing |
| Catalog-at-rest | Operator, via bucket SSE-KMS | SlateDB SSTs + WAL — the keys, stats, and schema | None: reads and writes plaintext through the store client |

## Design

### Data-file encryption: moraine as faithful conduit

DuckLake owns data-file crypto end to end. With `ENCRYPTED` set it writes
Parquet data and delete files under Parquet Modular Encryption, generating a
fresh key per write operation, and stores each key — raw, base64 — in the
`encryption_key` column of `ducklake_data_file` / `ducklake_delete_file`,
with an `encrypted` flag in `ducklake_metadata`. There is no KMS indirection
in DuckLake as of this writing: the catalog holds the actual keys.

moraine's entire responsibility is to carry that material losslessly. The
keys are ordinary fields of `ducklake_*` rows; RFC 0002 encodes the rows and
RFC 0006 returns them exactly as DuckLake wrote them. moraine never decrypts
a data file, never derives or validates a key, never reads a Parquet byte
(RFC 0007), and invents no encryption of its own. Compaction and GC never
read data bytes, so an encrypted file is scheduled for deletion exactly as a
plaintext one is.

When is `ENCRYPTED` worth enabling at all? With SSE-KMS on the data bucket
too, the bucket-reader adversary is already covered for data files, and
`ENCRYPTED` — whose keys live in the catalog — can never protect more than
the catalog does. Its marginal value is client-side crypto across **split
trust domains**: bulk data hosted on a store the operator does not trust,
catalog and KMS on one they do (DuckLake's "zero-trust data hosting"). In a
single trust domain it is redundant with SSE. Either way the switch is
DuckLake's and the operator's; moraine carries one more opaque column and is
otherwise indifferent to whether it is set.

Mechanically, the switch is one bit of catalog metadata moraine does hold:
DuckLake fixes whether a catalog is encrypted when the catalog is created,
so moraine records an `encrypted` flag (a stored global option, the
`ducklake_metadata` row DuckLake reads back) at bootstrap and never
afterward. It reaches a fresh store through the attach —
`ATTACH 'ducklake:moraine:…' (DATA_PATH …, ENCRYPTED, META_ENCRYPTED true)`,
DuckLake's `META_` passthrough handing `ENCRYPTED` to the inner moraine
attach — and later attaches need no flag: DuckLake adopts the stored value.
The flag decides nothing inside moraine; it is served back so DuckLake
knows to encrypt.

### Catalog-at-rest: SSE-KMS on the bucket

The bucket holding SlateDB's objects is configured with SSE-KMS: the object
store encrypts every SST and WAL object at rest, keys live in the KMS, and
moraine reads and writes plaintext through the object-store client. The
entire cost is bucket configuration.

Key policy, grants, and rotation posture are documented for operators in
the "Operating a lake" guide.

This mirrors DuckLake's own trust model. Its manifesto pitches "zero-trust
data hosting" with "keys managed by the catalog database", and its FAQ
delegates catalog protection to the catalog DBMS's authentication (e.g.
PostgreSQL's, when Postgres is the catalog). moraine's catalog DBMS is
SlateDB on a bucket; the bucket's access control and SSE are the exact
equivalent of the Postgres deployment's auth and RDS at-rest encryption.

### Threat model

- **Protects:** the catalog — and, via DuckLake's scheme, the data files —
  against an adversary with read access to the bucket: a leaked backup, a
  stolen credential, a mis-scoped IAM read. SSE ciphertext is inert without
  the KMS grant.
- **Does not protect against:** a compromised writer process (it holds the
  keys it uses) or live memory scraping. And a party that can read the
  catalog plaintext reads the stats: column min/max are stored verbatim
  because DuckLake reads them to prune files (RFC 0002, RFC 0009). That leak
  is the price of usable pruning; no at-rest scheme changes it.
- **Key custody is not moraine's.** DuckLake generates the data-file keys;
  the operator owns the KMS. Under SSE, no key material enters moraine at
  all.

### Inlined data

Inlined data (RFC 0005) lives inside catalog values, not as Parquet in the
object store, so its confidentiality is a case of catalog-at-rest: covered by
SSE like every other value, and *not* covered by DuckLake's data-file scheme
(there is no separate file to encrypt). A deployment that relies on data-file
encryption and also inlines data has its inlined rows protected exactly as
well as its catalog — another face of the central insight.

## Prior art

Surveyed mid-2026. The pattern is uniform: catalogs delegate at-rest
protection of their own metadata to the platform beneath them, and none does
application-level encryption of its own store.

- **DuckLake** stores raw base64 keys in catalog rows and delegates catalog
  protection to the catalog DBMS's authentication and at-rest story
  ([encryption docs](https://ducklake.select/docs/stable/duckdb/advanced_features/encryption),
  [FAQ](https://ducklake.select/faq)).
- **Apache Iceberg** (spec v3, shipped in 1.11.0) does client-side envelope
  encryption of data files and the manifest tree, with wrapped keys carried
  in table metadata — but the root `metadata.json` is *not* encrypted; its
  protection is an explicit "catalog security requirement" delegated to the
  catalog and storage
  ([encryption docs](https://iceberg.apache.org/docs/nightly/encryption/)).
  The REST catalog spec carries opaque wrapped keys and performs no KMS
  operations.
- **AWS Glue Data Catalog** is the one catalog that encrypts its own
  metadata — as transparent platform-side SSE-KMS, not application-level
  crypto
  ([docs](https://docs.aws.amazon.com/glue/latest/dg/encrypt-glue-data-catalog.html)).
- **Hive Metastore, Apache Polaris, Project Nessie** ship no at-rest
  encryption of their own; deployments rely on the backing RDBMS or store
  (TDE, encrypted RDS, and the like).
- **Unity Catalog and Snowflake** encrypt metadata at the platform layer
  (with customer-managed-key options); neither defines application-level
  ciphertext in its catalog model. **Delta Lake**'s transaction log likewise
  relies on storage SSE; application-level log encryption remains an open
  proposal ([delta#2269](https://github.com/delta-io/delta/issues/2269)).
- **SlateDB** ships no encryption of its own, but exposes a
  `BlockTransformer` hook (v0.10.1) for a user-supplied SST-block cipher —
  with real gaps today: manifests and SST footers stay plaintext. It is the
  substrate seam for a different design, not a facility moraine configures or
  a complete answer for an untrusted bucket.

## Alternatives considered

- **moraine-level envelope encryption of value payloads.** moraine would
  encrypt each value's protobuf payload with a per-value DEK wrapped by a
  KMS-held KEK, keeping the 5-byte framing header (RFC 0002) plaintext so
  corruption detection and encoding-version negotiation survive. Rejected:
  it defends against the same adversary SSE already handles — a bucket
  reader — at far higher cost. It puts DEK/KEK handling and a KMS dependency
  inside the core read/write path; opaque payloads block any migration or
  tooling that must read a value's fields (RFC 0015); and ciphertext defeats
  SlateDB's SST block compression, inflating inlined data by exactly the
  cross-chunk redundancy RFC 0005 counts on reclaiming. No surveyed catalog
  application-encrypts its own metadata (Prior art), and the partial coverage
  of SlateDB's `BlockTransformer` does not change the SSE-KMS decision.
- **Encrypting the stats values to plug the min/max leak.** Rejected:
  DuckLake reads those strings for file pruning (RFC 0002, RFC 0009);
  encrypting them breaks pruning. The leak is the cost of usable,
  uninterpreted stats.
- **Treating data-file encryption as sufficient on its own.** Rejected — the
  spine of this RFC. A plaintext catalog leaks the data keys themselves
  (DuckLake stores them raw), plus stats, row counts, names, and schema.
  Data-file encryption without catalog-at-rest protection buys far less than
  it appears to.
