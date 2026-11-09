# Vendored slatedb 0.18.1 with moraine's patches

This directory is the `slatedb` crate as `cargo package` emits it from
upstream [slatedb/slatedb] at v0.18.1 (commit `7351512d`), with the
patches in `moraine-patches/` applied on top. The workspace manifest
patches `slatedb` to this path; `slatedb-common`, `slatedb-mirror` and
`slatedb-txn-obj` come from crates.io at 0.18.1 unchanged.

The patches:

1. `0001`: `DbReader::refresh()`, an explicit refresh through the
   manifest poller, so a committer reads the log as it stands instead of
   waiting for the poll. The four commits of slatedb/slatedb#2080
   squashed; tracked upstream as slatedb/slatedb#2200, and RFC-0036
   (slatedb/slatedb#2186) is the upstream design that would replace it.
2. `0002`: the sequence tracker drops stored entries ahead of a write
   instead of asserting. A manifest written by the version 1 tracker
   codec can carry a truncated, near-`u64::MAX` entry; this lets such a
   store heal on its next flush.

To re-vendor on a new upstream release: check out the release tag,
`git am moraine-patches/*.patch`, run `cargo package -p slatedb
--no-verify`, extract the `.crate` here, delete `Cargo.toml.orig`,
`.cargo_vcs_info.json` and `Cargo.lock`, and copy `LICENSE`,
`MORAINE.md` and `moraine-patches/` back in.

[slatedb/slatedb]: https://github.com/slatedb/slatedb
