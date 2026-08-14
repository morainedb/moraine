//! The integration suite: the public API against real SlateDB on
//! in-memory object storage. Object-storage-backed runs stay in the
//! separate `object_storage` target, which automation invokes by name.

mod fixtures;

mod cache;
mod catalog;
mod census;
mod checkpoint;
mod commit_concurrency;
mod counting_store;
mod crash_recovery;
mod data_files;
mod group_commit;
mod index_backfill;
mod index_lookup;
mod inline_data;
mod interrupt;
mod macros;
mod maintenance_status;
mod measure;
mod partitioning;
mod row_location;
mod runtime;
mod schema_evolution;
mod sorting;
mod staged_index_build;
mod tags;
mod views_options;
