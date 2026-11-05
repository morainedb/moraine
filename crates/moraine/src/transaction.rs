//! The commit protocol: turns catalog mutations into one atomic store
//! write, with conflict classification and bounded benign-race retry.

pub(crate) mod commit;
pub(crate) mod file_targets;
pub(crate) mod index_maintenance;
pub(crate) mod inline;
pub(crate) mod maintenance_status;
pub(crate) mod migration;
pub(crate) mod names;
pub(crate) mod operations;
pub(crate) mod read_scope;
pub(crate) mod scoped;
pub(crate) mod session;
pub(crate) mod staged;
pub(crate) mod verbs;
pub(crate) mod wal_move;

pub use demand::Transaction;
pub use migration::MigrationReport;
pub(crate) use verbs::EncodedIndexEntry;
pub(crate) mod demand;
pub use wal_move::WalStoreMove;

pub(crate) mod table_read;
