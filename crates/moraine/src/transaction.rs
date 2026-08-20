//! The commit protocol: turns catalog mutations into one atomic store
//! write, with conflict classification and bounded benign-race retry.

pub(crate) mod commit;
pub(crate) mod folder;
pub(crate) mod index_maintenance;
pub(crate) mod inline;
pub(crate) mod maintenance_status;
pub(crate) mod migration;
pub(crate) mod operations;
pub(crate) mod slot_commit;
pub(crate) mod staged;
mod verbs;

pub use migration::MigrationReport;
pub use verbs::Transaction;
