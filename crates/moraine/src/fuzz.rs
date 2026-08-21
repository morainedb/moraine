//! Decode entry points for the fuzz targets, which live in their own crate
//! and so cannot reach the crate-private codecs directly.
//!
//! Every function here is total by contract: arbitrary bytes decode into a
//! value or fail as an error, and never panic. That is the whole property
//! the targets assert — they discard the result and let a panic be the
//! finding.

use crate::store::{
    key::{CurrentKey, Key, SysKey},
    proto, read, value,
};

/// Decodes arbitrary bytes as a store key.
pub fn decode_key(bytes: &[u8]) {
    let _ = Key::decode(bytes);
}

/// Decodes an arbitrary key/value pair the way a scan does: the key names
/// the subspace, and the subspace decides which record type the value must
/// be. A pair whose key does not decode is dropped — the key codec is
/// [`decode_key`]'s subject, and reaching the value decoders is this
/// target's.
pub fn decode_record(key_bytes: &[u8], value_bytes: &[u8]) {
    let Ok(key) = Key::decode(key_bytes) else {
        return;
    };
    match key {
        Key::Current(CurrentKey::Entity(entity)) => {
            let _ = read::decode_entity(entity, value_bytes);
        }
        Key::History(history) => {
            let _ = read::decode_entity(history.entity, value_bytes);
        }
        Key::Current(CurrentKey::GcFile { .. }) => {
            let _ = value::decode_value::<proto::GcFileValue>(value_bytes);
        }
        Key::Snapshot { .. } => {
            let _ = value::decode_value::<proto::SnapshotValue>(value_bytes);
        }
        Key::Changelog { .. } => {
            let _ = value::decode_value::<proto::ChangelogValue>(value_bytes);
        }
        Key::SchemaVersion { .. } => {
            let _ = value::decode_value::<proto::SchemaVersionValue>(value_bytes);
        }
        Key::Sys(SysKey::Format) => {
            let _ = value::decode_value::<proto::FormatValue>(value_bytes);
        }
        Key::Sys(SysKey::Head) => {
            let _ = value::decode_value::<proto::HeadValue>(value_bytes);
        }
        Key::Sys(SysKey::Migration) => {
            let _ = value::decode_value::<proto::MigrationValue>(value_bytes);
        }
        Key::Sys(SysKey::MaintenanceStatus) => {
            let _ = value::decode_value::<proto::MaintenanceStatusValue>(value_bytes);
        }
        Key::Sys(SysKey::Leader) => {
            let _ = value::decode_value::<proto::LeaderValue>(value_bytes);
        }
        Key::Sys(SysKey::Secret) => {
            let _ = value::decode_value::<proto::SecretValue>(value_bytes);
        }
        Key::Inline(_) | Key::Index(_) => {}
    }
}
