//! Object identities shared by cleanup and file publication.

use std::path::{Component, Path, PathBuf};

use crate::{Error, Result, store::key::Key};

pub(super) fn key(path: &str) -> Vec<u8> {
    Key::OrphanDeletion { path: path.into() }.encode()
}

pub(super) fn join(parent: &str, path: &str, relative: bool) -> String {
    if !relative || parent.is_empty() {
        path.to_owned()
    } else {
        format!("{}/{path}", parent.trim_end_matches('/'))
    }
}

fn local_path(path: &str) -> Result<Option<PathBuf>> {
    if path.is_empty() {
        return Err(Error::Constraint(
            "orphan deletion requires a nonempty path".into(),
        ));
    }
    let path = path.replace('\\', "/");
    if path.starts_with('~') {
        return Err(Error::Constraint(
            "expand the local home directory before reserving a cleanup path".into(),
        ));
    }
    let path = if let Some(rest) = path.strip_prefix("file:") {
        if let Some(rest) = rest.strip_prefix("//localhost/") {
            format!("/{rest}")
        } else if rest.starts_with("///") {
            rest[2..].to_owned()
        } else if rest.starts_with("//") || !rest.starts_with('/') {
            return Err(Error::Constraint("unsupported local file URL".into()));
        } else {
            rest.to_owned()
        }
    } else if path.contains("://") {
        return Ok(None);
    } else {
        path
    };
    #[cfg(windows)]
    let path = if path.starts_with('/') && path.as_bytes().get(2) == Some(&b':') {
        path[1..].to_owned()
    } else {
        path
    };
    std::path::absolute(path)
        .map(Some)
        .map_err(|error| Error::Constraint(format!("cannot resolve cleanup path: {error}")))
}

pub(super) fn canonical(path: &str) -> Result<String> {
    if let Some(path) = local_path(path)? {
        return canonical_local(&path);
    }
    let normalized = path.replace('\\', "/");
    let (_, remote) = normalized
        .split_once("://")
        .ok_or_else(|| Error::Constraint("invalid remote path".into()))?;
    Ok(format!(
        "remote://{}",
        remote.split('?').next().unwrap_or(remote)
    ))
}

/// Keeps the addressed name as well as symlink targets, including dangling
/// links.
pub(super) fn identities(path: &str) -> Result<Vec<String>> {
    let Some(mut path) = local_path(path)? else {
        return Ok(vec![canonical(path)?]);
    };
    let mut identities = Vec::new();
    for _ in 0..40 {
        let address = if let (Some(parent), Some(name)) = (path.parent(), path.file_name()) {
            lexical(&PathBuf::from(canonical_local(parent)?).join(name))?
        } else {
            canonical_local(&path)?
        };
        if identities.contains(&address) {
            return Err(Error::Constraint("cyclic cleanup symlink".into()));
        }
        identities.push(address);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let target = std::fs::read_link(&path).map_err(|error| {
                    Error::Constraint(format!("cannot resolve cleanup symlink: {error}"))
                })?;
                path = if target.is_absolute() {
                    target
                } else {
                    path.parent().unwrap_or_else(|| Path::new("/")).join(target)
                };
            }
            Ok(_) => return Ok(identities),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(identities),
            Err(error) => {
                return Err(Error::Constraint(format!(
                    "cannot resolve cleanup symlink: {error}"
                )));
            }
        }
    }
    Err(Error::Constraint("cleanup symlink depth exceeds 40".into()))
}

fn canonical_local(path: &Path) -> Result<String> {
    let mut ancestor = path;
    let mut suffix = Vec::new();
    let resolved = loop {
        match std::fs::canonicalize(ancestor) {
            Ok(existing) => {
                let mut resolved = existing;
                for part in suffix.iter().rev() {
                    resolved.push(part);
                }
                break resolved;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = ancestor.file_name() else {
                    break path.to_owned();
                };
                suffix.push(name.to_owned());
                let Some(parent) = ancestor.parent() else {
                    break path.to_owned();
                };
                ancestor = parent;
            }
            Err(error) => {
                return Err(Error::Constraint(format!(
                    "cannot resolve cleanup path: {error}"
                )));
            }
        }
    };
    lexical(&resolved)
}

fn lexical(path: &Path) -> Result<String> {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
        .to_str()
        .map(|path| path.replace('\\', "/"))
        .ok_or_else(|| Error::Constraint("cleanup path is not UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn deletion_keys_roundtrip(path in any::<String>()) {
            let encoded = key(&path);
            prop_assert_eq!(encoded[0], 12);
            prop_assert_eq!(Key::decode(&encoded).unwrap(), Key::OrphanDeletion { path });
        }
    }

    #[test]
    fn aliases_and_queries_name_the_same_remote_object() {
        assert_eq!(
            canonical("s3://bucket/a.parquet").unwrap(),
            canonical("s3a://bucket/a.parquet?signature=secret").unwrap()
        );
        assert_ne!(
            canonical("s3://one/a.parquet").unwrap(),
            canonical("s3://two/a.parquet").unwrap()
        );
    }

    #[test]
    fn absent_local_files_keep_a_stable_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file.parquet");
        std::fs::write(&path, b"bytes").unwrap();
        let before = canonical(path.to_str().unwrap()).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(canonical(path.to_str().unwrap()).unwrap(), before);
    }
}
