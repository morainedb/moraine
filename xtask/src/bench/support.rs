//! Temporary catalog and data directories for CLI benchmarks.

use std::{fs, path::PathBuf};

use anyhow::Context;

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        if path.exists() {
            fs::remove_dir_all(&path).with_context(|| format!("clearing {}", path.display()))?;
        }
        let directory = Self(path);
        for child in [directory.store(), directory.data()] {
            fs::create_dir_all(&child).with_context(|| format!("creating {}", child.display()))?;
        }
        Ok(directory)
    }

    pub fn store(&self) -> PathBuf {
        self.0.join("store")
    }

    pub fn data(&self) -> PathBuf {
        self.0.join("data")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
