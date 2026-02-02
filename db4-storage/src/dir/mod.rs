use std::{
    io,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

#[derive(Debug)]
pub enum GraphDir {
    Temp(TempDir),
    Path(PathBuf),
}

impl GraphDir {
    pub fn path(&self) -> &Path {
        match self {
            GraphDir::Temp(dir) => dir.path(),
            GraphDir::Path(path) => path,
        }
    }
    pub fn gid_resolver_dir(&self) -> PathBuf {
        self.path().join("gid_resolver")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.path().join("wal")
    }

    pub fn create_dir(&self) -> Result<(), io::Error> {
        if let GraphDir::Path(path) = self {
            std::fs::create_dir_all(path)?;
        }
        Ok(())
    }
}

impl AsRef<Path> for GraphDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

impl<'a> From<&'a Path> for GraphDir {
    fn from(path: &'a Path) -> Self {
        GraphDir::Path(path.to_path_buf())
    }
}
