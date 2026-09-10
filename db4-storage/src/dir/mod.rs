use std::{
    io,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct GraphDir(PathBuf);

impl GraphDir {
    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn nodes_dir(&self) -> PathBuf {
        self.path().join("nodes")
    }

    pub fn edges_dir(&self) -> PathBuf {
        self.path().join("edges")
    }

    pub fn graph_props_dir(&self) -> PathBuf {
        self.path().join("graph_props")
    }

    pub fn gid_resolver_dir(&self) -> PathBuf {
        self.path().join("gid_resolver")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.path().join("wal")
    }

    pub fn create_dir(&self) -> Result<(), io::Error> {
        std::fs::create_dir_all(self.path())?;
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
        GraphDir(path.to_path_buf())
    }
}
