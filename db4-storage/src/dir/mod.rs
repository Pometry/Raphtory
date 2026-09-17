use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct GraphDir(PathBuf);

impl GraphDir {
    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn nodes(&self) -> PathBuf {
        self.path().join("nodes")
    }

    pub fn node_type_index(&self) -> PathBuf {
        // NOTE: node_type_index is stored under the nodes dir.
        self.nodes().join("type_index")
    }

    pub fn edges(&self) -> PathBuf {
        self.path().join("edges")
    }

    pub fn graph_props(&self) -> PathBuf {
        self.path().join("graph_props")
    }

    pub fn gid_resolver(&self) -> PathBuf {
        self.path().join("gid_resolver")
    }

    pub fn wal(&self) -> PathBuf {
        self.path().join("wal")
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
