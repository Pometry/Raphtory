pub(crate) mod incremental;
mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

use std::{
    fs::{self, File, OpenOptions},
    ops::Deref,
    path::{Path, PathBuf},
};

pub use proto::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode};

#[derive(Clone, Debug)]
pub struct GraphFolder {
    path: PathBuf,
}

impl GraphFolder {
    // TODO: make private once possible
    pub fn get_graph_path(&self) -> PathBuf {
        self.path.join("graph")
    }

    fn create_graph_file(&self) -> Result<File, std::io::Error> {
        let _ignored = fs::create_dir(&self.path);
        File::create(self.get_graph_path())
    }

    fn append_to_graph(&self) -> Result<File, std::io::Error> {
        let _ignored = fs::create_dir(&self.path);
        OpenOptions::new().append(true).open(self.get_graph_path())
    }

    // TODO: make private once possible
    pub fn get_vectors_file(&self) -> PathBuf {
        self.path.join("graph")
    }

    // TODO: make private once possible
    pub fn get_base_path(&self) -> &Path {
        &self.path
    }
}

// impl From<PathBuf> for GraphFolder {
//     fn from(value: PathBuf) -> Self {
//         Self { path: value }
//     }
// }

impl<P: AsRef<Path>> From<P> for GraphFolder {
    fn from(value: P) -> Self {
        let path: &Path = value.as_ref();
        Self {
            path: path.to_path_buf(),
        }
    }
}

impl From<&GraphFolder> for GraphFolder {
    fn from(value: &GraphFolder) -> Self {
        Self {
            path: value.path.clone(),
        }
    }
}
