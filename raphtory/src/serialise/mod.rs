use memmap2::Mmap;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

pub(crate) mod incremental;
mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub use proto::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode};

#[derive(Clone, Debug)]
pub struct GraphFolder {
    path: PathBuf,
    force_zip_format: bool, // TODO: rename to prefer_folder, because we ignore it in some instances
}

enum GraphReader {
    Zip(Vec<u8>),
    Folder(Mmap),
}

impl AsRef<[u8]> for GraphReader {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Zip(bytes) => bytes.as_ref(),
            Self::Folder(mmap) => mmap.as_ref(),
        }
    }
}

impl GraphFolder {
    pub fn new_as_zip(path: impl AsRef<Path>) -> Self {
        let folder: GraphFolder = path.into();
        Self {
            force_zip_format: true,
            ..folder
        }
    }

    // pub fn into_zip_format(self) -> Self {
    //     Self {
    //         force_zip_format: true,
    //         ..self
    //     }
    // }

    // TODO: make it private again once we stop using it from the graphql crate
    pub fn get_graph_path(&self) -> PathBuf {
        self.path.join("graph")
    }

    pub fn read_graph(&self) -> Result<GraphReader, io::Error> {
        if self.path.is_file() {
            let file = File::open(&self.path)?;
            let mut archive = ZipArchive::new(file)?;
            let mut entry = archive.by_name("graph")?;
            let mut buf = vec![];
            entry.read_to_end(&mut buf)?;
            Ok(GraphReader::Zip(buf))
        } else {
            let file = File::open(self.get_graph_path())?;
            let buf = unsafe { memmap2::MmapOptions::new().map(&file)? };
            Ok(GraphReader::Folder(buf))
        }
    }

    pub fn write_graph(&self, buf: &[u8]) -> Result<(), io::Error> {
        if self.force_zip_format {
            let file = File::create(&self.path)?;
            let mut zip = ZipWriter::new(file);
            zip.start_file::<_, ()>("graph", FileOptions::default())?;
            zip.write_all(buf)
        } else {
            let _ignored = fs::create_dir(&self.path);
            let mut file = File::create(self.get_graph_path())?;
            file.write_all(buf)
        }
    }

    // fn create_graph_file(&self) -> Result<File, std::io::Error> {
    //     let _ignored = fs::create_dir(&self.path);
    //     File::create(self.get_graph_path())
    // }

    fn get_appendable_graph_file(&self) -> Result<File, std::io::Error> {
        let _ignored = fs::create_dir(&self.path);
        OpenOptions::new().append(true).open(self.get_graph_path())
    }

    // TODO: make private once possible
    pub fn get_vectors_path(&self) -> PathBuf {
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
            force_zip_format: false,
        }
    }
}

impl From<&GraphFolder> for GraphFolder {
    fn from(value: &GraphFolder) -> Self {
        value.clone()
    }
}

// this mod focuses on the zip format, as the folder format is
// the default and is largelly exercised in other places
#[cfg(test)]
mod zip_tests {
    use crate::{
        prelude::{AdditionOps, CacheOps, Graph, GraphViewOps, NO_PROPS},
        serialise::GraphFolder,
    };

    use super::{StableDecode, StableEncode};

    #[test]
    fn test_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None);
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        graph.encode(GraphFolder::new_as_zip(&temp_file));
        let graph = Graph::decode(&temp_file).unwrap();
        assert_eq!(graph.count_nodes(), 1);
    }

    // #[test]
    // fn test_folder_format() {
    //     let graph = Graph::new();
    //     graph.add_node(0, 0, NO_PROPS, None);
    //     let tempdir = TempDir::new().unwrap();
    //     graph.encode(GraphFolder::new_as_folder(&tempdir)).unwrap();
    //     let graph = Graph::decode(&tempdir).unwrap();
    //     assert_eq!(graph.count_nodes(), 1);
    // }

    #[test]
    fn test_load_cached_from_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None);
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        graph.encode(GraphFolder::new_as_zip(&temp_file)).unwrap();
        let result = Graph::load_cached(&temp_file);
        assert!(result.is_err());
    }
}
