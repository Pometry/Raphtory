use crate::{
    core::utils::errors::GraphError,
    db::api::view::{IndexSpec, IndexSpecBuilder},
    prelude::AdditionOps,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use zip::ZipArchive;

pub trait IndexMutationOps: Sized + AdditionOps {
    fn create_index(&self) -> Result<(), GraphError>;

    fn create_index_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError>;

    fn create_index_in_ram(&self) -> Result<(), GraphError>;

    fn create_index_in_ram_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError>;

    fn load_index(&self, path: &PathBuf) -> Result<(), GraphError>;

    fn persist_index_to_disk(&self, path: &PathBuf) -> Result<(), GraphError>;

    fn persist_index_to_disk_zip(&self, path: &PathBuf) -> Result<(), GraphError>;
}

impl<G: AdditionOps> IndexMutationOps for G {
    fn create_index(&self) -> Result<(), GraphError> {
        let index_spec = IndexSpecBuilder::new(self.clone())
            .with_all_node_props()
            .with_all_edge_props()
            .build();
        self.create_index_with_spec(index_spec)
    }

    fn create_index_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.get_or_create_index(index_spec)?;
                Ok(())
            })
    }

    fn create_index_in_ram(&self) -> Result<(), GraphError> {
        let index_spec = IndexSpecBuilder::new(self.clone())
            .with_all_node_props()
            .with_all_edge_props()
            .build();
        self.create_index_in_ram_with_spec(index_spec)
    }

    fn create_index_in_ram_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.get_or_create_index_in_ram(index_spec)?;
                Ok(())
            })
    }

    fn load_index(&self, path: &PathBuf) -> Result<(), GraphError> {
        fn has_index<P: AsRef<Path>>(zip_path: P) -> Result<bool, GraphError> {
            let file = File::open(&zip_path)?;
            let mut archive = ZipArchive::new(file)?;

            for i in 0..archive.len() {
                let entry = archive.by_index(i)?;
                let entry_path = Path::new(entry.name());

                if let Some(first_component) = entry_path.components().next() {
                    if first_component.as_os_str() == "index" {
                        return Ok(true);
                    }
                }
            }

            Ok(false)
        }

        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                if path.is_file() {
                    if has_index(path)? {
                        storage.get_or_load_index(path.clone())?;
                    } else {
                        return Ok(()); // Skip if no index in zip
                    }
                } else {
                    let index_path = path.join("index");
                    if index_path.exists() && index_path.read_dir()?.next().is_some() {
                        storage.get_or_load_index(path.clone())?;
                    }
                }

                Ok(())
            })
    }

    fn persist_index_to_disk(&self, path: &PathBuf) -> Result<(), GraphError> {
        let path = path.join("index");
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.persist_index_to_disk(&path)?;
                Ok(())
            })
    }

    fn persist_index_to_disk_zip(&self, path: &PathBuf) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.persist_index_to_disk_zip(&path)?;
                Ok(())
            })
    }
}
