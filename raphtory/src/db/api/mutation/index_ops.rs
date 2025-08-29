use crate::{
    db::api::view::{IndexSpec, IndexSpecBuilder},
    errors::GraphError,
    prelude::AdditionOps,
    serialise::GraphFolder,
};
use std::{fs::File, path::Path};
use zip::ZipArchive;

/// Mutation operations for managing indexes.
pub trait IndexMutationOps: Sized + AdditionOps {
    /// Creates a new index with default specifications.
    ///
    /// Returns:
    ///     Ok(()) if the index was created successfully.
    ///     Err(GraphError) if the operation fails.
    fn create_index(&self) -> Result<(), GraphError>;

    /// Creates a new index using the provided index specification.
    ///
    /// Arguments:
    ///     index_spec: - The specification for the index to be created.
    ///
    /// Returns:
    ///     Ok(()) if the index was created successfully.
    ///     Err(GraphError) if the operation fails.
    fn create_index_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError>;

    /// Creates a new index in RAM with default specifications.
    ///
    /// Returns:
    ///     Ok(()) if the in-memory index was created successfully.
    ///     Err(GraphError) if the operation fails.
    fn create_index_in_ram(&self) -> Result<(), GraphError>;

    /// Creates a new index in RAM using the provided index specification.
    ///
    /// Arguments:
    ///     index_spec: - The specification for the in-memory index to be created.
    ///
    /// Returns:
    ///     Ok(()) if the in-memory index was created successfully.
    ///     Err(GraphError) if the operation fails.
    fn create_index_in_ram_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError>;

    /// Loads an index from the specified disk path.
    ///
    /// Arguments:
    ///     path - The path to the folder containing the index data.
    ///
    /// Returns:
    ///     Ok(()) if the index was loaded successfully.
    ///     Err(GraphError) if the operation fails.
    fn load_index(&self, path: &GraphFolder) -> Result<(), GraphError>;

    /// Persists the current index to disk at the specified path.
    ///
    /// Arguments:
    ///     path - The path to the folder where the index should be saved.
    ///
    /// Returns:
    ///     Ok(()) if the index was persisted successfully.
    ///     Err(GraphError) if the operation fails.
    fn persist_index_to_disk(&self, path: &GraphFolder) -> Result<(), GraphError>;

    /// Persists the current index to disk as a compressed ZIP file at the specified path.
    ///
    /// Arguments:
    ///     path - The path to the folder where the ZIP file should be saved.
    ///
    /// Returns:
    ///     Ok(()) if the index was persisted and compressed successfully.
    ///     Err(GraphError) if the operation fails.
    fn persist_index_to_disk_zip(&self, path: &GraphFolder) -> Result<(), GraphError>;

    /// Drops (removes) the current index from the database.
    ///
    /// Returns:
    ///     Ok(()) if the index was dropped successfully.
    ///     Err(GraphError) if the operation fails.
    fn drop_index(&self) -> Result<(), GraphError>;
}

impl<G: AdditionOps> IndexMutationOps for G {
    fn create_index(&self) -> Result<(), GraphError> {
        let index_spec = IndexSpecBuilder::new(self.clone())
            .with_all_node_properties_and_metadata()
            .with_all_edge_properties_and_metadata()
            .build();
        self.create_index_with_spec(index_spec)
    }

    fn create_index_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.create_index_if_empty(index_spec)?;
                Ok(())
            })
    }

    fn create_index_in_ram(&self) -> Result<(), GraphError> {
        let index_spec = IndexSpecBuilder::new(self.clone())
            .with_all_node_properties_and_metadata()
            .with_all_edge_properties_and_metadata()
            .build();
        self.create_index_in_ram_with_spec(index_spec)
    }

    fn create_index_in_ram_with_spec(&self, index_spec: IndexSpec) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.create_index_in_ram_if_empty(index_spec)?;
                Ok(())
            })
    }

    fn load_index(&self, path: &GraphFolder) -> Result<(), GraphError> {
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
                if path.is_zip() {
                    if has_index(path.get_base_path())? {
                        storage.load_index_if_empty(&path)?;
                    } else {
                        return Ok(()); // Skip if no index in zip
                    }
                } else {
                    let index_path = path.get_index_path();
                    if index_path.exists() && index_path.read_dir()?.next().is_some() {
                        storage.load_index_if_empty(&path)?;
                    }
                }

                Ok(())
            })
    }

    fn persist_index_to_disk(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.persist_index_to_disk(&path)?;
                Ok(())
            })
    }

    fn persist_index_to_disk_zip(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.persist_index_to_disk_zip(&path)?;
                Ok(())
            })
    }

    fn drop_index(&self) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.drop_index()?;
                Ok(())
            })
    }
}
