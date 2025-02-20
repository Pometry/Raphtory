use crate::{
    core::utils::errors::GraphError, disk_graph::DiskGraphStorage,
    io::parquet_loaders::read_struct_arrays,
};
use pometry_storage::RAError;
use std::path::Path;

impl DiskGraphStorage {
    pub fn load_node_types_from_parquets(
        &mut self,
        parquet_path: impl AsRef<Path>,
        type_col: &str,
        chunk_size: usize,
    ) -> Result<(), GraphError> {
        let chunks =
            read_struct_arrays(parquet_path.as_ref(), Some(&[type_col]))?.map(
                |chunk| match chunk {
                    Ok(chunk) => {
                        let (_, cols, _) = chunk.into_data();
                        cols.into_iter().next().ok_or(RAError::EmptyChunk)
                    }
                    Err(err) => Err(err),
                },
            );
        self.load_node_types_from_arrays(chunks, chunk_size)
    }
}
