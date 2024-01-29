use std::{num::NonZeroUsize, path::Path};

use arrow2::{
    array::StructArray,
    chunk::Chunk,
    datatypes::{DataType, Schema},
};
use rayon::prelude::*;

use crate::arrow::{
    concat,
    file_prefix::GraphPaths,
    load::mmap::{mmap_batch, write_batches},
    load::parquet_reader::{ParquetOffset, TrySlice},
    Error,
};

pub struct EdgePropsBuilder<P> {
    graph_dir: P,
    time_col_idx: usize,
}

impl<P: AsRef<Path> + Send + Sync> EdgePropsBuilder<P> {
    pub(crate) fn new(graph_dir: P, time_col_idx: usize) -> Self {
        Self {
            graph_dir,
            time_col_idx,
        }
    }

    pub(crate) fn load_t_edges_from_par_structs(
        &self,
        num_threads: NonZeroUsize,
        iter: impl IndexedParallelIterator<Item = Vec<ParquetOffset<impl TrySlice>>>,
    ) -> Result<Vec<StructArray>, Error> {
        // TODO: make this dependent on number of cores, number of columns and available memory

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.get())
            .build()
            .unwrap();

        let arrays = pool.install(|| {
            let arrays = iter
                .map(|parquet_offsets| {
                    parquet_offsets
                        .into_iter()
                        .map(|offset| offset.index.try_slice(offset.range))
                        .collect::<Result<Vec<_>, _>>()
                        .and_then(|arrays| concat(arrays))
                })
                .enumerate()
                .map(|(chunk_id, struct_arr)| {
                    struct_arr.and_then(|struct_arr| {
                        let file_path = GraphPaths::EdgeTProps.to_path(&self.graph_dir, chunk_id);
                        write_temporal_properties(file_path, struct_arr, self.time_col_idx)
                    })
                })
                .collect::<Result<Vec<_>, Error>>();
            arrays
        });
        let arrays = arrays?;
        if arrays.is_empty() {
            Err(Error::NoEdgeLists)
        } else {
            Ok(arrays)
        }
    }
}

fn write_temporal_properties(
    file_path: impl AsRef<Path>,
    chunk: StructArray,
    time_col_idx: usize,
) -> Result<StructArray, Error> {
    let (mut fields, mut values, _) = chunk.into_data();
    // make sure the time column is first
    values.swap(0, time_col_idx);
    fields.swap(0, time_col_idx);
    let schema = Schema::from(fields.clone());

    write_batches(file_path.as_ref(), schema, &[Chunk::new(values)])?;
    let mmapped_chunk = unsafe { mmap_batch(file_path.as_ref(), 0)? };
    let mmapped = StructArray::new(DataType::Struct(fields), mmapped_chunk.into_arrays(), None);
    Ok(mmapped)
}
