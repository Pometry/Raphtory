use crate::{
    arrow2::{array::StructArray, datatypes::ArrowDataType as DataType},
    cast_time_column, concat,
    file_prefix::GraphPaths,
    load::{
        parquet_reader::{ParquetOffset, TrySlice},
        writer::write_and_mmap_batch,
    },
    RAError,
};
use itertools::Itertools;
use polars_arrow::array::{PrimitiveArray, Utf8Array};
use raphtory_api::core::storage::dict_mapper::DictMapper;
use rayon::prelude::*;
use std::{num::NonZeroUsize, path::Path};

pub struct PropsBuilder<P> {
    graph_dir: P,
    time_col_idx: Option<usize>,
    graph_paths: GraphPaths,
}

impl<P: AsRef<Path> + Send + Sync> PropsBuilder<P> {
    pub(crate) fn new(graph_dir: P, graph_paths: GraphPaths, time_col_idx: Option<usize>) -> Self {
        Self {
            graph_dir,
            time_col_idx,
            graph_paths,
        }
    }

    pub(crate) fn load_props_from_par_structs<T: Send>(
        &self,
        num_threads: NonZeroUsize,
        iter: impl IndexedParallelIterator<Item = Vec<ParquetOffset<impl TrySlice>>>,
        map_fn: impl Fn(&Self, usize, StructArray) -> Result<T, RAError> + Send + Sync,
    ) -> Result<Vec<T>, RAError> {
        // TODO: make this dependent on number of cores, number of columns and available memory
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.get())
            .build()
            .unwrap();

        let arrays = pool.install(|| {
            iter.map(|parquet_offsets| {
                parquet_offsets
                    .into_iter()
                    .map(|offset| offset.index.try_slice(offset.range))
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(concat)
            })
            .enumerate()
            .map(|(chunk_id, struct_arr)| {
                struct_arr.and_then(|struct_array| map_fn(self, chunk_id, struct_array))
            })
            .collect::<Result<Vec<_>, RAError>>()
        });
        let arrays = arrays?;
        Ok(arrays)
    }

    pub(crate) fn write_props(
        &self,
        chunk: StructArray,
        chunk_id: usize,
    ) -> Result<StructArray, RAError> {
        let (mut fields, mut values, _) = chunk.into_data();
        // make sure the time column is first
        if let Some(time_col_idx) = self.time_col_idx {
            fields.swap(0, time_col_idx);
            values.swap(0, time_col_idx);
            let time_col = cast_time_column(values[0].clone())?;
            fields[0].data_type = time_col.data_type().clone();
            values[0] = time_col;
        }
        let chunk = StructArray::new(DataType::Struct(fields), values, None);
        write_and_mmap_batch(self.graph_paths, self.graph_dir.as_ref(), chunk, chunk_id)
    }

    pub(crate) fn write_dict_encoded_props(
        &self,
        chunk: StructArray,
        chunk_id: usize,
        dict_mapper: &DictMapper,
    ) -> Result<PrimitiveArray<u64>, RAError> {
        let v = chunk.values()[0]
            .as_any()
            .downcast_ref::<Utf8Array<i64>>()
            .ok_or(RAError::TypeCastError)?;
        let v = v
            .iter()
            .map(|s| match s {
                Some(ss) => dict_mapper.get_or_create_id(ss).inner() as u64,
                None => 0u64,
            })
            .collect_vec();

        let chunk = PrimitiveArray::from_vec(v);
        write_and_mmap_batch(self.graph_paths, self.graph_dir.as_ref(), chunk, chunk_id)
    }
}
