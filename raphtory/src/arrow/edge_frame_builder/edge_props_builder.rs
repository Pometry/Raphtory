use std::path::Path;

use arrow2::{
    array::{Array, PrimitiveArray, StructArray},
    buffer::Buffer,
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    offset::OffsetsBuffer,
    types::NativeType,
};
use rayon::prelude::*;

use crate::arrow::{
    array_as_id_iter,
    chunked_array::chunked_array::ChunkedArray,
    ipc,
    mmap::{mmap_batch, write_batches},
    parquet_reader::LoadStruct,
    Error, GraphChunk, GID,
};

pub struct EdgePropsBuilder<P> {
    graph_dir: P,
    pub(crate) src_col_idx: usize,
    pub(crate) dst_col_idx: usize,
    time_col_idx: usize,
}

impl<P: AsRef<Path> + Send + Sync> EdgePropsBuilder<P> {
    pub(crate) fn new(
        graph_dir: P,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
    ) -> Self {
        Self {
            graph_dir,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
        }
    }

    pub(crate) fn load_t_edge_offsets_from_par_chunks(
        &self,
        chunks: impl ParallelIterator<Item = Result<GraphChunk, arrow2::error::Error>>,
    ) -> Result<OffsetsBuffer<i64>, Error> {
        let bounds_and_counts = chunks
            .map(|chunk| self.count_chunk(chunk))
            .collect::<Result<Vec<_>, Error>>()?;

        let mut bounds_and_counts_iter = bounds_and_counts.into_iter();
        let (mut old_bounds, counts) = bounds_and_counts_iter
            .next()
            .ok_or_else(|| Error::NoEdgeLists)?;

        let mut offsets: Vec<i64> = vec![0];
        offsets.extend(counts.into_iter().scan(0i64, |state, v| {
            let new = v as i64 + *state;
            *state = new;
            Some(new)
        }));

        for (bounds, counts) in bounds_and_counts_iter {
            let mut counts_iter = counts.into_iter();

            if bounds.first == old_bounds.last {
                let first_count = counts_iter.next().ok_or_else(|| Error::EmptyChunk)?;
                *offsets.last_mut().unwrap() += first_count as i64;
            }
            let last_value = *offsets.last().unwrap();
            offsets.extend(counts_iter.scan(last_value, |state, v| {
                let new = v as i64 + *state;
                *state = new;
                Some(new)
            }));
            old_bounds = bounds;
        }

        let buffer = Buffer::from(offsets);
        write_buffer(
            self.graph_dir.as_ref().join("edge_offsets.ipc"),
            buffer.clone(),
        )?;
        //safety: we made some offsets that are increasing and start at 0
        Ok(unsafe { OffsetsBuffer::new_unchecked(buffer) })
    }

    pub(crate) fn load_t_edges_from_par_structs(
        &self,
        iter: impl IndexedParallelIterator<Item = impl LoadStruct>,
        schema: &Schema,
    ) -> Result<ChunkedArray<StructArray>, Error> {
        // TODO: make this dependent on number of cores, number of columns and available memory

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();

        let arrays = pool.install(|| {
            let arrays = iter
                .map(|parquet_offsets| parquet_offsets.load_struct(schema))
                .enumerate()
                .map(|(chunk_id, struct_arr)| {
                    let file_path = self
                        .graph_dir
                        .as_ref()
                        .join(format!("edge_chunk_{:08}.ipc", chunk_id));

                    write_temporal_properties(file_path, struct_arr, self.time_col_idx)
                })
                .collect::<Result<Vec<_>, Error>>().unwrap(); // TODO: handle error
            arrays
        });

        Ok(ChunkedArray::from_vec(arrays))
    }

    fn count_chunk(
        &self,
        chunk: Result<GraphChunk, arrow2::error::Error>,
    ) -> Result<(EdgeBounds, Vec<usize>), Error> {
        let chunk = chunk?;
        let srcs = &chunk.srcs;
        let dests = &chunk.dsts;

        let srcs = array_as_id_iter(srcs)?;
        let dests = array_as_id_iter(dests)?;

        let mut iter = srcs.zip(dests);
        let first = iter.next().ok_or_else(|| Error::EmptyChunk)?;
        let mut last = first.clone();
        let mut counts: Vec<usize> = vec![1];

        for edge in iter {
            if edge == last {
                *counts.last_mut().expect("this is not empty") += 1;
            } else {
                counts.push(1);
            }
            last = edge;
        }

        Ok((EdgeBounds { first, last }, counts))
    }
}

struct EdgeBounds {
    first: (GID, GID),
    last: (GID, GID),
}

fn write_buffer<T: NativeType>(
    file_path: impl AsRef<Path>,
    buffer: Buffer<T>,
) -> Result<(), Error> {
    let arr: PrimitiveArray<T> =
        unsafe { PrimitiveArray::from_inner_unchecked(T::PRIMITIVE.into(), buffer, None) };

    let schema = Schema::from(vec![Field::new("offsets", arr.data_type().clone(), false)]);
    let chunk = Chunk::new(vec![arr.boxed()]);
    write_batches(file_path, schema, &[chunk])?;
    Ok(())
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
    // let mmapped_chunk = ipc::read_batch(file_path.as_ref()).unwrap(); // uncomment for better errors in case of broken ipc file
    let mmapped_chunk = unsafe { mmap_batch(file_path.as_ref(), 0)? };
    let mmapped = StructArray::new(DataType::Struct(fields), mmapped_chunk.into_arrays(), None);
    Ok(mmapped)
}
