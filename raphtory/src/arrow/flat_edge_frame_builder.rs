use crate::arrow::{
    chunked_array::{chunked_array::ChunkedArray, list_array::ChunkedListArray},
    mmap::{mmap_batch, write_batches},
    Error, TEMPORAL_PROPS_COLUMN,
};
use arrow2::{
    array::{growable::GrowableStruct, Array, MutableArray, MutableStructArray, StructArray},
    chunk::Chunk,
    compute::{concatenate, concatenate::concatenate},
    datatypes::{DataType, Field, Schema},
};
use std::{cmp::min, path::PathBuf};

pub struct EdgeFrameBuilder {
    temporal: ChunkedListArray<StructArray>,
    constant: ChunkedArray<StructArray>,

    t_props: Vec<StructArray>,
    edge_src_id: Vec<u64>,  // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>,  // the dst ids for the edge in the current chunk
    edge_offsets: Vec<i64>, // the offsets of the edge list for the current chunk

    t_props_len: usize, // where in the current chunk are we positioned?
    no_edge_updates: usize,
    pub(crate) last_update: Option<(u64, u64)>,
    edge_count: usize,
    location_path: PathBuf,
}

impl EdgeFrameBuilder {
    fn new(temporal_chunk_size: usize, constant_chunk_size: usize) -> EdgeFrameBuilder {
        Self {
            temporal: ChunkedListArray::new(temporal_chunk_size),
            constant: ChunkedArray::new(constant_chunk_size),
            t_props: vec![],
            edge_src_id: vec![],
            edge_dst_id: vec![],
            edge_offsets: vec![],
            t_props_len: 0,
            no_edge_updates: 0,
            last_update: None,
            edge_count: 0,
            location_path: Default::default(),
        }
    }

    pub fn extend_tprops_slice(&mut self, copy_from: &StructArray) -> Result<(), Error> {
        if self.t_props_len > 0 {
            let first_len = min(
                self.temporal.chunk_size() - self.t_props_len,
                copy_from.len(),
            );
            let first = copy_from.clone().sliced(0, first_len);
            self.t_props_len += first.len();
            self.t_props.push(first);

            if self.t_props_len == self.temporal.chunk_size() {
                let mut refs: Vec<&dyn Array> = Vec::with_capacity(self.t_props.len());
                for v in self.t_props.iter() {
                    refs.push(v);
                }
                let chunk = concatenate(&refs)?;
                let chunk: StructArray = chunk
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone();
                self.push_temporal(chunk)?;
                self.t_props.clear();
                self.t_props_len = 0;
            }
            if first_len < copy_from.len() {
                let remainder = copy_from
                    .clone()
                    .sliced(first_len, copy_from.len() - first_len);
                self.extend_tprops_slice(&remainder)?;
            }
        } else {
            if copy_from.len() < self.temporal.chunk_size() {
                self.t_props_len += copy_from.len();
                self.t_props.push(copy_from.clone());
            } else {
                let first = copy_from.clone().sliced(0, self.temporal.chunk_size());
                self.push_temporal(first)?;
                if copy_from.len() > self.temporal.chunk_size() {
                    let remainder = copy_from.clone().sliced(
                        self.temporal.chunk_size(),
                        copy_from.len() - self.temporal.chunk_size(),
                    );
                    self.extend_tprops_slice(&remainder)?;
                }
            }
        }
        Ok(())
    }

    fn push_temporal(&mut self, chunk: StructArray) -> Result<(), Error> {
        let (fields, values, _) = chunk.into_data();
        let schema = Schema::from(fields.clone());
        let file_path = self
            .location_path
            .join(format!("edge_chunk_{:08}.ipc", self.temporal.num_chunks()));
        write_batches(file_path.as_path(), schema, &[Chunk::new(values)])?;
        let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
        let mmapped = StructArray::new(DataType::Struct(fields), mmapped_chunk.into_arrays(), None);
        self.temporal.push_chunk(mmapped);
        Ok(())
    }
}

#[cfg(test)]
mod test {}
