use crate::arrow::{
    col_graph2::Time,
    mmap::{mmap_batch, write_batches},
    DST_COLUMN, E_ADDITIONS_COLUMN, SRC_COLUMN,
};
use arrow2::{
    array::{Array, ListArray, MutableArray, PrimitiveArray},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result as ArrowResult,
    offset::OffsetsBuffer,
};
use std::path::{Path, PathBuf};

use super::{Error, LoadChunk};

pub struct EdgeFrameBuilder {
    pub(crate) edge_chunks: Vec<Chunk<Box<dyn Array>>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}

    edge_timestamps: Vec<Time>, // the timestamps of the edge for the current chunk
    edge_src_id: Vec<u64>,      // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>,      // the dst ids for the edge in the current chunk
    edge_offsets: Vec<i64>,     // the offsets of the edge list for the current chunk

    in_chunk_offset: usize, // where in the current chunk are we positioned?

    chunk_size: usize,
    chunk_offset: i64,
    pub(crate) last_update: Option<(u64, u64)>,
    edge_count: usize,
    location_path: PathBuf,
}

impl EdgeFrameBuilder {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, path: P) -> Self {
        Self {
            edge_chunks: vec![],
            edge_timestamps: vec![],
            edge_src_id: vec![],
            edge_dst_id: vec![],
            edge_offsets: vec![],
            in_chunk_offset: 0,
            chunk_size,
            chunk_offset: 0,
            last_update: None,
            edge_count: 0,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut edge_timestamps_prev = Vec::with_capacity(self.edge_timestamps.len());
        let mut edge_src_id_prev = Vec::with_capacity(self.edge_timestamps.len());
        let mut edge_dst_id_prev = Vec::with_capacity(self.edge_timestamps.len());
        let mut edge_offsets_prev = Vec::with_capacity(self.edge_offsets.len());

        std::mem::swap(&mut edge_timestamps_prev, &mut self.edge_timestamps);
        std::mem::swap(&mut edge_src_id_prev, &mut self.edge_src_id);
        std::mem::swap(&mut edge_dst_id_prev, &mut self.edge_dst_id);
        std::mem::swap(&mut edge_offsets_prev, &mut self.edge_offsets);

        // fill chunk with empty adjacency lists
        edge_offsets_prev.push(self.chunk_offset);

        let col = new_arrow_edge_list_chunk(
            edge_timestamps_prev,
            edge_src_id_prev,
            edge_dst_id_prev,
            edge_offsets_prev,
        )?;

        self.persist_and_mmap_adj_chunk(col)?;
        self.chunk_offset = 0i64;
        Ok(())
    }

    fn persist_and_mmap_adj_chunk(&mut self, chunk: Chunk<Box<dyn Array>>) -> ArrowResult<()> {
        let schema = Schema::from(vec![
            Field::new(SRC_COLUMN, chunk[0].data_type().clone(), false),
            Field::new(DST_COLUMN, chunk[1].data_type().clone(), false),
            Field::new(E_ADDITIONS_COLUMN, chunk[2].data_type().clone(), false),
        ]);
        let file_path = self
            .location_path
            .join(format!("edge_chunk_{:08}.ipc", self.edge_chunks.len()));
        write_batches(file_path.as_path(), schema, &[chunk])?;
        let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
        self.edge_chunks.push(mmapped_chunk);
        Ok(())
    }

    pub(crate) fn push_update_with_props(
        &mut self,
        src: u64,
        dst: u64,
        chunk: &mut LoadChunk,
    ) -> Result<(), Error> {
        if self
            .last_update
            .filter(|(prev_src, prev_dst)| prev_src != &src || prev_dst != &dst)
            .is_some()
            || self.last_update.is_none()
        {
            if (self.edge_count + 1) % self.chunk_size == 0 && self.last_update.is_some() {
                self.push_chunk_v2(chunk)?;
            }
            self.edge_src_id.push(src);
            self.edge_dst_id.push(dst);
            self.edge_offsets.push(self.chunk_offset);
            self.edge_count += 1;
        }

        self.in_chunk_offset += 1;
        self.chunk_offset += 1;
        self.last_update = Some((src, dst));
        Ok(())
    }

    pub(crate) fn extend_time_slice(&mut self, arr: &PrimitiveArray<i64>){
        let time_slice = arr.values();
        self.edge_timestamps.extend_from_slice(time_slice);
        self.in_chunk_offset = 0;
    }

    pub(crate) fn push_chunk_v2(&mut self, load_chunk: &mut LoadChunk) -> ArrowResult<()> {
        let arr = load_chunk.split_timestamps_at(self.in_chunk_offset);
        self.extend_time_slice(&arr);
        self.push_chunk()?;
        Ok(())
    }

}

fn new_arrow_edge_list_chunk(
    edge_timestamps: Vec<Time>,
    edge_src_ids: Vec<u64>,
    edge_dst_ids: Vec<u64>,
    edge_offsets: Vec<i64>,
) -> ArrowResult<Chunk<Box<dyn Array>>> {
    let timestamp_values = PrimitiveArray::from_vec(edge_timestamps);
    let dtype = <ListArray<i64>>::default_datatype(DataType::Int64);
    let offsets = OffsetsBuffer::try_from(edge_offsets)?;

    let timestamps: Box<dyn Array> = Box::new(ListArray::new(
        dtype,
        offsets,
        Box::new(timestamp_values),
        None,
    ));
    let src_col: Box<dyn Array> = Box::new(PrimitiveArray::from_vec(edge_src_ids));
    let dst_col: Box<dyn Array> = Box::new(PrimitiveArray::from_vec(edge_dst_ids));

    Ok(Chunk::new(vec![src_col, dst_col, timestamps]))
}
