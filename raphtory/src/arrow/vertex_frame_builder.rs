use crate::arrow::{
    adj_schema,
    mmap::{mmap_batch, write_batches},
    Error,
};
use arrow2::{
    array::{Array, ListArray, MutableListArray, MutablePrimitiveArray, MutableStructArray},
    chunk::Chunk,
    datatypes::{Field as ArrowField, Schema as ArrowSchema},
    error::Result as ArrowResult,
    offset::Offsets,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{global_order::GlobalOrder, vertex_chunk::VertexChunk, GID};

pub(crate) struct VertexFrameBuilder<GO: GlobalOrder> {
    pub(crate) adj_out_chunks: Vec<VertexChunk>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) global_order: Arc<GO>,            // the sorted global ids of the vertices

    adj_out_dst: Vec<u64>, // the dst of the adjacency list for the current chunk
    adj_out_eid: Vec<u64>, // the eid of the adjacency list for the current chunk
    adj_out_offsets: Vec<i64>, // the offsets of the adjacency list for the current chunk

    chunk_size: usize,
    chunk_adj_out_offset: i64,
    pub(crate) last_edge: Option<(GID, GID)>,
    last_dst_idx: usize,
    last_src_idx: usize,
    last_chunk: Option<usize>,
    e_id: u64,
    location_path: PathBuf,
}

impl<GO: GlobalOrder> VertexFrameBuilder<GO> {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, go: Arc<GO>, path: P) -> Self {
        Self {
            adj_out_chunks: vec![],
            global_order: go.clone(),
            adj_out_dst: vec![],
            adj_out_eid: vec![],
            adj_out_offsets: vec![],
            chunk_size,
            chunk_adj_out_offset: 0,
            last_edge: None,
            last_dst_idx: 0,
            last_src_idx: 0,
            last_chunk: None,
            e_id: 0,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut adj_out_eid_prev = Vec::with_capacity(self.adj_out_eid.len());
        let mut adj_out_dst_prev = Vec::with_capacity(self.adj_out_dst.len());
        let mut adj_out_offsets_prev = Vec::with_capacity(self.adj_out_offsets.len());

        std::mem::swap(&mut adj_out_eid_prev, &mut self.adj_out_eid);
        std::mem::swap(&mut adj_out_dst_prev, &mut self.adj_out_dst);
        std::mem::swap(&mut adj_out_offsets_prev, &mut self.adj_out_offsets);

        // fill chunk with empty adjacency lists
        adj_out_offsets_prev.push(self.chunk_adj_out_offset);

        let col =
            new_arrow_adj_list_chunk(adj_out_dst_prev, adj_out_eid_prev, adj_out_offsets_prev);

        self.persist_and_mmap_adj_chunk(col)?;
        self.chunk_adj_out_offset = 0i64;
        Ok(())
    }

    fn persist_and_mmap_adj_chunk(&mut self, col: Box<dyn Array>) -> ArrowResult<()> {
        let dtype = col.data_type().clone();
        let schema = ArrowSchema::from(vec![ArrowField::new("adj_out", dtype, false)]);
        let file_path = self.location_path.join(format!(
            "adj_out_chunk_{:08}.ipc",
            self.adj_out_chunks.len()
        ));
        let chunk = [Chunk::try_new(vec![col])?];
        write_batches(file_path.as_path(), schema, &chunk)?;
        let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
        self.adj_out_chunks.push(VertexChunk::new(mmapped_chunk));
        Ok(())
    }

    fn find_or_push_vertex(&mut self, vertex: &GID) -> usize {
        self.global_order.find(vertex).unwrap()
    }

    pub(crate) fn push_update(&mut self, src: GID, dst: GID) -> Result<(u64, u64), Error> {
        let same_edge = self
            .last_edge
            .as_ref()
            .map(|(prev_src, prev_dst)| prev_src == &src && prev_dst == &dst)
            .unwrap_or_default();

        let not_same_source = self
            .last_edge
            .as_ref()
            .filter(|(prev_src, _)| prev_src == &src)
            .is_none();

        if !same_edge {
            if not_same_source {
                // new source or first edge
                self.last_src_idx = self.find_or_push_vertex(&src);
                let chunk_id = self.last_src_idx / self.chunk_size;
                if let Some(last_chunk) = self.last_chunk{
                    assert!(chunk_id >= last_chunk, "Chunk id {} is less than last chunk {}", chunk_id, last_chunk);
                }
                self.last_chunk = Some(chunk_id);
                // figure out what chunk are we in
                self.extend_empty(self.last_src_idx)?;
            }
            self.adj_out_eid.push(self.e_id);
            self.last_dst_idx = self.find_or_push_vertex(&dst);
            self.adj_out_dst.push(self.last_dst_idx as u64);

            self.e_id += 1;
            self.chunk_adj_out_offset += 1;
        }
        self.last_edge = Some((src, dst));
        Ok((self.last_src_idx as u64, self.last_dst_idx as u64))
    }

    fn extend_empty(&mut self, new_src: usize) -> Result<(), Error> {
        let old_chunk = self.adj_out_chunks.len();
        let new_chunk = new_src / self.chunk_size;
        for _ in old_chunk..new_chunk {
            self.adj_out_offsets
                .resize(self.chunk_size, self.chunk_adj_out_offset);
            self.push_chunk()?;
        }
        self.adj_out_offsets
            .resize((new_src % self.chunk_size) + 1, self.chunk_adj_out_offset);
        Ok(())
    }

    pub(crate) fn finalise_empty_chunks(&mut self) -> Result<(), Error> {
        if self.last_edge.is_some() {
            self.extend_empty(self.global_order.len() - 1)?;
            if !self.adj_out_offsets.is_empty() {
                self.push_chunk()?;
            }
        }
        Ok(())
    }
}

fn new_arrow_adj_list_chunk(
    adj_out_dst: Vec<u64>,
    adj_out_eid: Vec<u64>,
    adj_out_offsets: Vec<i64>,
) -> Box<dyn Array> {
    let dst_col = Box::new(MutablePrimitiveArray::<u64>::from_vec(adj_out_dst));
    let eid_col = Box::new(MutablePrimitiveArray::<u64>::from_vec(adj_out_eid));

    let values = MutableStructArray::new(adj_schema(), vec![dst_col, eid_col]);

    let outbound2 = MutableListArray::new_from_mutable(
        values,
        Offsets::try_from(adj_out_offsets).unwrap(),
        None,
    );

    let outbound: ListArray<i64> = outbound2.into();
    Box::new(outbound)
}
