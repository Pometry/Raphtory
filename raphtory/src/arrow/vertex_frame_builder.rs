use crate::arrow::{
    adj_schema,
    mmap::{mmap_batch, write_batches},
    E_COLUMN, V_COLUMN,
};
use ahash::AHashMap;
use arrow2::{
    array::{Array, ListArray, MutableListArray, MutablePrimitiveArray, MutableStructArray},
    chunk::Chunk,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    error::Result as ArrowResult,
    offset::Offsets,
};
use itertools::Itertools;
use std::path::{Path, PathBuf};

use super::{
    global_order::{self, GlobalOrder},
    vertex_chunk::VertexChunk,
    GID,
};

pub(crate) struct VertexFrameBuilder<GO: GlobalOrder> {
    pub(crate) adj_out_chunks: Vec<VertexChunk>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) global_order: GO,                 // the sorted global ids of the vertices

    adj_out_dst: Vec<u64>, // the dst of the adjacency list for the current chunk
    adj_out_eid: Vec<u64>, // the eid of the adjacency list for the current chunk
    adj_out_offsets: Vec<i64>, // the offsets of the adjacency list for the current chunk

    chunk_size: usize,
    chunk_adj_out_offset: i64,
    pub(crate) last_edge: Option<(GID, GID)>,
    last_dst_idx: usize,
    vertex_count: usize,
    e_id: u64,
    location_path: PathBuf,
}

impl<GO: GlobalOrder> VertexFrameBuilder<GO> {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, go: GO, path: P) -> Self {
        Self {
            adj_out_chunks: vec![],
            global_order: go,
            adj_out_dst: vec![],
            adj_out_eid: vec![],
            adj_out_offsets: vec![0],
            chunk_size,
            chunk_adj_out_offset: 0,
            last_edge: None,
            last_dst_idx: 0,
            vertex_count: 0,
            e_id: 0,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self, fill_size: usize) -> ArrowResult<()> {
        let mut adj_out_eid_prev = Vec::with_capacity(self.adj_out_eid.len());
        let mut adj_out_dst_prev = Vec::with_capacity(self.adj_out_dst.len());
        let mut adj_out_offsets_prev = Vec::with_capacity(self.adj_out_offsets.len());

        std::mem::swap(&mut adj_out_eid_prev, &mut self.adj_out_eid);
        std::mem::swap(&mut adj_out_dst_prev, &mut self.adj_out_dst);
        std::mem::swap(&mut adj_out_offsets_prev, &mut self.adj_out_offsets);

        // fill chunk with empty adjacency lists
        adj_out_offsets_prev.resize(fill_size + 1, self.chunk_adj_out_offset);

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

    // pub(crate) fn load_sources<ID: Into<GID> + PartialEq>(
    //     &mut self,
    //     sources: impl IntoIterator<Item = ID>,
    // ) {
    //     self.global_order = sources.into_iter().map(|id| id.into()).collect();
    //     self.global_order.maybe_sort();
    // }

    fn find_or_push_vertex(&mut self, vertex: &GID) -> usize {
        // let id = self.global_order.len();
        // if let Some(id) = self.global_order.get(vertex) {
        //     return *id as usize;
        // } else {
        //     self.global_order.insert(vertex.clone(), id as u64);
        // }
        // id
        self.global_order.find(vertex).unwrap()
    }

    pub(crate) fn push_update(&mut self, src: GID, dst: GID) -> ArrowResult<(u64, u64)> {
        if self
            .last_edge
            .as_ref()
            .filter(|(prev_src, _)| prev_src != &src)
            .is_some()
            && (self.vertex_count + 1) % self.chunk_size == 0
        {
            self.push_chunk(self.chunk_size)?;
        }

        let same_edge = self
            .last_edge
            .as_ref()
            .map(|(prev_src, prev_dst)| prev_src == &src && prev_dst == &dst)
            .unwrap_or_default();

        if !same_edge {
            self.adj_out_eid.push(self.e_id);
            self.last_dst_idx = self.find_or_push_vertex(&dst);
            self.adj_out_dst.push(self.last_dst_idx as u64);

            if let Some((prev_src, prev_dst)) = self.last_edge.as_ref() {
                // println!("prev edge = ({prev_src:?}, {prev_dst:?})");
                // println!("new edge = ({src:?}, {dst:?})");
                assert!(prev_src < &src || (prev_src == &src && prev_dst <= &dst));
                if prev_src != &src {
                    self.adj_out_offsets.push(self.chunk_adj_out_offset);
                    self.vertex_count += 1;
                }
            }

            self.e_id += 1;
            self.chunk_adj_out_offset += 1;
        }
        self.last_edge = Some((src, dst));
        Ok((self.vertex_count as u64, self.last_dst_idx as u64))
    }

    pub(crate) fn finalise_empty_chunks(&mut self) -> ArrowResult<()> {
        if self.last_edge.is_some() {
            // deal with the last chunk
            let remaining_slots_in_chunk = self.chunk_size - self.adj_out_offsets.len();
            let remaining_vertices = self.global_order.len() - self.vertex_count - 1;
            let fill_chunk_remaining = remaining_slots_in_chunk.min(remaining_vertices);
            self.push_chunk(self.adj_out_offsets.len() + fill_chunk_remaining)?;

            // deal with the rest of the vertices
            let remaining_vertices = remaining_vertices - fill_chunk_remaining;
            let remaining_chunks = remaining_vertices / self.chunk_size;
            let size_of_last_chunk = remaining_vertices % self.chunk_size;

            for _ in 0..remaining_chunks {
                self.push_chunk(self.chunk_size)?;
            }
            if size_of_last_chunk > 0 {
                self.push_chunk(size_of_last_chunk)?;
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