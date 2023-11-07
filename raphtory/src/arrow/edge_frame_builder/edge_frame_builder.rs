use crate::arrow::{
    chunked_array::chunked_array::ChunkedArray,
    mmap::{mmap_batch, write_batches},
    DST_COLUMN, SRC_COLUMN,
};
use arrow2::{
    array::PrimitiveArray,
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result as ArrowResult,
};
use std::path::{Path, PathBuf};

use crate::arrow::{Error, LoadChunk};

pub struct EdgeFrameBuilder {
    pub(crate) src_chunks: ChunkedArray<PrimitiveArray<u64>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) dst_chunks: ChunkedArray<PrimitiveArray<u64>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}

    edge_src_id: Vec<u64>, // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>, // the dst ids for the edge in the current chunk

    chunk_size: usize,
    pub(crate) last_update: Option<(u64, u64)>,
    location_path: PathBuf,
}

impl EdgeFrameBuilder {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, max_list_size: usize, path: P) -> Self {
        Self {
            src_chunks: ChunkedArray::new(chunk_size),
            dst_chunks: ChunkedArray::new(chunk_size),
            edge_src_id: vec![],
            edge_dst_id: vec![],
            chunk_size,
            last_update: None,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut edge_src_id_prev = Vec::with_capacity(self.edge_src_id.len());
        let mut edge_dst_id_prev = Vec::with_capacity(self.edge_dst_id.len());

        std::mem::swap(&mut edge_src_id_prev, &mut self.edge_src_id);
        std::mem::swap(&mut edge_dst_id_prev, &mut self.edge_dst_id);

        self.persist_and_mmap_adj_chunk(edge_src_id_prev, edge_dst_id_prev)?;
        Ok(())
    }

    fn persist_and_mmap_adj_chunk(&mut self, src: Vec<u64>, dst: Vec<u64>) -> ArrowResult<()> {
        let schema = Schema::from(vec![
            Field::new(SRC_COLUMN, DataType::UInt64, false),
            Field::new(DST_COLUMN, DataType::UInt64, false),
        ]);
        let file_path = self
            .location_path
            .join(format!("edge_ids_{:08}.ipc", self.src_chunks.len()));
        let chunk = Chunk::new(vec![
            PrimitiveArray::from_vec(src).boxed(),
            PrimitiveArray::from_vec(dst).boxed(),
        ]);
        write_batches(file_path.as_path(), schema, &[chunk])?;
        let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
        let src = mmapped_chunk[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        let dst = mmapped_chunk[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();

        self.src_chunks.push_chunk(src.clone());
        self.dst_chunks.push_chunk(dst.clone());

        Ok(())
    }

    pub(crate) fn write_down_chunk(&mut self, load_chunk: &mut LoadChunk) -> Result<(), Error> {
        self.push_chunk()?;
        Ok(())
    }

    pub(crate) fn finalize(&mut self, load_chunk: &mut LoadChunk) -> Result<(), Error> {
        self.push_chunk()?;
        Ok(())
    }

    pub(crate) fn push_update(&mut self, src_id: u64, dst_id: u64) -> Result<(), Error> {
        if self.last_update != Some((src_id, dst_id)) {
            self.edge_src_id.push(src_id);
            self.edge_dst_id.push(dst_id);

            if self.edge_src_id.len() == self.chunk_size {
                self.push_chunk()?;
            }
        }

        self.last_update = Some((src_id, dst_id));
        Ok(())
    }
}
