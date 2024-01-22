use crate::arrow::{
    edge,
    mmap::{mmap_batch, mmap_buffer, write_batches, write_buffer},
    node_builder::LoadingState,
    DST_COLUMN, SRC_COLUMN,
};
use arrow2::{
    array::PrimitiveArray,
    buffer::Buffer,
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result as ArrowResult,
    offset::OffsetsBuffer,
    types::NativeType,
};
use std::path::{Path, PathBuf};

use crate::arrow::Error;

pub struct EdgeFrameBuilder {
    pub(crate) src_chunks: Vec<PrimitiveArray<u64>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) dst_chunks: Vec<PrimitiveArray<u64>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}

    pub(crate) src_offsets: Vec<OffsetsBuffer<i64>>,
    pub(crate) edge_offsets: Vec<OffsetsBuffer<i64>>,

    edge_src_id: Vec<u64>, // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>, // the dst ids for the edge in the current chunk

    cur_src_offset: Vec<i64>,
    cur_edge_offset: Vec<i64>,

    chunk_size: usize,
    pub(crate) last_update: Option<(u64, u64)>,
    location_path: PathBuf,
}

impl EdgeFrameBuilder {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, path: P) -> Self {
        Self {
            src_chunks: vec![],
            dst_chunks: vec![],

            src_offsets: vec![],
            edge_offsets: vec![],

            edge_src_id: vec![],
            edge_dst_id: vec![],

            cur_src_offset: vec![0],
            cur_edge_offset: vec![0],

            chunk_size,
            last_update: None,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    pub(crate) fn push_update_state(&mut self, state: LoadingState) -> Result<(), Error> {
        let LoadingState {
            deduped_src_ids,
            deduped_dst_ids,
            edge_counts,
            src_counts,
        } = state;

        // check bounds
        let first = deduped_src_ids
            .first()
            .copied()
            .zip(deduped_dst_ids.first().copied())
            .ok_or(Error::EmptyChunk)?;

        let edge_counts = if self.last_update == Some(first) {
            let first_count = edge_counts[0];
            *self.cur_edge_offset.last_mut().unwrap() += first_count as i64;
            &edge_counts[1..]
        } else {
            &edge_counts
        };

        let src_counts = if self.last_update.map(|(src, _)| src) == Some(first.0) {
            let first_count = src_counts[0];
            *self.cur_src_offset.last_mut().unwrap() += first_count as i64;
            &src_counts[1..]
        } else {
            &src_counts
        };

        self.update_src_offsets(&src_counts)?;

        self.update_edge_offsets(&edge_counts)?;

        self.last_update = deduped_src_ids
            .last()
            .copied()
            .zip(deduped_dst_ids.last().copied());
        Ok(())
    }

    fn push_src_offset_chunk(&mut self) -> Result<(), Error> {
        let mut new_src_offset = Vec::with_capacity(self.cur_src_offset.len());
        std::mem::swap(&mut new_src_offset, &mut self.cur_src_offset);

        let last_elem = new_src_offset.pop().ok_or(Error::EmptyChunk)?;
        self.cur_src_offset.push(last_elem);

        let id = self.cur_src_offset.len();

        let file_path = self
            .location_path
            .join(format!("src_offsets_{:08}.ipc", id));

        write_buffer(&file_path, Buffer::from(new_src_offset))?;

        let buffer = unsafe { mmap_buffer::<i64>(file_path, 0)? };
        self.src_offsets
            .push(unsafe { OffsetsBuffer::new_unchecked(buffer) });

        Ok(())
    }

    fn push_edge_offset_chunk(&self) -> Result<(), Error> {
        todo!()
    }

    fn update_src_offsets(&mut self, src_counts: &[usize]) -> Result<(), Error> {
        let src_off_remaining = self.chunk_size - self.cur_src_offset.len() + 1;
        extend_offsets(&mut self.cur_src_offset, &src_counts[0..src_off_remaining]);

        if self.cur_src_offset.len() == self.chunk_size + 1 {
            self.push_src_offset_chunk()?;
            self.update_src_offsets(&src_counts[src_off_remaining..])?;
        }
        Ok(())
    }

    fn update_edge_offsets(&mut self, edge_counts: &[usize]) -> Result<(), Error> {
        let edge_off_remaining = self.chunk_size - self.cur_edge_offset.len() + 1;
        extend_offsets(
            &mut self.cur_edge_offset,
            &edge_counts[0..edge_off_remaining],
        );

        if self.cur_edge_offset.len() == self.chunk_size + 1 {
            self.push_edge_offset_chunk()?;
            self.update_edge_offsets(&edge_counts[edge_off_remaining..])?;
        }

        Ok(())
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

        self.src_chunks.push(src.clone());
        self.dst_chunks.push(dst.clone());

        Ok(())
    }

    pub(crate) fn finalize(&mut self) -> Result<(), Error> {
        if self.edge_src_id.len() > 0 {
            self.push_chunk()?;
        }
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

fn extend_offsets<'a, I: IntoIterator<Item = &'a usize>>(offsets: &mut Vec<i64>, counts: I) {
    let last_value = *offsets.last().unwrap(); // FIXME: could initialise here?
    offsets.extend(counts.into_iter().scan(last_value, |state, v| {
        let new = *v as i64 + *state;
        *state = new;
        Some(new)
    }));
}
