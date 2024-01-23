use crate::arrow::{
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
};
use itertools::{EitherOrBoth, Itertools};
use std::{
    cmp::min,
    mem,
    path::{Path, PathBuf},
};

use crate::arrow::Error;

#[derive(Debug)]
pub struct EdgeFrameBuilder {
    pub(crate) src_chunks: Vec<PrimitiveArray<u64>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) dst_chunks: Vec<PrimitiveArray<u64>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}

    pub(crate) adj_out_offsets: Vec<OffsetsBuffer<i64>>,
    pub(crate) edge_offsets: Vec<OffsetsBuffer<i64>>,

    edge_src_id: Vec<u64>, // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>, // the dst ids for the edge in the current chunk

    cur_adj_out_offset: Vec<i64>,
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

            adj_out_offsets: vec![],
            edge_offsets: vec![],

            edge_src_id: vec![],
            edge_dst_id: vec![],

            cur_adj_out_offset: vec![0],
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

        if self.last_update == Some(first) {
            let first_count = edge_counts[0];
            *self.cur_edge_offset.last_mut().unwrap() += first_count as i64;
            self.update_src_dst(&deduped_src_ids[1..], &deduped_dst_ids[1..])?;
            self.update_edge_offsets(&edge_counts[1..])?;
        } else {
            self.update_src_dst(&deduped_src_ids, &deduped_dst_ids)?;
            self.update_edge_offsets(&edge_counts)?;
        };

        if self.last_update.map(|(src, _)| src) == Some(first.0) {
            let first_count = src_counts[0];
            *self.cur_adj_out_offset.last_mut().unwrap() += first_count as i64;
            self.update_adj_out_offsets(&deduped_src_ids[1..], &src_counts[1..])?;
        } else {
            self.update_adj_out_offsets(&deduped_src_ids, &src_counts)?;
        };

        self.last_update = deduped_src_ids
            .last()
            .copied()
            .zip(deduped_dst_ids.last().copied());
        Ok(())
    }

    fn push_adj_out_offset_chunk(&mut self) -> Result<(), Error> {
        let mut new_adj_out_offset = Vec::with_capacity(self.cur_adj_out_offset.len());
        mem::swap(&mut new_adj_out_offset, &mut self.cur_adj_out_offset);

        let incomplete = new_adj_out_offset.pop().ok_or(Error::EmptyChunk)?;
        if let Some(last_elem) = new_adj_out_offset.last() {
            self.cur_adj_out_offset.push(*last_elem);
        }
        self.cur_adj_out_offset.push(incomplete);

        self.persist_adj_out_offset_chunk(new_adj_out_offset)?;

        Ok(())
    }

    fn persist_adj_out_offset_chunk(&mut self, new_adj_out_offset: Vec<i64>) -> Result<(), Error> {
        let id = self.adj_out_offsets.len();

        let file_path = self
            .location_path
            .join(format!("adj_out_offsets_{:08}.ipc", id));

        write_buffer(&file_path, Buffer::from(new_adj_out_offset))?;

        let buffer = unsafe { mmap_buffer::<i64>(file_path, 0)? };
        self.adj_out_offsets
            .push(unsafe { OffsetsBuffer::new_unchecked(buffer) });
        Ok(())
    }

    fn push_edge_offset_chunk(&mut self) -> Result<(), Error> {
        let mut new_edge_offset = Vec::with_capacity(self.cur_edge_offset.len());
        mem::swap(&mut new_edge_offset, &mut self.cur_edge_offset);

        let incomplete = new_edge_offset.pop().ok_or(Error::EmptyChunk)?;
        if let Some(last_elem) = new_edge_offset.last() {
            self.cur_edge_offset.push(*last_elem);
        }
        self.cur_edge_offset.push(incomplete);

        self.persist_edge_offset_chunk(new_edge_offset)?;

        Ok(())
    }

    fn persist_edge_offset_chunk(&mut self, new_edge_offset: Vec<i64>) -> Result<(), Error> {
        let id = self.edge_offsets.len();

        let file_path = self
            .location_path
            .join(format!("edge_offsets_{:08}.ipc", id));

        write_buffer(&file_path, Buffer::from(new_edge_offset))?;

        let buffer = unsafe { mmap_buffer::<i64>(file_path, 0)? };
        self.edge_offsets
            .push(unsafe { OffsetsBuffer::new_unchecked(buffer) });
        Ok(())
    }

    fn update_adj_out_offsets(
        &mut self,
        deduped_src_ids: &[u64],
        src_counts: &[usize],
    ) -> Result<(), Error> {
        let next_id = self.last_update.map(|(src, _)| src + 1).unwrap_or(0);
        let mut last_offset = *self.cur_adj_out_offset.last().unwrap();
        let all_nodes = next_id..=deduped_src_ids.last().copied().unwrap_or(0);
        for merged in all_nodes.merge_join_by(
            deduped_src_ids.iter().dedup().zip(src_counts),
            |left_id, (right_id, _)| left_id.cmp(right_id),
        ) {
            println!("{merged:?}");
            match merged {
                EitherOrBoth::Both(_, (_, count)) => {
                    last_offset += *count as i64;
                }
                EitherOrBoth::Right((_, count)) => {
                    last_offset += *count as i64;
                }
                EitherOrBoth::Left(_) => {}
            }
            self.cur_adj_out_offset.push(last_offset);

            if self.cur_adj_out_offset.len() == self.chunk_size + 2 {
                self.push_adj_out_offset_chunk()?;
            }
        }
        Ok(())
    }

    fn update_edge_offsets(&mut self, edge_counts: &[usize]) -> Result<(), Error> {
        let edge_off_remaining = min(
            self.chunk_size + 2 - self.cur_edge_offset.len(),
            edge_counts.len(),
        );
        extend_offsets(
            &mut self.cur_edge_offset,
            &edge_counts[0..edge_off_remaining],
        );

        if self.cur_edge_offset.len() == self.chunk_size + 2 {
            self.push_edge_offset_chunk()?;
            self.update_edge_offsets(&edge_counts[edge_off_remaining..])?;
        }

        Ok(())
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut edge_src_id_prev = Vec::with_capacity(self.edge_src_id.len());
        let mut edge_dst_id_prev = Vec::with_capacity(self.edge_dst_id.len());

        mem::swap(&mut edge_src_id_prev, &mut self.edge_src_id);
        mem::swap(&mut edge_dst_id_prev, &mut self.edge_dst_id);

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
        let adj_out_offsets = mem::take(&mut self.cur_adj_out_offset);
        self.persist_adj_out_offset_chunk(adj_out_offsets)?;
        let edge_offsets = mem::take(&mut self.cur_edge_offset);
        self.persist_edge_offset_chunk(edge_offsets)?;
        Ok(())
    }

    pub(crate) fn update_src_dst(&mut self, src_ids: &[u64], dst_ids: &[u64]) -> Result<(), Error> {
        for (src_id, dst_id) in src_ids.iter().zip(dst_ids) {
            self.edge_src_id.push(*src_id);
            self.edge_dst_id.push(*dst_id);
            if self.edge_src_id.len() == self.chunk_size {
                self.push_chunk()?;
            }
        }
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

#[cfg(test)]
mod test {
    use arrow2::{array::PrimitiveArray, buffer::Buffer, offset::OffsetsBuffer};

    use crate::arrow::node_builder::LoadingState;

    #[test]
    fn load_1_edge() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut edge_builder = super::EdgeFrameBuilder::new(2, tempdir.path());

        let state = LoadingState {
            deduped_src_ids: vec![0],
            deduped_dst_ids: vec![1],
            edge_counts: vec![1],
            src_counts: vec![1],
        };

        edge_builder
            .push_update_state(state)
            .expect("push update state");

        edge_builder.finalize().expect("finalize");

        assert_eq!(
            edge_builder.src_chunks,
            vec![PrimitiveArray::from_vec(vec![0u64])]
        );
        assert_eq!(
            edge_builder.dst_chunks,
            vec![PrimitiveArray::from_vec(vec![1u64])]
        );

        let expected: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 1])) };
        assert_eq!(edge_builder.adj_out_offsets, vec![expected.clone()]);
        assert_eq!(edge_builder.edge_offsets, vec![expected]);
    }

    #[test]
    fn load_3_edges_fit_chunk_size() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut edge_builder = super::EdgeFrameBuilder::new(4, tempdir.path());

        let state = LoadingState {
            deduped_src_ids: vec![0, 0, 1],
            deduped_dst_ids: vec![1, 2, 2],
            edge_counts: vec![2, 1, 1],
            src_counts: vec![2, 1],
        };

        edge_builder
            .push_update_state(state)
            .expect("push update state");

        edge_builder.finalize().expect("finalize");

        assert_eq!(
            edge_builder.src_chunks,
            vec![PrimitiveArray::from_vec(vec![0u64, 0u64, 1u64])]
        );
        assert_eq!(
            edge_builder.dst_chunks,
            vec![PrimitiveArray::from_vec(vec![1u64, 2u64, 2u64])]
        );

        let edge_offsets: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 2, 3, 4])) };
        assert_eq!(edge_builder.edge_offsets, vec![edge_offsets]);

        let adj_out_offsets: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 2, 3])) };
        assert_eq!(edge_builder.adj_out_offsets, vec![adj_out_offsets]);
    }

    #[test]
    fn load_edges_2_chunks_no_dups() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut edge_builder = super::EdgeFrameBuilder::new(2, tempdir.path());

        let state1 = LoadingState {
            deduped_src_ids: vec![0, 0],
            deduped_dst_ids: vec![0, 1],
            edge_counts: vec![1, 1],
            src_counts: vec![2],
        };

        let state2 = LoadingState {
            deduped_src_ids: vec![0, 1],
            deduped_dst_ids: vec![2, 2],
            edge_counts: vec![1, 1],
            src_counts: vec![1, 1],
        };

        let states = vec![state1, state2];

        for state in states {
            edge_builder
                .push_update_state(state)
                .expect("push update state");
        }

        edge_builder.finalize().expect("finalize");

        assert_eq!(
            edge_builder.src_chunks,
            vec![
                PrimitiveArray::from_vec(vec![0u64, 0u64]),
                PrimitiveArray::from_vec(vec![0u64, 1u64])
            ]
        );
        assert_eq!(
            edge_builder.dst_chunks,
            vec![
                PrimitiveArray::from_vec(vec![0u64, 1u64]),
                PrimitiveArray::from_vec(vec![2u64, 2u64])
            ]
        );

        let edge_offsets1: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 1, 2])) };
        let edge_offsets2: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![2, 3, 4])) };

        assert_eq!(
            edge_builder.edge_offsets,
            vec![edge_offsets1, edge_offsets2]
        );

        let adj_out_offsets1: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 3, 4])) };

        let adj_out_offsets2 = unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![4i64, 4])) };

        assert_eq!(
            edge_builder.adj_out_offsets,
            vec![adj_out_offsets1, adj_out_offsets2]
        );
    }
}
