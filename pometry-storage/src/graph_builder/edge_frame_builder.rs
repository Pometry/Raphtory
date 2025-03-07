use crate::{
    arrow2::{array::PrimitiveArray, buffer::Buffer, offset::OffsetsBuffer},
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::ChunkedOffsets,
        mutable_chunked_array::MutPrimitiveChunkedArray,
    },
    file_prefix::GraphPaths,
    load::{
        mmap::{mmap_buffer, write_buffer},
        parquet_source::LoadingState,
    },
    RAError,
};
use itertools::{EitherOrBoth, Itertools};
#[cfg(feature = "progress")]
use kdam::{BarBuilder, BarExt};
use std::{
    cmp::min,
    fs::File,
    mem,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct EdgeFrameBuilder {
    pub(crate) src_chunks: MutPrimitiveChunkedArray<u64>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) dst_chunks: MutPrimitiveChunkedArray<u64>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}

    pub(crate) adj_out_offsets: Vec<OffsetsBuffer<i64>>,
    pub(crate) edge_offsets: Vec<OffsetsBuffer<i64>>,

    cur_adj_out_offset: Vec<i64>,
    cur_edge_offset: Vec<i64>,

    chunk_size: usize,
    pub(crate) last_update: Option<(u64, u64)>,
    location_path: PathBuf,

    tmp_location_path: Option<PathBuf>,

    #[cfg(feature = "progress")]
    pb: kdam::Bar,
}

impl EdgeFrameBuilder {
    pub(crate) fn new<P: AsRef<Path>>(
        chunk_size: usize,
        path: P,
        tmp_location_path: Option<&Path>,
    ) -> Self {
        let path = path.as_ref();
        Self {
            src_chunks: MutPrimitiveChunkedArray::new_persisted(
                chunk_size,
                path,
                GraphPaths::AdjOutSrcs,
            ),
            dst_chunks: MutPrimitiveChunkedArray::new_persisted(
                chunk_size,
                path,
                GraphPaths::AdjOutDsts,
            ),

            adj_out_offsets: vec![],
            edge_offsets: vec![],

            cur_adj_out_offset: vec![0],
            cur_edge_offset: vec![0],

            chunk_size,
            last_update: None,
            location_path: path.to_path_buf(),
            tmp_location_path: tmp_location_path.map(|p| p.to_path_buf()),

            #[cfg(feature = "progress")]
            pb: BarBuilder::default()
                .desc(format!("Adj List {path:?}"))
                .build()
                .unwrap(),
        }
    }

    pub(crate) fn push_update_state(&mut self, state: LoadingState) -> Result<(), RAError> {
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
            .ok_or(RAError::EmptyChunk)?;

        if self.last_update == Some(first) {
            let first_count = edge_counts[0];
            *self.cur_edge_offset.last_mut().unwrap() += first_count as i64;
            self.update_src_dst(&deduped_src_ids[1..], &deduped_dst_ids[1..])?;
            self.update_edge_offsets(&edge_counts[1..])?;
        } else {
            self.update_src_dst(&deduped_src_ids, &deduped_dst_ids)?;
            self.update_edge_offsets(&edge_counts)?;
        };

        let (first_src, first_count) = src_counts[0];
        if self.last_update.map(|(src, _)| src) == Some(first_src) {
            if self.last_update != Some(first) {
                *self.cur_adj_out_offset.last_mut().unwrap() += first_count as i64;
            } else {
                // we have already counted the first edge in the chunk so we need to subtract 1
                *self.cur_adj_out_offset.last_mut().unwrap() +=
                    (first_count as i64).saturating_sub(1);
            }
            self.update_adj_out_offsets(&src_counts[1..])?;
        } else {
            self.update_adj_out_offsets(&src_counts)?;
        };

        self.last_update = deduped_src_ids
            .last()
            .copied()
            .zip(deduped_dst_ids.last().copied());
        Ok(())
    }

    fn push_adj_out_offset_chunk(&mut self) -> Result<(), RAError> {
        let mut new_adj_out_offset = Vec::with_capacity(self.cur_adj_out_offset.len());
        mem::swap(&mut new_adj_out_offset, &mut self.cur_adj_out_offset);

        let incomplete = new_adj_out_offset.pop().ok_or(RAError::EmptyChunk)?;
        if let Some(last_elem) = new_adj_out_offset.last() {
            self.cur_adj_out_offset.push(*last_elem);
        }
        self.cur_adj_out_offset.push(incomplete);

        self.persist_adj_out_offset_chunk(new_adj_out_offset)?;

        Ok(())
    }

    fn persist_adj_out_offset_chunk(
        &mut self,
        new_adj_out_offset: Vec<i64>,
    ) -> Result<(), RAError> {
        let id = self.adj_out_offsets.len();

        let file_path = GraphPaths::AdjOutOffsets.to_path(&self.location_path, id);

        write_buffer(
            File::create_new(&file_path)?,
            Buffer::from(new_adj_out_offset),
        )?;

        let buffer = unsafe { mmap_buffer::<i64>(file_path, 0)? };
        self.adj_out_offsets
            .push(unsafe { OffsetsBuffer::new_unchecked(buffer) });
        Ok(())
    }

    fn push_edge_offset_chunk(&mut self) -> Result<(), RAError> {
        let mut new_edge_offset = Vec::with_capacity(self.cur_edge_offset.len());
        mem::swap(&mut new_edge_offset, &mut self.cur_edge_offset);

        let incomplete = new_edge_offset.pop().ok_or(RAError::EmptyChunk)?;
        if let Some(last_elem) = new_edge_offset.last() {
            self.cur_edge_offset.push(*last_elem);
        }
        self.cur_edge_offset.push(incomplete);

        self.persist_edge_offset_chunk(new_edge_offset)?;

        Ok(())
    }

    fn persist_edge_offset_chunk(&mut self, new_edge_offset: Vec<i64>) -> Result<(), RAError> {
        let id = self.edge_offsets.len();

        let graph_path = self
            .tmp_location_path
            .as_ref()
            .unwrap_or(&self.location_path);

        let file_path = GraphPaths::EdgeTPropsOffsets.to_path(graph_path, id);

        write_buffer(File::create_new(&file_path)?, Buffer::from(new_edge_offset))?;

        let buffer = unsafe { mmap_buffer::<i64>(file_path, 0)? };
        self.edge_offsets
            .push(unsafe { OffsetsBuffer::new_unchecked(buffer) });
        Ok(())
    }

    fn update_adj_out_offsets(&mut self, src_counts: &[(u64, usize)]) -> Result<(), RAError> {
        let next_id = self.last_update.map(|(src, _)| src + 1).unwrap_or(0);
        let mut last_offset = *self.cur_adj_out_offset.last().unwrap();
        let all_nodes = next_id..=src_counts.last().map(|(src, _)| *src).unwrap_or(0);
        for merged in
            all_nodes.merge_join_by(src_counts, |left_id, (right_id, _)| left_id.cmp(right_id))
        {
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

    fn update_edge_offsets(&mut self, edge_counts: &[usize]) -> Result<(), RAError> {
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

    pub(crate) fn finalize(mut self, num_nodes: usize) -> Result<EFBResult, RAError> {
        if self.last_update.map(|(src, _)| src as usize) != Some(num_nodes.saturating_sub(1)) {
            self.update_adj_out_offsets(&[((num_nodes - 1) as u64, 0)])?;
        }

        let adj_out_offsets = mem::take(&mut self.cur_adj_out_offset);
        self.persist_adj_out_offset_chunk(adj_out_offsets)?;
        let edge_offsets = mem::take(&mut self.cur_edge_offset);
        self.persist_edge_offset_chunk(edge_offsets)?;

        Ok(EFBResult {
            adj_out_offsets: ChunkedOffsets::new(self.adj_out_offsets, self.chunk_size),
            src_chunks: self.src_chunks.finish()?,
            dst_chunks: self.dst_chunks.finish()?,
            edge_offsets: ChunkedOffsets::new(self.edge_offsets, self.chunk_size),
        })
    }

    pub(crate) fn update_src_dst(
        &mut self,
        src_ids: &[u64],
        dst_ids: &[u64],
    ) -> Result<(), RAError> {
        for (src_id, dst_id) in src_ids.iter().zip(dst_ids) {
            self.src_chunks.push(*src_id)?;
            self.dst_chunks.push(*dst_id)?;
        }
        #[cfg(feature = "progress")]
        self.pb.update(src_ids.len()).unwrap();
        Ok(())
    }
}
#[derive(Clone)]
pub struct EFBResult {
    pub(crate) adj_out_offsets: ChunkedOffsets,
    pub(crate) src_chunks: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    pub(crate) dst_chunks: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    pub(crate) edge_offsets: ChunkedOffsets,
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
    use crate::{
        arrow2::{array::PrimitiveArray, buffer::Buffer, offset::OffsetsBuffer},
        chunked_array::{chunked_array::ChunkedArray, chunked_offsets::ChunkedOffsets},
        graph_builder::EFBResult,
        load::parquet_source::LoadingState,
        prelude::{ArrayOps, BaseArrayOps},
    };
    use itertools::Itertools;

    #[test]
    fn load_1_edge() {
        let tempdir = tempfile::tempdir().unwrap();
        let chunk_size = 2;
        let mut edge_builder = super::EdgeFrameBuilder::new(chunk_size, tempdir.path(), None);

        let state = LoadingState {
            deduped_src_ids: vec![0],
            deduped_dst_ids: vec![1],
            edge_counts: vec![1],
            src_counts: vec![(0, 1)],
        };

        edge_builder
            .push_update_state(state)
            .expect("push update state");

        let EFBResult {
            adj_out_offsets,
            src_chunks,
            dst_chunks,
            edge_offsets,
        } = edge_builder.finalize(2).expect("finalize");

        assert_eq!(src_chunks.iter().collect_vec(), [0u64]);
        assert_eq!(&*src_chunks.chunks, [PrimitiveArray::from_vec(vec![0u64])]);
        assert_eq!(dst_chunks.iter().collect_vec(), [1u64]);
        assert_eq!(&*dst_chunks.chunks, [PrimitiveArray::from_vec(vec![1u64])]);

        let expected_adj_out = vec![Buffer::from(vec![0, 1, 1]), Buffer::from(vec![1])];
        let expected_adj_out = expected_adj_out
            .into_iter()
            .map(|buf| unsafe { OffsetsBuffer::new_unchecked(buf) })
            .collect::<Vec<_>>();
        assert_eq!(adj_out_offsets, ChunkedOffsets::new(expected_adj_out, 2));

        let expected_edge: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 1])) };
        assert_eq!(edge_offsets, vec![expected_edge].into());
    }

    #[test]
    fn load_3_edges_fit_chunk_size() {
        let tempdir = tempfile::tempdir().unwrap();
        let chunk_size = 4;
        let mut edge_builder = super::EdgeFrameBuilder::new(chunk_size, tempdir.path(), None);

        let state = LoadingState {
            deduped_src_ids: vec![0, 0, 1],
            deduped_dst_ids: vec![1, 2, 2],
            edge_counts: vec![2, 1, 1],
            src_counts: vec![(0, 2), (1, 1)],
        };

        edge_builder
            .push_update_state(state)
            .expect("push update state");

        // edge_builder.finalize(3).expect("finalize");
        let EFBResult {
            adj_out_offsets,
            src_chunks,
            dst_chunks,
            edge_offsets,
        } = edge_builder.finalize(3).expect("finalize");

        assert_eq!(
            &*src_chunks.chunks,
            [PrimitiveArray::from_vec(vec![0u64, 0u64, 1u64])]
        );
        assert_eq!(
            &*dst_chunks.chunks,
            [PrimitiveArray::from_vec(vec![1u64, 2u64, 2u64])]
        );

        let expected: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 2, 3, 4])) };
        assert_eq!(
            edge_offsets,
            ChunkedOffsets::new(vec![expected], chunk_size)
        );

        let expected: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 2, 3, 3])) };
        assert_eq!(
            adj_out_offsets,
            ChunkedOffsets::new(vec![expected], chunk_size)
        );
    }

    #[test]
    fn load_edges_2_chunks_no_dups() {
        let tempdir = tempfile::tempdir().unwrap();
        let chunk_size = 2;
        let mut edge_builder = super::EdgeFrameBuilder::new(chunk_size, tempdir.path(), None);

        let state1 = LoadingState {
            deduped_src_ids: vec![0, 0],
            deduped_dst_ids: vec![0, 1],
            edge_counts: vec![1, 1],
            src_counts: vec![(0, 2)],
        };

        let state2 = LoadingState {
            deduped_src_ids: vec![0, 1],
            deduped_dst_ids: vec![2, 2],
            edge_counts: vec![1, 1],
            src_counts: vec![(0, 1), (1, 1)],
        };

        let states = vec![state1, state2];

        for state in states {
            edge_builder
                .push_update_state(state)
                .expect("push update state");
        }

        let EFBResult {
            adj_out_offsets,
            src_chunks,
            dst_chunks,
            edge_offsets,
        } = edge_builder.finalize(3).expect("finalize");

        assert_eq!(
            src_chunks,
            ChunkedArray::from_non_nulls(vec![
                PrimitiveArray::from_vec(vec![0u64, 0u64]),
                PrimitiveArray::from_vec(vec![0u64, 1u64])
            ])
        );
        assert_eq!(
            dst_chunks,
            ChunkedArray::from_non_nulls(vec![
                PrimitiveArray::from_vec(vec![0u64, 1u64]),
                PrimitiveArray::from_vec(vec![2u64, 2u64])
            ])
        );

        let edge_offsets1: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 1, 2])) };
        let edge_offsets2: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![2, 3, 4])) };

        assert_eq!(
            edge_offsets,
            ChunkedOffsets::new(vec![edge_offsets1, edge_offsets2], chunk_size)
        );

        let adj_out_offsets1: OffsetsBuffer<i64> =
            unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![0, 3, 4])) };

        let adj_out_offsets2 = unsafe { OffsetsBuffer::new_unchecked(Buffer::from(vec![4i64, 4])) };

        assert_eq!(
            adj_out_offsets,
            ChunkedOffsets::new(vec![adj_out_offsets1, adj_out_offsets2], chunk_size)
        );
    }

    #[test]
    fn load_2_chunks_with_dups() {
        let tempdir = tempfile::tempdir().unwrap();
        let chunk_size = 2;
        let mut edge_builder = super::EdgeFrameBuilder::new(chunk_size, tempdir.path(), None);

        let state1 = LoadingState {
            deduped_src_ids: vec![0, 0],
            deduped_dst_ids: vec![1, 2],
            edge_counts: vec![1, 1],
            src_counts: vec![(0, 2)],
        };
        let state2 = LoadingState {
            deduped_src_ids: vec![0, 1, 1],
            deduped_dst_ids: vec![2, 2, 3],
            edge_counts: vec![1, 1, 2],
            src_counts: vec![(0, 1), (1, 2)],
        };

        let states = vec![state1, state2];

        for state in states {
            edge_builder
                .push_update_state(state)
                .expect("push update state");
        }

        let EFBResult {
            adj_out_offsets,
            dst_chunks,
            ..
        } = edge_builder.finalize(4).expect("finalize");

        assert_eq!(adj_out_offsets.last(), dst_chunks.len());

        assert_eq!(adj_out_offsets.lengths().collect_vec(), [2, 2, 0, 0]);
        assert_eq!(dst_chunks.iter().collect_vec(), [1, 2, 2, 3]);
    }

    #[test]
    fn load_2_chunks_start_at_2_with_dups() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut edge_builder = super::EdgeFrameBuilder::new(2, tempdir.path(), None);

        let state1 = LoadingState {
            deduped_src_ids: vec![2, 2],
            deduped_dst_ids: vec![3, 4],
            edge_counts: vec![1, 1],
            src_counts: vec![(2, 2)],
        };
        let state2 = LoadingState {
            deduped_src_ids: vec![2, 3, 3],
            deduped_dst_ids: vec![4, 1, 5],
            edge_counts: vec![1, 1, 1],
            src_counts: vec![(2, 1), (3, 2)],
        };

        let states = vec![state1, state2];

        for state in states {
            edge_builder
                .push_update_state(state)
                .expect("push update state");
        }

        let EFBResult {
            adj_out_offsets,
            dst_chunks,
            ..
        } = edge_builder.finalize(6).expect("finalize");

        assert_eq!(adj_out_offsets.last(), dst_chunks.len());

        assert_eq!(adj_out_offsets.lengths().collect_vec(), [0, 0, 2, 2, 0, 0]);
        assert_eq!(dst_chunks.iter().collect_vec(), [3, 4, 1, 5]);
    }
}
