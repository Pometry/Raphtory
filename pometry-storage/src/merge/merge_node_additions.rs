use crate::{
    chunked_array::{
        can_slice_and_len::AsIter,
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::ExplodedIndexIter,
        list_array::ChunkedListArray,
        mutable_chunked_array::{ChunkedOffsetsBuilder, MutPrimitiveChunkedArray},
    },
    file_prefix::GraphPaths,
    merge::merge_chunks::{IndexedView, MutChunk, OutputChunk},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use polars_arrow::array::PrimitiveArray;
use raphtory_api::atomic_extra::atomic_usize_from_mut_slice;
use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    prelude::*,
};
use std::{ops::Range, path::Path, sync::atomic::Ordering};
use tracing::instrument;

#[derive(Copy, Clone, Debug)]
struct MergeAdditions<'a> {
    index: usize,
    reduced_index: usize,
    additions: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    node_map: &'a [usize],
}

impl<'a> MergeAdditions<'a> {
    fn new(
        additions: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
        node_map: &'a [usize],
    ) -> Self {
        Self {
            index: 0,
            reduced_index: 0,
            additions,
            node_map,
        }
    }
    #[inline]
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    #[inline]
    fn reduced(&self, index: usize) -> usize {
        self.additions
            .offsets()
            .find_index_from(index, self.reduced_index)
    }
}

impl<'a> IndexedView for MergeAdditions<'a> {
    type Value = (usize, i64);

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.additions.values().len() {
            let reduced = self.reduced(index);
            Some((self.node_map[reduced], self.additions.values().get(index)))
        } else {
            None
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let start_offset = self.index(range.start);
        let end_offset = self.index(range.end);
        let start_index = self.reduced(start_offset);
        let end_index = self
            .additions
            .offsets()
            .find_index_from(end_offset, start_index);
        ExplodedIndexIter::new_from_index_range(
            self.additions.offsets(),
            start_index,
            start_offset,
            end_index,
            end_offset,
        )
        .map(|i| self.node_map[i])
        .zip(
            self.additions
                .values()
                .iter_chunks_range(start_offset..end_offset)
                .flat_map(|c| c.as_iter()),
        )
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.additions.values().len());
        self.reduced_index = self.reduced(self.index);
    }

    fn len(&self) -> usize {
        self.additions.values().len() - self.index
    }
}

#[derive(Debug)]
struct AdditionsChunk {
    nodes: Vec<usize>,
    times: Vec<i64>,
}

#[derive(Debug)]
struct AdditionsChunkMut<'a> {
    nodes: &'a mut [usize],
    times: &'a mut [i64],
}

impl OutputChunk for AdditionsChunk {
    type Value = (usize, i64);
    type Chunk<'a>
        = AdditionsChunkMut<'a>
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.nodes.truncate(len);
        self.times.truncate(len);
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        Self {
            nodes: vec![0; len],
            times: vec![0; len],
        }
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        AdditionsChunkMut {
            nodes: &mut self.nodes,
            times: &mut self.times,
        }
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.nodes
            .par_drain_tail(start)
            .zip(self.times.par_drain_tail(start))
    }
}

impl<'b> MutChunk for AdditionsChunkMut<'b> {
    type Value = (usize, i64);
    type Chunk<'a>
        = AdditionsChunkMut<'a>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        self.nodes
            .iter_mut()
            .zip(self.times.iter_mut())
            .zip(values)
            .for_each(|((node_entry, t_entry), (node, time))| {
                *node_entry = node;
                *t_entry = time;
            });
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.nodes
            .par_iter_mut()
            .zip(self.times.par_iter_mut())
            .zip(values)
            .for_each(|((node_entry, t_entry), (node, time))| {
                *node_entry = node;
                *t_entry = time;
            });
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>> {
        self.nodes
            .par_chunks_mut(chunk_size)
            .zip(self.times.par_chunks_mut(chunk_size))
            .map(|(nodes, times)| AdditionsChunkMut { nodes, times })
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        AdditionsChunkMut {
            nodes: self.nodes.tail(start),
            times: self.times.tail(start),
        }
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        self.nodes.copy_within(range.clone(), dst);
        self.times.copy_within(range, dst);
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[instrument(level = "debug", skip_all)]
pub fn merge_additions(
    graph_dir: &Path,
    left_additions: &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    left_map: &[usize],
    right_additions: &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    right_map: &[usize],
    num_nodes: usize,
) -> Result<ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>, RAError> {
    let chunk_size = left_additions
        .values()
        .chunk_size()
        .max(right_additions.values().chunk_size());
    let offsets_chunk_size = left_additions
        .offsets()
        .chunk_size()
        .max(right_additions.offsets().chunk_size());
    let mut additions =
        MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::NodeAdditions);
    let mut offsets_builder = ChunkedOffsetsBuilder::new_persisted(
        offsets_chunk_size,
        graph_dir,
        GraphPaths::NodeAdditionsOffsets,
    );
    let mut offsets_counts = Vec::with_capacity(chunk_size);
    let mut first_node = 0;
    for chunk in MergeAdditions::new(left_additions, left_map)
        .merge_dedup_chunks_by::<AdditionsChunk>(
            MergeAdditions::new(right_additions, right_map),
            chunk_size,
            |l, r| l.cmp(r),
            |v| v,
        )
    {
        let last_node = *chunk.nodes.last().unwrap();
        let num_nodes = last_node - first_node + 1;
        offsets_counts.clear();
        offsets_counts.resize(num_nodes, 0);
        let offsets_counters = atomic_usize_from_mut_slice(&mut offsets_counts);
        additions.push_chunk(chunk.times)?;
        chunk.nodes.par_iter().for_each(|node| {
            offsets_counters[node - first_node].fetch_add(1, Ordering::Relaxed);
        });
        offsets_builder.push_counts(&mut offsets_counts, first_node, last_node)?;
        first_node = last_node;
    }
    offsets_builder.ensure_size(num_nodes)?;
    let additions = additions.finish()?;
    let offsets = offsets_builder.finish()?;
    Ok(ChunkedListArray::new_from_parts(additions, offsets))
}
