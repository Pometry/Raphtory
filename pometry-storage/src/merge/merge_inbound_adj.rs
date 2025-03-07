use std::{ops::Range, path::Path, sync::atomic::Ordering};

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
    nodes::{AdjTotal, Nodes},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use itertools::Itertools;
use polars_arrow::array::PrimitiveArray;
use raphtory_api::{atomic_extra::atomic_usize_from_mut_slice, core::entities::VID};
use rayon::{iter::IndexedParallelIterator, prelude::*};
use tracing::instrument;

#[derive(Copy, Clone, Debug)]
struct MergeInbound<'a> {
    index: usize,
    reduced: usize,
    srcs: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    node_map: &'a [usize],
}

impl<'a> MergeInbound<'a> {
    fn new(
        srcs: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
        node_map: &'a [usize],
    ) -> Self {
        Self {
            index: 0,
            reduced: 0,
            srcs,
            node_map,
        }
    }

    #[inline]
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    #[inline]
    fn reduced(&self, index: usize) -> usize {
        self.srcs.offsets().find_index_from(index, self.reduced)
    }
}

impl<'a> IndexedView for MergeInbound<'a> {
    type Value = (u64, u64);

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.srcs.values().len() {
            let reduced = self.reduced(index);
            Some((
                self.node_map[reduced] as u64,
                self.node_map[self.srcs.values().get(index) as usize] as u64,
            ))
        } else {
            None
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let start_offset = self.index(range.start);
        let end_offset = self.index(range.end);
        let start_index = self.reduced(start_offset);
        let end_index = self.srcs.offsets().find_index_from(end_offset, start_index);
        ExplodedIndexIter::new_from_index_range(
            self.srcs.offsets(),
            start_index,
            start_offset,
            end_index,
            end_offset,
        )
        .zip_eq(
            self.srcs
                .values()
                .iter_chunks_range(start_offset..end_offset)
                .flat_map(|c| c.as_iter()),
        )
        .map(|(dst, src)| {
            (
                self.node_map[dst] as u64,
                self.node_map[src as usize] as u64,
            )
        })
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.srcs.values().len());
        self.reduced = self.reduced(self.index);
    }

    fn len(&self) -> usize {
        self.srcs.values().len() - self.index
    }
}

#[derive(Debug)]
struct InboundsChunk {
    dsts: Vec<u64>,
    srcs: Vec<u64>,
}

struct InboundsChunkRef<'a> {
    dsts: &'a mut [u64],
    srcs: &'a mut [u64],
}

impl<'b> MutChunk for InboundsChunkRef<'b> {
    type Value = (u64, u64);
    type Chunk<'a>
        = InboundsChunkRef<'a>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        self.dsts
            .iter_mut()
            .zip(self.srcs.iter_mut())
            .zip(values)
            .for_each(|((dst_entry, src_entry), (dst, src))| {
                *dst_entry = dst;
                *src_entry = src;
            })
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.dsts
            .par_iter_mut()
            .zip(self.srcs.par_iter_mut())
            .zip(values)
            .for_each(|((dst_entry, src_entry), (dst, src))| {
                *src_entry = src;
                *dst_entry = dst;
            })
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>> {
        self.dsts
            .par_chunks_mut(chunk_size)
            .zip(self.srcs.par_chunks_mut(chunk_size))
            .map(|(dsts, srcs)| InboundsChunkRef { dsts, srcs })
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        InboundsChunkRef {
            dsts: self.dsts.tail(start),
            srcs: self.srcs.tail(start),
        }
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        self.dsts.copy_within(range.clone(), dst);
        self.srcs.copy_within(range, dst);
    }

    fn len(&self) -> usize {
        self.dsts.len()
    }
}

impl OutputChunk for InboundsChunk {
    type Value = (u64, u64);
    type Chunk<'a>
        = InboundsChunkRef<'a>
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.dsts.truncate(len);
        self.srcs.truncate(len);
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        Self {
            dsts: vec![0; len],
            srcs: vec![0; len],
        }
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        InboundsChunkRef {
            dsts: &mut self.dsts,
            srcs: &mut self.srcs,
        }
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.dsts
            .par_drain_tail(start)
            .zip(self.srcs.par_drain_tail(start))
    }
}

#[instrument(level = "debug", skip_all)]
pub fn merge_inbounds(
    graph_dir: &Path,
    global_nodes: &Nodes<AdjTotal>,
    left_nodes: &Nodes,
    left_node_map: &[usize],
    right_nodes: &Nodes,
    right_node_map: &[usize],
    merged_nodes: &mut Nodes,
) -> Result<(), RAError> {
    let left_inbound_srcs = left_nodes.inbound_neighbours();
    let right_inbound_srcs = right_nodes.inbound_neighbours();
    let chunk_size = left_inbound_srcs
        .values()
        .chunk_size()
        .max(right_inbound_srcs.values().chunk_size());
    let offsets_chunk_size = left_inbound_srcs
        .offsets()
        .chunk_size()
        .max(right_inbound_srcs.offsets().chunk_size());
    let mut srcs =
        MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::AdjInSrcs);
    let mut offsets_builder = ChunkedOffsetsBuilder::new_persisted(
        offsets_chunk_size,
        graph_dir,
        GraphPaths::AdjInOffsets,
    );
    let mut offsets_counts = Vec::with_capacity(chunk_size);
    let mut edges =
        MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::AdjInEdges);
    let mut first_dst = 0;
    for chunk in MergeInbound::new(left_inbound_srcs, left_node_map)
        .merge_dedup_chunks_by::<InboundsChunk>(
            MergeInbound::new(right_inbound_srcs, right_node_map),
            chunk_size,
            |l, r| l.cmp(r),
            |v| v,
        )
    {
        let last_dst = *chunk.dsts.last().unwrap() as usize;
        let num_dsts = last_dst - first_dst + 1;
        offsets_counts.clear();
        offsets_counts.resize(num_dsts, 0);
        let offsets_counters = atomic_usize_from_mut_slice(&mut offsets_counts);
        let eids = chunk
            .srcs
            .par_iter()
            .zip(chunk.dsts.par_iter())
            .map(|(&src, &dst)| {
                offsets_counters[(dst as usize) - first_dst].fetch_add(1, Ordering::Relaxed);

                let eid = global_nodes
                    .find_edge(VID(src as usize), VID(dst as usize))
                    .expect("Edge not found");

                eid.0 as u64
            })
            .collect();
        edges.push_chunk(eids)?;
        srcs.push_chunk(chunk.srcs)?;
        offsets_builder.push_counts(&mut offsets_counts, first_dst, last_dst)?;
        first_dst = last_dst;
    }
    let srcs = srcs.finish()?;
    offsets_builder.ensure_size(merged_nodes.len())?;
    let offsets = offsets_builder.finish()?;
    let edges = edges.finish()?;
    merged_nodes.adj_in.neighbours = ChunkedListArray::new_from_parts(srcs, offsets.clone());
    merged_nodes.adj_in.edges = ChunkedListArray::new_from_parts(edges, offsets);
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        chunked_array::{
            chunked_array::ChunkedArray, chunked_offsets::ChunkedOffsets,
            list_array::ChunkedListArray,
        },
        merge::{merge_chunks::IndexedView, merge_inbound_adj::MergeInbound},
    };
    use polars_arrow::offset::OffsetsBuffer;

    #[test]
    fn broken_case() {
        let offsets =
            ChunkedOffsets::from(OffsetsBuffer::try_from(vec![0, 1, 1, 2, 2, 4]).unwrap());
        let values = ChunkedArray::from(vec![vec![0u64, 1, 0, 3]]);
        let inbounds = ChunkedListArray::new_from_parts(values, offsets);
        let merger = MergeInbound::new(&inbounds, &[0, 1, 2, 3, 4]);
        let all_v: Vec<_> = merger.range(0..4).collect();
        assert_eq!(all_v, [(0, 0), (2, 1), (4, 0), (4, 3)])
    }
}
