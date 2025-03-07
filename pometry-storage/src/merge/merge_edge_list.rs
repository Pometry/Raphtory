use crate::{
    chunked_array::{
        can_slice_and_len::AsIter,
        chunked_array::{ChunkedArray, NonNull},
        mutable_chunked_array::{MutChunkedOffsets, MutPrimitiveChunkedArray},
    },
    edge_list::EdgeList,
    file_prefix::GraphPaths,
    merge::merge_chunks::{IndexedView, MutChunk, OutputChunk},
    nodes::{AdjTotal, Nodes},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use itertools::Itertools;
use polars_arrow::array::PrimitiveArray;
use raphtory_api::{atomic_extra::atomic_usize_from_mut_slice, compute::par_cum_sum};
use rayon::prelude::*;
use std::{ops::Range, path::Path, sync::atomic::Ordering};
use tracing::instrument;

#[derive(Copy, Clone, Debug)]
struct MergeEdgeIds<'a> {
    index: usize,
    index_map: &'a [usize],
    srcs: &'a ChunkedArray<PrimitiveArray<u64>, NonNull>,
    dsts: &'a ChunkedArray<PrimitiveArray<u64>, NonNull>,
}

impl<'a> MergeEdgeIds<'a> {
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    fn new(edges: &'a EdgeList, index_map: &'a [usize]) -> Self {
        let srcs = edges.srcs();
        let dsts = edges.dsts();
        assert_eq!(srcs.len(), dsts.len(), "{srcs:?}\n{dsts:?}");
        Self {
            srcs,
            dsts,
            index_map,
            index: 0,
        }
    }
}

impl<'a> IndexedView for MergeEdgeIds<'a> {
    type Value = (u64, u64); // ((src, dst), num_updates)

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);

        if index < self.srcs.len() {
            Some((
                self.index_map[self.srcs.get(index) as usize] as u64,
                self.index_map[self.dsts.get(index) as usize] as u64,
            ))
        } else {
            None
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let range = self.index(range.start)..self.index(range.end);
        self.srcs
            .iter_chunks_range(range.clone())
            .flat_map(|chunk| chunk.as_iter())
            .map(|i| self.index_map[i as usize] as u64)
            .zip_eq(
                self.dsts
                    .iter_chunks_range(range.clone())
                    .flat_map(|chunk| chunk.as_iter())
                    .map(|i| self.index_map[i as usize] as u64),
            )
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.srcs.len())
    }

    fn len(&self) -> usize {
        self.srcs.len() - self.index
    }
}

struct AdjOutputChunk {
    srcs: Vec<u64>,
    dsts: Vec<u64>,
}

impl OutputChunk for AdjOutputChunk {
    type Value = (u64, u64);
    type Chunk<'a>
        = AdjOutputMutChunk<'a>
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.srcs.truncate(len);
        self.dsts.truncate(len);
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        let srcs = vec![0; len];
        let dsts = vec![0; len];
        Self { srcs, dsts }
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        AdjOutputMutChunk {
            srcs: &mut self.srcs,
            dsts: &mut self.dsts,
        }
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.srcs
            .par_drain(start..)
            .zip_eq(self.dsts.par_drain(start..))
    }
}

struct AdjOutputMutChunk<'a> {
    srcs: &'a mut [u64],
    dsts: &'a mut [u64],
}

impl<'b> MutChunk for AdjOutputMutChunk<'b> {
    type Value = (u64, u64);
    type Chunk<'a>
        = AdjOutputMutChunk<'a>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        self.srcs
            .iter_mut()
            .zip_eq(self.dsts.iter_mut())
            .zip(values)
            .for_each(|((src_entry, dst_entry), (src, dst))| {
                *src_entry = src;
                *dst_entry = dst;
            })
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.srcs
            .par_iter_mut()
            .zip_eq(self.dsts.par_iter_mut())
            .zip(values)
            .for_each(|((src_entry, dst_entry), (src, dst))| {
                *src_entry = src;
                *dst_entry = dst;
            })
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>> {
        self.srcs
            .par_chunks_mut(chunk_size)
            .zip_eq(self.dsts.par_chunks_mut(chunk_size))
            .map(|(srcs, dsts)| AdjOutputMutChunk { srcs, dsts })
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        AdjOutputMutChunk {
            srcs: self.srcs.tail(start),
            dsts: self.dsts.tail(start),
        }
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        self.srcs.copy_within(range.clone(), dst);
        self.dsts.copy_within(range.clone(), dst);
    }

    fn len(&self) -> usize {
        self.srcs.len()
    }
}

#[instrument(level = "debug", skip_all)]
pub fn merge_edge_list(
    graph_dir: impl AsRef<Path>,
    num_nodes: usize,
    left_map: &[usize],
    left_edges: &EdgeList,
    right_map: &[usize],
    right_edges: &EdgeList,
) -> Result<(Nodes<AdjTotal>, EdgeList), RAError> {
    let mut adj_out_offsets = vec![0usize; num_nodes + 1];
    let src_counts = atomic_usize_from_mut_slice(&mut adj_out_offsets[1..]);
    let chunk_size = left_edges
        .srcs()
        .chunk_size()
        .max(right_edges.srcs().chunk_size());
    let graph_dir = graph_dir.as_ref();
    let mut srcs = MutPrimitiveChunkedArray::<u64>::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::AdjOutSrcs,
    );
    let mut dsts = MutPrimitiveChunkedArray::<u64>::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::AdjOutDsts,
    );

    for chunk in MergeEdgeIds::new(left_edges, left_map).merge_dedup_chunks_by::<AdjOutputChunk>(
        MergeEdgeIds::new(right_edges, right_map),
        chunk_size,
        |l, r| l.cmp(r),
        |v| v,
    ) {
        chunk.srcs.par_iter().for_each(|&src| {
            src_counts[src as usize].fetch_add(1, Ordering::Relaxed);
        });
        srcs.push_chunk(chunk.srcs)?;

        // TODO: Would it help to compute inbound offsets here?
        dsts.push_chunk(chunk.dsts)?;
    }
    par_cum_sum(&mut adj_out_offsets);
    let mut adj_out_offsets_builder = MutChunkedOffsets::new(
        num_nodes,
        Some((GraphPaths::AdjOutOffsets, graph_dir.to_path_buf())),
        true,
    );
    adj_out_offsets_builder.push_chunk(adj_out_offsets)?;
    let offsets = adj_out_offsets_builder.finish()?;
    let dsts = dsts.finish()?;
    let srcs = srcs.finish()?;

    let edge_list = EdgeList::new(srcs, dsts.clone());
    let nodes = Nodes::new_out_only_total(offsets, dsts);
    Ok((nodes, edge_list))
}
