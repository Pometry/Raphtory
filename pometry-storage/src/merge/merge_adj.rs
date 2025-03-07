use crate::{
    chunked_array::{
        can_slice_and_len::AsIter,
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::ChunkedOffsets,
        mutable_chunked_array::{
            ChunkedOffsetsBuilder, MutChunkedOffsets, MutPrimitiveChunkedArray,
        },
    },
    edges::EdgeTemporalProps,
    file_prefix::GraphPaths,
    global_order::GIDArray,
    graph::{fix_t_prop_offsets, TemporalGraph},
    merge::{
        merge_chunks::{IndexedView, MutChunk, OutputChunk},
        merge_props::merge_edge_props,
    },
    nodes::{AdjTotal, Nodes},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use itertools::Itertools;
use polars_arrow::array::PrimitiveArray;
use raphtory_api::{atomic_extra::atomic_usize_from_mut_slice, compute::par_cum_sum};
use rayon::prelude::*;
use std::{ops::Range, path::Path, sync::atomic::Ordering};
use tempfile::TempDir;
use tracing::instrument;

use super::find_new_triplet;

#[derive(Copy, Clone, Debug)]
struct MergeEdgeIds<'a> {
    index: usize,
    index_map: &'a [usize],
    dsts: &'a ChunkedArray<PrimitiveArray<u64>, NonNull>,
    edges: &'a ChunkedArray<PrimitiveArray<u64>, NonNull>,
    old_graph: &'a TemporalGraph,
    new_global_nodes: &'a Nodes<AdjTotal>,
    edge_offsets: &'a ChunkedOffsets,
}

impl<'a> MergeEdgeIds<'a> {
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    fn new(
        layer_id: usize,
        old_graph: &'a TemporalGraph,
        new_global_nodes: &'a Nodes<AdjTotal>,
        index_map: &'a [usize],
    ) -> Self {
        let layer = old_graph.layer(layer_id);
        let dsts = layer.nodes_storage().outbound_neighbours().values();
        let edges = layer.nodes_storage().outbound_edges().values();
        let edge_offsets = layer.edges_storage().time_col().offsets();
        Self {
            index: 0,
            index_map,
            dsts,
            edges,
            old_graph,
            new_global_nodes,
            edge_offsets,
        }
    }
}

impl<'a> IndexedView for MergeEdgeIds<'a> {
    type Value = ((u64, u64, u64), usize); // ((src, dst, eid), num_updates)

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        // get the actual edge id from the edge_ids array using the index

        if index < self.dsts.len() {
            let local_eid = self.edges.get(index);
            // lookup the offsets of the edges
            let (start_offset, end_offset) = self.edge_offsets.start_end(local_eid as usize);
            find_new_triplet(
                local_eid,
                self.dsts.get(index),
                &self.old_graph.edge_list,
                &self.new_global_nodes,
                &self.index_map,
            )
            .map(|triplet| (triplet, end_offset - start_offset))
        } else {
            None
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let range = self.index(range.start)..self.index(range.end);
        self.edges
            .iter_chunks_range(range.clone())
            .flat_map(|chunk| chunk.as_iter())
            .zip_eq(
                self.dsts
                    .iter_chunks_range(range.clone())
                    .flat_map(|chunk| chunk.as_iter()),
            )
            .map(move |(local_eid, dst)| {
                let (start_offset, end_offset) = self.edge_offsets.start_end(local_eid as usize);
                find_new_triplet(
                    local_eid,
                    dst,
                    &self.old_graph.edge_list,
                    &self.new_global_nodes,
                    &self.index_map,
                )
                .map(|triplet| (triplet, end_offset - start_offset))
                .expect("Failed to find new triplet")
            })
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.dsts.len())
    }

    fn len(&self) -> usize {
        self.dsts.len() - self.index
    }
}

struct AdjOutputChunk {
    srcs: Vec<u64>,
    dsts: Vec<u64>,
    edges: Vec<u64>,
    counts: Vec<usize>,
}

impl AdjOutputChunk {
    fn first_edge(&self) -> Option<(u64, u64, u64)> {
        let src = *self.srcs.first()?;
        let dst = *self.dsts.first()?;
        let edge = *self.edges.first()?;
        Some((src, dst, edge))
    }

    fn last_edge(&self) -> Option<(u64, u64, u64)> {
        let src = *self.srcs.last()?;
        let dst = *self.dsts.last()?;
        let edge = *self.edges.last()?;
        Some((src, dst, edge))
    }
}

impl OutputChunk for AdjOutputChunk {
    type Value = ((u64, u64, u64), usize);
    type Chunk<'a>
        = AdjOutputMutChunk<'a>
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.srcs.truncate(len);
        self.dsts.truncate(len);
        self.edges.truncate(len);
        self.counts.truncate(len);
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        let srcs = vec![0; len];
        let dsts = vec![0; len];
        let edges = vec![0; len];
        let counts = vec![0; len];
        Self {
            srcs,
            dsts,
            edges,
            counts,
        }
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        AdjOutputMutChunk {
            srcs: &mut self.srcs,
            dsts: &mut self.dsts,
            edges: &mut self.edges,
            counts: &mut self.counts,
        }
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.srcs
            .par_drain(start..)
            .zip_eq(self.dsts.par_drain(start..))
            .zip_eq(self.edges.par_drain(start..))
            .zip_eq(self.counts.par_drain(start..))
            .map(|(((src, dst), edge), count)| ((src, dst, edge), count))
    }
}

struct AdjOutputMutChunk<'a> {
    srcs: &'a mut [u64],
    dsts: &'a mut [u64],
    edges: &'a mut [u64],
    counts: &'a mut [usize],
}

impl<'b> MutChunk for AdjOutputMutChunk<'b> {
    type Value = ((u64, u64, u64), usize);
    type Chunk<'a>
        = AdjOutputMutChunk<'a>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        self.srcs
            .iter_mut()
            .zip_eq(self.dsts.iter_mut())
            .zip_eq(self.edges.iter_mut())
            .zip_eq(self.counts.iter_mut())
            .zip(values)
            .for_each(
                |(
                    (((src_entry, dst_entry), edge_entry), count_entry),
                    ((src, dst, eid), count),
                )| {
                    *src_entry = src;
                    *dst_entry = dst;
                    *edge_entry = eid;
                    *count_entry = count;
                },
            )
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.srcs
            .par_iter_mut()
            .zip_eq(self.dsts.par_iter_mut())
            .zip_eq(self.edges.par_iter_mut())
            .zip_eq(self.counts.par_iter_mut())
            .zip(values)
            .for_each(
                |(
                    (((src_entry, dst_entry), edge_entry), count_entry),
                    ((src, dst, eid), count),
                )| {
                    *src_entry = src;
                    *dst_entry = dst;
                    *edge_entry = eid;
                    *count_entry = count;
                },
            )
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>> {
        self.srcs
            .par_chunks_mut(chunk_size)
            .zip_eq(self.dsts.par_chunks_mut(chunk_size))
            .zip_eq(self.edges.par_chunks_mut(chunk_size))
            .zip_eq(self.counts.par_chunks_mut(chunk_size))
            .map(|(((srcs, dsts), edges), counts)| AdjOutputMutChunk {
                srcs,
                dsts,
                edges,
                counts,
            })
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        AdjOutputMutChunk {
            srcs: self.srcs.tail(start),
            dsts: self.dsts.tail(start),
            edges: self.edges.tail(start),
            counts: self.counts.tail(start),
        }
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        self.srcs.copy_within(range.clone(), dst);
        self.dsts.copy_within(range.clone(), dst);
        self.edges.copy_within(range.clone(), dst);
        self.counts.copy_within(range, dst);
    }

    fn len(&self) -> usize {
        self.srcs.len()
    }
}

#[instrument(level = "debug", skip_all)]
pub fn merge_adj(
    graph_dir: impl AsRef<Path>,
    global_nodes: &Nodes<AdjTotal>,
    total_num_edges: usize,
    gids: &GIDArray,

    left_map: &[usize],
    left_graph: &TemporalGraph,
    left_layer_id: usize,

    right_map: &[usize],
    right_graph: &TemporalGraph,
    right_layer_id: usize,
) -> Result<(Nodes, EdgeTemporalProps), RAError> {
    let num_nodes = gids.len();
    let mut adj_out_offsets = vec![0usize; num_nodes + 1];
    let src_counts = atomic_usize_from_mut_slice(&mut adj_out_offsets[1..]);
    let chunk_size = left_graph
        .layer(left_layer_id)
        .nodes_storage()
        .outbound_neighbours()
        .values()
        .chunk_size()
        .max(
            right_graph
                .layer(right_layer_id)
                .nodes_storage()
                .outbound_neighbours()
                .values()
                .chunk_size(),
        );
    let graph_dir = graph_dir.as_ref();
    let mut edge_ids = MutPrimitiveChunkedArray::<u64>::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::AdjOutSrcs,
    );
    let mut dsts = MutPrimitiveChunkedArray::<u64>::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::AdjOutDsts,
    );
    let tmp_dir = TempDir::new_in(graph_dir)?;
    let mut edge_ts_offsets = ChunkedOffsetsBuilder::new_persisted(
        chunk_size,
        tmp_dir.path(),
        GraphPaths::EdgeTPropsOffsets,
    );

    let left_merge_ids = MergeEdgeIds::new(left_layer_id, left_graph, &global_nodes, left_map);
    let right_merge_ids = MergeEdgeIds::new(right_layer_id, right_graph, &global_nodes, right_map);

    for mut chunk in left_merge_ids.merge_join_chunks_by::<AdjOutputChunk>(
        right_merge_ids,
        chunk_size,
        |(l, _), (r, _)| l.cmp(r),
        |v| v.reduce(|(e, l_count), (_, r_count)| (e, l_count + r_count)),
    ) {
        chunk.srcs.par_iter().for_each(|&src| {
            src_counts[src as usize].fetch_add(1, Ordering::Relaxed);
        });
        let first_e = chunk.first_edge().unwrap();
        let last_e = chunk.last_edge().unwrap();
        edge_ts_offsets.push_counts(&mut chunk.counts, first_e, last_e)?;
        edge_ids.push_chunk(chunk.edges)?;

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
    let edge_ids = edge_ids.finish()?;

    let edge_props = merge_edge_props::merge_properties(
        graph_dir,
        left_graph,
        left_layer_id,
        left_map,
        right_graph,
        right_layer_id,
        right_map,
    )?;
    let edge_ts_offsets = edge_ts_offsets.finish()?;

    // let layer_edge_ids = edge_ids.iter();
    let edge_ts_offsets = fix_t_prop_offsets(
        total_num_edges,
        chunk_size,
        graph_dir,
        &edge_ts_offsets,
        0u64..edge_ids.len() as u64,
        edge_ids.iter(),
    )?;

    let edges = EdgeTemporalProps::from_structs(edge_props, edge_ts_offsets);
    let nodes = Nodes::new_only_outbound(offsets, dsts, edge_ids)?;
    Ok((nodes, edges))
}
