use crate::{
    chunked_array::{
        chunked_offsets::ChunkedOffsets, mutable_chunked_array::ChunkedOffsetsBuilder,
    },
    edge_list::EdgeList,
    nodes::{AdjTotal, Nodes},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use itertools::EitherOrBoth;
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::entities::{EID, VID},
};
use rayon::prelude::*;
use std::sync::atomic::Ordering::Relaxed;

mod merge_adj;
pub mod merge_chunks;
mod merge_edge_list;
pub mod merge_gids;
pub mod merge_graph;
mod merge_inbound_adj;
mod merge_node_additions;
mod merge_node_types;
mod merge_props;

fn reindex_node_offsets(
    mut builder: ChunkedOffsetsBuilder<usize>,
    node_map: &[usize],
    offsets: &ChunkedOffsets,
    num_nodes: usize,
) -> Result<ChunkedOffsets, RAError> {
    if node_map.is_empty() {
        assert!(offsets.is_empty());
        return Ok(offsets.clone());
    }
    let mut counts = Vec::with_capacity(2 * offsets.chunk_size());
    let mut first_n = 0;
    let mut index = 0;
    for chunk in offsets.iter_chunks() {
        let last_n = node_map[index + chunk.len_proxy() - 1] + 1;
        counts.clear();
        counts.resize(last_n - first_n, 0);
        let counters = atomic_usize_from_mut_slice(&mut counts);
        chunk.par_windows(2).enumerate().for_each(|(i, r)| {
            counters[node_map[i + index] - first_n].store((r[1] - r[0]) as usize, Relaxed)
        });
        builder.push_counts(&mut counts, first_n, last_n - 1)?;
        first_n = last_n;
        index += chunk.len_proxy();
    }
    builder.ensure_size(num_nodes)?;
    builder.finish()
}

pub fn find_new_triplet(
    local_eid: u64,
    dst: u64,
    old_edge_list: &EdgeList,
    new_global_nodes: &Nodes<AdjTotal>,
    index_map: &[usize],
) -> Option<(u64, u64, u64)> {
    let VID(src) = old_edge_list.get_src(EID(local_eid as usize));

    let new_src = index_map[src] as u64;
    let new_dst = index_map[dst as usize] as u64;

    new_global_nodes
        .find_edge(VID(new_src as usize), VID(new_dst as usize))
        .map(|eid| (new_src, new_dst, eid.0 as u64))
}

#[derive(Default, Copy, Clone, Debug)]
enum EitherIndex {
    #[default]
    Empty,
    Left(usize),
    Right(usize),
}

#[derive(Default, Copy, Clone, Debug)]
enum EitherOrBothIndex {
    #[default]
    Empty,
    Left(usize),
    Right(usize),
    Both(usize, usize),
}

impl From<EitherIndex> for EitherOrBothIndex {
    fn from(value: EitherIndex) -> Self {
        match value {
            EitherIndex::Empty => Self::Empty,
            EitherIndex::Left(l) => Self::Left(l),
            EitherIndex::Right(r) => Self::Right(r),
        }
    }
}

impl From<EitherOrBoth<usize>> for EitherOrBothIndex {
    fn from(value: EitherOrBoth<usize>) -> Self {
        match value {
            EitherOrBoth::Both(l, r) => Self::Both(l, r),
            EitherOrBoth::Left(l) => Self::Left(l),
            EitherOrBoth::Right(r) => Self::Right(r),
        }
    }
}

impl From<EitherOrBoth<&usize>> for EitherOrBothIndex {
    fn from(value: EitherOrBoth<&usize>) -> Self {
        match value {
            EitherOrBoth::Both(l, r) => Self::Both(*l, *r),
            EitherOrBoth::Left(l) => Self::Left(*l),
            EitherOrBoth::Right(r) => Self::Right(*r),
        }
    }
}
