use crate::{
    arrow2::array::PrimitiveArray,
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        list_array::ChunkedListArray,
        mutable_chunked_array::{MutChunkedOffsets, MutPrimitiveChunkedArray},
    },
    file_prefix::GraphPaths,
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use parking_lot::Mutex;
use raphtory_api::compute::par_cum_sum;
use rayon::prelude::*;
use std::{
    collections::BinaryHeap,
    path::Path,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub fn build_inbound_adj_index(
    graph_dir: impl AsRef<Path>,
    outbound: &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    outbound_edge_ids: &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
) -> Result<
    (
        ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
        ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    ),
    RAError,
> {
    let graph_dir = graph_dir.as_ref();
    let num_nodes = outbound.len();
    let mut counts = Vec::with_capacity(num_nodes);
    counts.resize_with(num_nodes + 1, || AtomicUsize::new(0));

    outbound.values().par_iter().for_each(|v| {
        counts[v as usize + 1].fetch_add(1, Ordering::Relaxed);
    });

    let mut offsets: Vec<_> = counts.into_iter().map(|v| v.into_inner()).collect();
    par_cum_sum(&mut offsets);

    let mut inbound_srcs = MutPrimitiveChunkedArray::new_persisted(
        outbound.values().chunk_size(),
        graph_dir,
        GraphPaths::AdjInSrcs,
    );
    let mut inbound_eids = MutPrimitiveChunkedArray::new_persisted(
        outbound.values().chunk_size(),
        graph_dir,
        GraphPaths::AdjInEdges,
    );

    let mut progress = Vec::with_capacity(num_nodes);
    progress.resize_with(num_nodes, || AtomicUsize::new(0));
    let chunk_size = outbound.values().chunk_size();

    let mut last_dst: usize = 0;
    let mut first_offset = 0;
    for (c, outbound_chunk) in outbound.values().iter_chunks().enumerate() {
        let last_offset = (c + 1) * chunk_size;
        let new_last_dst = offsets[last_dst + 1..].partition_point(|v| *v < last_offset) + last_dst;
        let partial_last_chunk =
            new_last_dst + 1 < offsets.len() && offsets[new_last_dst + 1] > last_offset;
        let offset_deltas = OffsetDeltas {
            first_offset,
            center_offsets: &offsets[last_dst + 1..new_last_dst + 1],
        };

        let actual_chunk_size = outbound_chunk.len();
        let mut src_ids: Vec<AtomicU64> = Vec::with_capacity(actual_chunk_size);
        src_ids.resize_with(actual_chunk_size, AtomicU64::default);
        let mut edge_ids: Vec<AtomicU64> = Vec::with_capacity(actual_chunk_size);
        edge_ids.resize_with(actual_chunk_size, AtomicU64::default);

        let in_progress = InProgressChunk::new(
            &offset_deltas,
            last_dst as u64,
            &progress,
            &src_ids,
            &edge_ids,
            partial_last_chunk,
        );
        outbound
            .par_values()
            .zip(outbound_edge_ids.par_values())
            .enumerate()
            .for_each(|(src_index, (dst_ids, edge_ids))| {
                let progress = progress[src_index].load(Ordering::Relaxed);
                let dst_ids = dst_ids.slice(progress..);
                let edge_ids = edge_ids.slice(progress..);
                let last_index = dst_ids.partition_point(|v| v <= new_last_dst as u64);
                dst_ids
                    .slice(..last_index)
                    .into_par_iter()
                    .zip(edge_ids.slice(..last_index).into_par_iter())
                    .for_each(|(dst_id, edge_id)| {
                        in_progress.push(dst_id, src_index as u64, edge_id)
                    });
            });

        let mut src_ids: Vec<_> = src_ids.into_iter().map(|v| v.into_inner()).collect();
        let mut edge_ids: Vec<_> = edge_ids.into_iter().map(|v| v.into_inner()).collect();

        split_mut_from_offsets(&mut src_ids, &offset_deltas)
            .into_par_iter()
            .for_each(|chunk| chunk.sort());
        split_mut_from_offsets(&mut edge_ids, &offset_deltas)
            .into_par_iter()
            .for_each(|chunk| chunk.sort());

        inbound_srcs.push_chunk(src_ids)?;
        inbound_eids.push_chunk(edge_ids)?;

        first_offset = last_offset;
        last_dst = new_last_dst;
    }

    let mut chunked_offsets = MutChunkedOffsets::new(
        offsets.len() - 1,
        Some((GraphPaths::AdjInOffsets, graph_dir.to_path_buf())),
        true,
    );
    chunked_offsets.push_chunk(offsets)?;
    let offsets = chunked_offsets.finish()?;
    let inbound_srcs = inbound_srcs.finish()?;
    let inbound_eids = inbound_eids.finish()?;

    let adj_in_neighbours = ChunkedListArray::new_from_parts(inbound_srcs, offsets.clone());
    let adj_in_edges = ChunkedListArray::new_from_parts(inbound_eids, offsets);
    Ok((adj_in_neighbours, adj_in_edges))
}

pub(crate) struct OffsetDeltas<'a> {
    first_offset: usize,
    center_offsets: &'a [usize],
}

impl<'a> OffsetDeltas<'a> {
    pub fn len(&self) -> usize {
        self.center_offsets.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.center_offsets
            .iter()
            .scan(self.first_offset, |last, v| {
                let delta = *v - *last;
                *last = *v;
                Some(delta)
            })
    }
}

#[derive(Debug)]
struct InProgressChunk<'a> {
    chunks: Vec<InProgressChunkSlice<'a>>,
    last_chunk: Option<InProgressPartialChunk<'a>>,
    first_dst: u64,
}

impl<'a> InProgressChunk<'a> {
    fn new(
        offset_deltas: &OffsetDeltas,
        first_dst: u64,
        node_progress: &'a [AtomicUsize],
        mut src_ids: &'a [AtomicU64],
        mut edge_ids: &'a [AtomicU64],
        partial_last_chunk: bool,
    ) -> Self {
        let mut chunks = Vec::with_capacity(offset_deltas.len());
        for delta in offset_deltas.iter() {
            let (chunk_src_ids, remainder_src_ids) = src_ids.split_at(delta);
            let (chunk_edge_ids, remainder_edge_ids) = edge_ids.split_at(delta);
            chunks.push(InProgressChunkSlice {
                edge_ids: chunk_edge_ids,
                src_ids: chunk_src_ids,
                node_progress,
                index: AtomicUsize::new(0),
            });
            src_ids = remainder_src_ids;
            edge_ids = remainder_edge_ids;
        }
        let last_chunk = if partial_last_chunk {
            Some(InProgressPartialChunk {
                edge_ids,
                src_ids,
                node_progress,
                heap_index: Mutex::new(BinaryHeap::with_capacity(src_ids.len())),
            })
        } else {
            // last chunk is complete, no need for extra checks
            chunks.push(InProgressChunkSlice {
                edge_ids,
                src_ids,
                node_progress,
                index: AtomicUsize::new(0),
            });
            None
        };
        Self {
            chunks,
            last_chunk,
            first_dst,
        }
    }

    fn dst_to_index(&self, dst_id: u64) -> usize {
        (dst_id - self.first_dst) as usize
    }

    fn push(&self, dst_id: u64, src_id: u64, edge_id: u64) {
        let index = self.dst_to_index(dst_id);
        match self.chunks.get(index) {
            None => {
                assert_eq!(index, self.chunks.len());
                self.last_chunk.as_ref().unwrap().push(src_id, edge_id)
            }
            Some(chunk) => chunk.push(src_id, edge_id),
        }
    }
}

#[derive(Debug)]
struct InProgressChunkSlice<'a> {
    edge_ids: &'a [AtomicU64],
    src_ids: &'a [AtomicU64],
    node_progress: &'a [AtomicUsize],
    index: AtomicUsize,
}

impl<'a> InProgressChunkSlice<'a> {
    fn push(&self, src_id: u64, edge_id: u64) {
        let index = self.index.fetch_add(1, Ordering::Relaxed);
        self.edge_ids[index].swap(edge_id, Ordering::Relaxed);
        self.src_ids[index].swap(src_id, Ordering::Relaxed);
        self.node_progress[src_id as usize].fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct InProgressPartialChunk<'a> {
    edge_ids: &'a [AtomicU64],
    src_ids: &'a [AtomicU64],
    node_progress: &'a [AtomicUsize],
    heap_index: Mutex<BinaryHeap<(u64, usize)>>,
}

impl<'a> InProgressPartialChunk<'a> {
    #[inline]
    fn len(&self) -> usize {
        self.edge_ids.len()
    }

    fn push(&self, src_id: u64, edge_id: u64) {
        let mut heap_index = self.heap_index.lock();
        let index = heap_index.len();
        if index < self.len() {
            heap_index.push((src_id, index));
            self.edge_ids[index].swap(edge_id, Ordering::Relaxed);
            self.src_ids[index].swap(src_id, Ordering::Relaxed);
            self.node_progress[src_id as usize].fetch_add(1, Ordering::Release);
        } else {
            let mut current_max = heap_index.peek_mut().unwrap();
            let old_src_id = current_max.0;
            if old_src_id > src_id {
                let index = current_max.1;
                current_max.0 = src_id;
                self.edge_ids[index].swap(edge_id, Ordering::Relaxed);
                self.src_ids[index].swap(src_id, Ordering::Relaxed);
                self.node_progress[src_id as usize].fetch_add(1, Ordering::Release);
                self.node_progress[old_src_id as usize].fetch_sub(1, Ordering::Acquire);
            }
        }
    }
}

pub fn split_mut_from_offsets<'a, T>(
    values: &'a mut [T],
    offsets: &OffsetDeltas,
) -> Vec<&'a mut [T]> {
    let mut chunks: Vec<&'a mut [T]> = Vec::with_capacity(offsets.len() + 1);
    let mut chunk: &'a mut [T];
    let mut remainder = values;
    for pos in offsets.iter() {
        (chunk, remainder) = remainder.split_at_mut(pos);
        chunks.push(chunk);
    }
    chunks.push(remainder);
    chunks
}
