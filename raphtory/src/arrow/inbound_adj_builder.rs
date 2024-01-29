use crate::arrow::{
    chunked_array::{
        chunked_array::ChunkedArray,
        list_array::ChunkedListArray,
        mutable_chunked_array::{MutableChunkedOffsets, MutablePrimitiveChunkedArray},
    },
    file_prefix::GraphPaths,
    prelude::BaseArrayOps,
    Error,
};
use arrow2::array::PrimitiveArray;
use itertools::Itertools;
use rayon::prelude::*;
use std::{
    ops::{Add, AddAssign},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Compute cumulative sum in parallel over `num_chunks` chunks
pub fn par_cum_sum<const chunk_size: usize>(values: &mut [usize]) {
    let num_chunks = values.len().div_ceil(chunk_size);
    let mut chunk_sums = Vec::with_capacity(num_chunks);
    values
        .par_chunks_mut(chunk_size)
        .map(|chunk| {
            let mut cum_sum = 0;
            for v in chunk {
                *v += cum_sum;
                cum_sum = *v;
            }
            cum_sum
        })
        .collect_into_vec(&mut chunk_sums);

    let mut cum_sum = 0;
    for (partial_sum, next_chunk) in chunk_sums
        .into_iter()
        .zip(values.chunks_mut(chunk_size).skip(1))
    {
        cum_sum += partial_sum;
        next_chunk.par_iter_mut().for_each(|v| *v += cum_sum);
    }
}

pub fn build_inbound_adj_index(
    graph_dir: impl AsRef<Path>,
    outbound: &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>>>,
) -> Result<
    (
        ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>>>,
        ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>>>,
    ),
    Error,
> {
    let graph_dir = graph_dir.as_ref();
    let num_nodes = outbound.len();
    let mut counts = Vec::with_capacity(num_nodes);
    counts.resize_with(num_nodes, || AtomicUsize::new(0));

    outbound.values().par_iter().for_each(|v| {
        if let Some(v) = v {
            counts[v as usize].fetch_add(1, Ordering::Relaxed);
        }
    });

    let mut offsets = MutableChunkedOffsets::new(
        outbound.offsets().chunk_size,
        Some((GraphPaths::AdjInOffsets, graph_dir.to_path_buf())),
        true,
    );

    let mut cum_sum = 0;
    for count in counts {
        cum_sum += count.load(Ordering::Relaxed);
        offsets.push(cum_sum)?;
    }
    let offsets = offsets.finish()?;

    let mut inbound_src = MutablePrimitiveChunkedArray::new(
        outbound.values().chunk_size,
        Some((GraphPaths::AdjInSrcs, graph_dir.to_path_buf())),
        true,
    );
    let mut inbound_eids = MutablePrimitiveChunkedArray::new(
        outbound.values().chunk_size,
        Some((GraphPaths::AdjInEdges, graph_dir.to_path_buf())),
        true,
    );

    let mut progress = vec![0usize, num_nodes];
    let mut last_dst = 0;
    for c in 1..=outbound.values().num_chunks() {}

    for (_, src, eid) in outbound
        .offsets()
        .iter()
        .enumerate()
        .map(|(src_id, eids)| {
            let dsts = outbound.values().slice(eids.clone());
            dsts.flatten()
                .zip(eids)
                .map(move |(dst, eid)| (dst, src_id as u64, eid as u64))
        })
        .kmerge()
    {
        inbound_src.push(src)?;
        inbound_eids.push(eid)?;
    }
    let inbound_srcs = inbound_src.finish()?;
    let inbound_eids = inbound_eids.finish()?;

    let adj_in_neighbours = ChunkedListArray::new_from_parts(inbound_srcs, offsets.clone());
    let adj_in_edges = ChunkedListArray::new_from_parts(inbound_eids, offsets);
    Ok((adj_in_neighbours, adj_in_edges))
}

#[cfg(test)]
mod test {
    use crate::arrow::inbound_adj_builder::par_cum_sum;
    use itertools::Itertools;

    #[test]
    fn test_cum_sum() {
        let mut values = (0..100).collect_vec();
        par_cum_sum::<8>(&mut values);
        let mut cum_sum = 0;
        for (index, v) in values.into_iter().enumerate() {
            cum_sum += index;
            assert_eq!(v, cum_sum)
        }
    }
}
