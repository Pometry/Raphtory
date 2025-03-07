use crate::{
    arrow2::array::PrimitiveArray,
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::{ChunkedOffsets, ExplodedValuesIter},
        list_array::ChunkedListArray,
        mutable_chunked_array::{MutChunkedOffsets, MutPrimitiveChunkedArray},
    },
    file_prefix::GraphPaths,
    graph_fragment::TempColGraphFragment,
    prelude::ArrayOps,
    RAError,
};
use bytemuck::{Pod, Zeroable};
use itertools::Itertools;
use memmap2::Mmap;
use raphtory_api::compute::par_cum_sum;
use rayon::prelude::*;
use std::{fs::File, io::Write};
use tempfile::TempDir;

#[derive(Pod, Zeroable, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Default)]
#[repr(C)]
struct NodeTs {
    node: u64,
    time: i64,
}

fn node_ts_slice(bytes: &[u8]) -> &[NodeTs] {
    bytemuck::cast_slice(bytes)
}

pub fn make_node_additions(
    graph: &TempColGraphFragment,
    srcs: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
    offsets: &ChunkedOffsets,
    chunk_size: usize,
) -> Result<ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>, RAError> {
    let temp_dir = TempDir::with_prefix_in("_temporary", graph.graph_dir.as_ref())?;
    let time_col_chunk_size = graph.edges.t_props_chunk_size();
    let dsts = graph.nodes_storage().outbound_neighbours().values();

    let mut chunk: Vec<NodeTs> = vec![NodeTs::default(); time_col_chunk_size];
    let tmp_chunks = graph
        .edges
        .time_col()
        .values()
        .iter_chunks()
        .enumerate()
        .map(|(chunk_id, time_chunk)| {
            let start_offset = chunk_id * time_col_chunk_size;
            let end_offset = (chunk_id + 1) * time_col_chunk_size;
            let srcs_chunk = ExplodedValuesIter::range(srcs, offsets, start_offset..end_offset);
            let dsts_chunk = ExplodedValuesIter::range(dsts, offsets, start_offset..end_offset);
            chunk.resize(time_chunk.len() * 2, NodeTs::default()); // make sure we have the correct length (last chunk)
            chunk
                .par_chunks_mut(2)
                .zip_eq(srcs_chunk)
                .zip_eq(dsts_chunk)
                .zip_eq(time_chunk.as_slice())
                .for_each(|(((v, src), dst), ts)| {
                    v[0] = NodeTs {
                        node: src,
                        time: *ts,
                    };
                    v[1] = NodeTs {
                        node: dst,
                        time: *ts,
                    };
                });
            chunk.par_sort_unstable();
            let actual_len = dedup_sorted_slice(&mut chunk);
            let mut file = File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(temp_dir.path().join(format!("tmp_chunk_{chunk_id}")))?;
            let bytes: &[u8] = bytemuck::cast_slice(&chunk[..actual_len]);
            file.write_all(bytes)?;
            let mmap = unsafe { Mmap::map(&file) }?;
            Ok(mmap)
        })
        .collect::<Result<Vec<_>, RAError>>()?;
    let mut node_ts_offsets = vec![0; graph.num_nodes() + 1];
    let mut node_additions_writer: MutPrimitiveChunkedArray<i64> =
        MutPrimitiveChunkedArray::new_persisted(
            chunk_size,
            &graph.graph_dir,
            GraphPaths::NodeAdditions,
        );

    let sorted_iter = tmp_chunks
        .iter()
        .map(|v| node_ts_slice(v).iter().copied())
        .kmerge()
        .dedup();

    let node_counts = &mut node_ts_offsets[1..];
    for NodeTs { node, time } in sorted_iter {
        node_counts[node as usize] += 1;
        node_additions_writer.push(time)?;
    }
    par_cum_sum(&mut node_ts_offsets);
    let mut offsets_writer = MutChunkedOffsets::new(
        node_ts_offsets.len(),
        Some((
            GraphPaths::NodeAdditionsOffsets,
            graph.graph_dir.to_path_buf(),
        )),
        true,
    );

    offsets_writer.push_chunk(node_ts_offsets)?;
    let offsets = offsets_writer.finish()?;
    let values = node_additions_writer.finish()?;
    Ok(ChunkedListArray::new_from_parts(values, offsets))
}

pub fn dedup_sorted_slice<T: PartialEq + Copy>(slice: &mut [T]) -> usize {
    if slice.is_empty() {
        return 0;
    }
    let mut i = 0;
    let mut j = 1;
    while j < slice.len() {
        if slice[i] != slice[j] {
            i += 1;
            slice[i] = slice[j];
        }
        j += 1;
    }
    i + 1
}

#[cfg(test)]
mod test {
    use crate::graph_builder::node_addition_builder::dedup_sorted_slice;
    use itertools::Itertools;
    use memmap2::MmapMut;
    use proptest::{arbitrary::any, prelude::*, proptest};
    use rayon::prelude::*;
    proptest! {
        #[test]
        fn test_slice_dedup(mut slice in any::<Vec<usize>>().prop_map(|mut v| {v.sort(); v})) {
            let expected = slice.iter().copied().dedup().collect_vec();
            let len = dedup_sorted_slice(&mut slice);
            assert_eq!(expected, slice);
            assert_eq!(len, expected.len());
        }

        #[test]
        fn test_mmap_mut(data in any::<Vec<i64>>()) {
            let workspace = tempfile::tempfile().unwrap();
            workspace.set_len(data.len() as u64 * 8).unwrap();

            let mut mmap = unsafe { MmapMut::map_mut(&workspace).unwrap() };

            let mmap_values: &mut [i64] = bytemuck::try_cast_slice_mut(&mut mmap).unwrap();

            mmap_values
                .par_iter_mut()
                .zip(data.par_iter())
                .for_each(|(target, source)| {
                    *target = *source;
                });

            assert_eq!(data, mmap_values)
        }
    }
}
