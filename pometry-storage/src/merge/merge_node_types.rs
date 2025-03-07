use crate::{
    chunked_array::{
        can_slice_and_len::AsIter,
        chunked_array::{ChunkedArray, NonNull},
        mutable_chunked_array::MutPrimitiveChunkedArray,
    },
    file_prefix::GraphPaths,
    merge::merge_chunks::IndexedView,
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use ahash::HashMap;
use itertools::EitherOrBoth;
use polars_arrow::array::{PrimitiveArray, Utf8Array};
use raphtory_api::atomic_extra::atomic_u64_from_mut_slice;
use rayon::prelude::*;
use std::{ops::Range, path::Path, sync::atomic::Ordering::Relaxed};

pub fn merge_node_type_names(
    left_names: &Utf8Array<i64>,
    right_names: &Utf8Array<i64>,
) -> (Utf8Array<i64>, Vec<usize>) {
    let node_type_map: HashMap<_, _> = left_names
        .values_iter()
        .enumerate()
        .map(|(id, name)| (name, id))
        .collect();
    let mut right_map = Vec::with_capacity(right_names.len());
    let mut next_index = left_names.len();

    let names = Utf8Array::from_iter_values(
        left_names
            .values_iter()
            .chain(
                right_names
                    .values_iter()
                    .filter(|&v| match node_type_map.get(v) {
                        None => {
                            right_map.push(next_index);
                            next_index += 1;
                            true
                        }
                        Some(index) => {
                            right_map.push(*index);
                            false
                        }
                    }),
            ),
    );
    (names, right_map)
}

#[derive(Copy, Clone, Debug)]
struct NodeTypeView<'a> {
    index: usize,
    node_map: &'a [usize],
    types: &'a ChunkedArray<PrimitiveArray<u64>, NonNull>,
}

impl<'a> NodeTypeView<'a> {
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    fn new(node_map: &'a [usize], types: &'a ChunkedArray<PrimitiveArray<u64>, NonNull>) -> Self {
        Self {
            index: 0,
            node_map,
            types,
        }
    }
}

impl<'a> IndexedView for NodeTypeView<'a> {
    type Value = (usize, u64);

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index >= self.node_map.len() {
            None
        } else {
            Some((self.node_map[index], self.types.get(index)))
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let start = self.index(range.start);
        let end = self.index(range.end);
        self.node_map[start..end].iter().copied().zip(
            self.types
                .iter_chunks_range(start..end)
                .flat_map(|c| c.as_iter()),
        )
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.node_map.len())
    }

    fn len(&self) -> usize {
        self.node_map.len() - self.index
    }
}

pub fn merge_node_type_ids(
    graph_dir: &Path,
    left: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
    left_node_map: &[usize],
    right: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
    right_node_map: &[usize],
    right_type_map: &[usize],
) -> Result<ChunkedArray<PrimitiveArray<u64>, NonNull>, RAError> {
    let chunk_size = left.chunk_size().max(right.chunk_size());
    let mut type_ids =
        MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::NodeTypeIds);

    for chunk in NodeTypeView::new(left_node_map, left).merge_join_chunks_by::<Vec<_>>(
        NodeTypeView::new(right_node_map, right),
        chunk_size,
        |(l, _), (r, _)| l.cmp(r),
        |idx| match idx {
            EitherOrBoth::Both((_, lt), (_, rt)) => {
                if rt > 0 {
                    right_type_map[rt as usize] as u64
                } else {
                    lt
                }
            }
            EitherOrBoth::Left((_, lt)) => lt,
            EitherOrBoth::Right((_, rt)) => right_type_map[rt as usize] as u64,
        },
    ) {
        type_ids.push_chunk(chunk)?;
    }
    Ok(type_ids.finish()?)
}

pub fn reindex_node_type_ids(
    graph_dir: &Path,
    old_ids: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
    node_map: &[usize],
    num_nodes: usize,
) -> Result<ChunkedArray<PrimitiveArray<u64>, NonNull>, RAError> {
    let chunk_size = old_ids.chunk_size();
    let mut type_ids =
        MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::NodeTypeIds);
    let mut start_idx = 0;
    let num_chunks = num_nodes.div_ceil(chunk_size);
    for chunk_id in 0..num_chunks {
        let chunk_end = ((chunk_id + 1) * chunk_size).min(num_nodes);
        let chunk_start = chunk_id * chunk_size;
        let end_idx = node_map[start_idx..].partition_point(|&v| v < chunk_end) + start_idx;
        let mut chunk = vec![0; chunk_end - chunk_start];
        let chunk_mut = atomic_u64_from_mut_slice(&mut chunk);
        old_ids
            .slice(start_idx..end_idx)
            .par_iter()
            .zip(&node_map[start_idx..end_idx])
            .for_each(|(v, idx)| {
                chunk_mut[idx - chunk_start].store(v, Relaxed);
            });
        type_ids.push_chunk(chunk)?;
        start_idx = end_idx;
    }
    Ok(type_ids.finish()?)
}
