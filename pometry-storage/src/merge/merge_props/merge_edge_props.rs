use crate::{
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::ExplodedValuesIter,
        list_array::ChunkedListArray,
        mutable_chunked_array::MutChunkedStructArray,
    },
    edge_list::EdgeList,
    file_prefix::GraphPaths,
    graph::TemporalGraph,
    graph_fragment::TempColGraphFragment,
    merge::{
        merge_chunks::IndexedView,
        merge_props::{self, merge_props_chunk},
        EitherIndex,
    },
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    RAError,
};
use itertools::izip;
use polars_arrow::{
    array::{PrimitiveArray, StructArray},
    compute::cast::{cast, CastOptions},
};
use std::{ops::Range, path::Path};

#[derive(Copy, Clone, Debug)]
struct MergePropertyIndex<'a> {
    index: usize,
    reduced_index: usize,
    old_edge_list: &'a EdgeList,
    timestamps: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    index_map: &'a [usize],
    index_type: fn(usize) -> EitherIndex,
}

impl<'a> MergePropertyIndex<'a> {
    fn new(
        layer: &'a TempColGraphFragment,
        old_edge_list: &'a EdgeList,
        index_map: &'a [usize],
        index_type: fn(usize) -> EitherIndex,
    ) -> Self {
        let timestamps = layer.edges_storage().time_col();
        Self {
            index: 0,
            reduced_index: 0,
            old_edge_list,
            timestamps,
            index_map,
            index_type,
        }
    }

    #[inline]
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    #[inline]
    fn reduced(&self, index: usize) -> usize {
        self.timestamps
            .offsets()
            .find_index_from(index, self.reduced_index)
    }

    #[inline]
    fn map_index(&self, index: usize) -> EitherIndex {
        (self.index_type)(index)
    }
}

impl<'a> IndexedView for MergePropertyIndex<'a> {
    type Value = ((u64, u64, i64), EitherIndex);

    #[inline]
    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.timestamps.values().len() {
            let reduced_index = self.reduced(index);

            let src = self.old_edge_list.srcs().get(reduced_index);
            let dst = self.old_edge_list.dsts().get(reduced_index);

            Some((
                (
                    self.index_map[src as usize] as u64,
                    self.index_map[dst as usize] as u64,
                    self.timestamps.values().get(index),
                ),
                self.map_index(index),
            ))
        } else {
            None
        }
    }

    #[inline]
    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let start_offset = self.index(range.start);
        let end_offset = self.index(range.end);
        let start_index = self.reduced(start_offset);
        let end_index = self
            .timestamps
            .offsets()
            .find_index_from(end_offset, start_index);

        izip!(
            ExplodedValuesIter::new_from_index_range(
                self.old_edge_list.srcs(),
                self.timestamps.offsets(),
                start_index,
                start_offset,
                end_index,
                end_offset
            )
            .map(|i| self.index_map[i as usize] as u64),
            ExplodedValuesIter::new_from_index_range(
                self.old_edge_list.dsts(),
                self.timestamps.offsets(),
                start_index,
                start_offset,
                end_index,
                end_offset
            )
            .map(|i| self.index_map[i as usize] as u64),
            self.timestamps.values().slice(start_offset..end_offset)
        )
        .zip((start_offset..end_offset).map(move |i| self.map_index(i)))
    }

    #[inline]
    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.timestamps.values().len());
        self.reduced_index = self.reduced(self.index);
    }

    #[inline]
    fn len(&self) -> usize {
        self.timestamps.values().len() - self.index
    }
}

pub fn merge_properties(
    graph_dir: &Path,
    left_edges: &TemporalGraph,
    left_layer_id: usize,
    left_map: &[usize],
    right_edges: &TemporalGraph,
    right_layer_id: usize,
    right_map: &[usize],
) -> Result<ChunkedArray<StructArray>, RAError> {
    let left_layer = left_edges.layer(left_layer_id);
    let right_layer = right_edges.layer(right_layer_id);

    let left_props = left_layer.edges_storage().temporal_props().values();
    let right_props = right_layer.edges_storage().temporal_props().values();
    let chunk_size = left_props.chunk_size().max(right_props.chunk_size());

    let (fields, col_idx) =
        merge_props::merge_fields(left_layer.edges_data_type(), right_layer.edges_data_type())?;

    let mut props = MutChunkedStructArray::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::EdgeTProps,
        fields.clone(),
    );

    for chunk_index in MergePropertyIndex::new(
        left_layer,
        &left_edges.edge_list,
        left_map,
        EitherIndex::Left,
    )
    .merge_chunks_by::<Vec<_>>(
        MergePropertyIndex::new(
            right_layer,
            &right_edges.edge_list,
            right_map,
            EitherIndex::Right,
        ),
        chunk_size,
        |(l, _), (r, _)| l.cmp(r),
        |(_, idx)| idx,
    ) {
        let mut cols = merge_props_chunk(left_props, right_props, &fields, &col_idx, &chunk_index);

        // first time column is a physical type of i64 but the logical type can be different
        if cols[0].data_type() != fields[0].data_type() {
            let col0 = cast(
                cols[0].as_ref(),
                fields[0].data_type(),
                CastOptions::unchecked(),
            )?;
            cols[0] = col0;
        }
        props.push_chunk(cols)?;
    }
    let props = props.finish()?;
    Ok(props)
}
