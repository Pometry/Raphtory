use crate::{
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::{ChunkedOffsets, ExplodedIndexIter},
        list_array::ChunkedListArray,
        mutable_chunked_array::{
            ChunkedOffsetsBuilder, MutChunkedOffsets, MutChunkedStructArray,
            MutPrimitiveChunkedArray,
        },
    },
    file_prefix::GraphPaths,
    merge::{
        merge_chunks::{IndexedView, MutChunk, OutputChunk},
        merge_props::{merge_fields, merge_props_chunk},
        reindex_node_offsets, EitherIndex, EitherOrBothIndex,
    },
    prelude::{ArrayOps, BaseArrayOps, Chunked, IntoUtf8Col, PrimitiveCol},
    properties::{ConstProps, Properties, TemporalProps},
    RAError,
};
use itertools::EitherOrBoth;
use polars_arrow::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{Field, PhysicalType, PrimitiveType},
    types::{NativeType, Offset},
};
use raphtory_api::{atomic_extra::atomic_usize_from_mut_slice, compute::par_cum_sum};
use rayon::prelude::*;
use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Range,
    path::Path,
    sync::atomic::Ordering,
};
use tracing::instrument;

#[instrument(level = "debug", skip_all)]
pub fn merge_properties<Index>(
    graph_dir: &Path,
    left: &Properties<Index>,
    left_map: &[usize],
    right: &Properties<Index>,
    right_map: &[usize],
    num_nodes: usize,
) -> Result<Properties<Index>, RAError>
where
    usize: From<Index>,
{
    let const_props = match (left.const_props.as_ref(), right.const_props.as_ref()) {
        (Some(left_props), Some(right_props)) => Some(merge_const_properties(
            graph_dir,
            left_props,
            left_map,
            right_props,
            right_map,
        )?),
        (None, Some(props)) => Some(copy_and_reindex_props(
            graph_dir, props, right_map, num_nodes,
        )?),
        (Some(props), None) => Some(copy_and_reindex_props(
            graph_dir, props, left_map, num_nodes,
        )?),
        (None, None) => None,
    };
    let temporal_props =
        merge_temp_properties(graph_dir, left, left_map, right, right_map, num_nodes)?;
    Ok(Properties::new(const_props, temporal_props))
}

fn get_fields<Index>(props: &Properties<Index>) -> Vec<Vec<Field>>
where
    usize: From<Index>,
{
    props
        .temporal_props()
        .into_iter()
        .map(|props| props.prop_dtypes().to_vec())
        .collect::<Vec<_>>()
}

fn resolve_merge_t_props(
    left: &[Vec<Field>],
    right: &[Vec<Field>],
) -> Result<
    Vec<(
        Vec<Field>,
        Vec<EitherOrBoth<usize>>,
        (Option<usize>, Option<usize>),
    )>,
    RAError,
> {
    if left.is_empty() && right.is_empty() {
        return Ok(vec![]);
    }
    let mut field_results: HashMap<Field, EitherOrBoth<(usize, usize)>> = left
        .iter()
        .enumerate()
        .flat_map(|(l_id, layer)| {
            layer
                .iter()
                .enumerate()
                .map(move |(f_id, field)| (field.clone(), EitherOrBoth::Left((l_id, f_id))))
        })
        .collect::<HashMap<_, _>>();

    for (l_id, l_fields) in left.iter().enumerate() {
        for (r_id, r_fields) in right.iter().enumerate() {
            let (fields, merge_outcome) = merge_fields(&l_fields, r_fields)?;
            for (field, outcome) in fields.iter().zip(&merge_outcome) {
                match field_results.entry(field.clone()) {
                    Entry::Occupied(occupied_entry) => {
                        if outcome.is_both() {
                            // we found a field that exists in both sides
                            *occupied_entry.into_mut() =
                                outcome.clone().map_any(|id| (l_id, id), |id| (r_id, id));
                        }
                    }
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry
                            .insert(outcome.clone().map_any(|id| (l_id, id), |id| (r_id, id)));
                    }
                }
            }
        }
    }

    for (l_id, fields) in right.iter().enumerate() {
        for (field_id, field) in fields.iter().enumerate() {
            if !field_results.contains_key(field) {
                field_results.insert(field.clone(), EitherOrBoth::Right((l_id, field_id)));
            }
        }
    }

    let mut field_results = field_results.into_iter().collect::<Vec<_>>();
    field_results.sort_by_key(|(_, outcome)| key(outcome));

    let (field, last_outcome) = &field_results[0];
    let (mut last_l_id, mut last_r_id, _, _) = key(last_outcome);

    let mut layer_outcomes = vec![(
        vec![field.clone()],
        vec![field_ids(last_outcome)],
        (
            (last_l_id != usize::MAX).then_some(last_l_id),
            (last_r_id != usize::MAX).then_some(last_r_id),
        ),
    )];

    let mut layer = 0;
    for (field, outcome) in &field_results[1..] {
        let (l_id, r_id, _, _) = key(outcome);
        if (l_id, r_id) != (last_l_id, last_r_id) {
            layer += 1;
            layer_outcomes.push((
                vec![],
                vec![],
                (
                    (l_id != usize::MAX).then_some(l_id),
                    (r_id != usize::MAX).then_some(r_id),
                ),
            ));
        }

        layer_outcomes[layer].0.push(field.clone());
        layer_outcomes[layer].1.push(field_ids(outcome));

        last_l_id = l_id;
        last_r_id = r_id;
    }

    Ok(layer_outcomes)
}

fn key(outcome: &EitherOrBoth<(usize, usize)>) -> (usize, usize, usize, usize) {
    match outcome {
        EitherOrBoth::Both((l_id, l_f_id), (r_id, r_f_id)) => (*l_id, *r_id, *l_f_id, *r_f_id),
        EitherOrBoth::Left((l_id, l_f_id)) => (*l_id, usize::MAX, *l_f_id, usize::MAX),
        EitherOrBoth::Right((r_id, r_f_id)) => (usize::MAX, *r_id, usize::MAX, *r_f_id),
    }
}

fn field_ids(outcome: &EitherOrBoth<(usize, usize)>) -> EitherOrBoth<usize> {
    match outcome {
        EitherOrBoth::Both((_, l_f_id), (_, r_f_id)) => EitherOrBoth::Both(*l_f_id, *r_f_id),
        EitherOrBoth::Left((_, l_f_id)) => EitherOrBoth::Left(*l_f_id),
        EitherOrBoth::Right((_, r_f_id)) => EitherOrBoth::Right(*r_f_id),
    }
}

#[derive(Copy, Clone, Debug)]
struct MergePropertyIndexWithSecondary<'a> {
    index: usize,
    reduced_index: usize,
    timestamps: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    secondary: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    index_map: &'a [usize],
    index_type: fn(usize) -> EitherIndex,
}

impl<'a> MergePropertyIndexWithSecondary<'a> {
    fn new<Index>(
        timestamps: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
        secondary: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
        index_map: &'a [usize],
        index_type: fn(usize) -> EitherIndex,
    ) -> Self
    where
        usize: From<Index>,
    {
        Self {
            index: 0,
            reduced_index: 0,
            timestamps,
            secondary,
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

impl<'a> IndexedView for MergePropertyIndexWithSecondary<'a> {
    type Value = ((usize, (i64, u64)), EitherIndex);

    #[inline]
    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.timestamps.values().len() {
            let reduced_index = self.reduced(index);
            Some((
                (
                    self.index_map[reduced_index],
                    (
                        self.timestamps.values().get(index),
                        self.secondary.values().get(index),
                    ),
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

        ExplodedIndexIter::new_from_index_range(
            self.timestamps.offsets(),
            start_index,
            start_offset,
            end_index,
            end_offset,
        )
        .map(|i| self.index_map[i])
        .zip(
            self.timestamps
                .values()
                .slice(start_offset..end_offset)
                .zip(self.secondary.values().slice(start_offset..end_offset)),
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

struct TPropsOutputWithSecondary {
    timestamps: Vec<i64>,
    secondary: Vec<u64>,
    chunk_idx: Vec<EitherIndex>,
}

struct TPropsOutputRefWithSecondary<'a> {
    timestamps: &'a mut [i64],
    secondary: &'a mut [u64],
    chunk_idx: &'a mut [EitherIndex],
}

impl<'b> MutChunk for TPropsOutputRefWithSecondary<'b> {
    type Value = ((i64, u64), EitherIndex);
    type Chunk<'a>
        = TPropsOutputRefWithSecondary<'a>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        self.timestamps
            .iter_mut()
            .zip(self.secondary.iter_mut())
            .zip(self.chunk_idx.iter_mut())
            .zip(values)
            .for_each(|(((t_entry, s_entry), idx_entry), ((t, s), idx))| {
                *t_entry = t;
                *s_entry = s;
                *idx_entry = idx;
            });
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.timestamps
            .par_iter_mut()
            .zip(self.secondary.par_iter_mut())
            .zip(self.chunk_idx.par_iter_mut())
            .zip(values)
            .for_each(|(((t_entry, s_entry), idx_entry), ((t, s), idx))| {
                *t_entry = t;
                *s_entry = s;
                *idx_entry = idx;
            });
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>> {
        self.timestamps
            .par_chunks_mut(chunk_size)
            .zip(self.secondary.par_chunks_mut(chunk_size))
            .zip(self.chunk_idx.par_chunks_mut(chunk_size))
            .map(
                |((timestamps, secondary), chunk_idx)| TPropsOutputRefWithSecondary {
                    timestamps,
                    secondary,
                    chunk_idx,
                },
            )
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        TPropsOutputRefWithSecondary {
            timestamps: self.timestamps.tail(start),
            secondary: self.secondary.tail(start),
            chunk_idx: self.chunk_idx.tail(start),
        }
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        self.timestamps.copy_within(range.clone(), dst);
        self.secondary.copy_within(range.clone(), dst);
        self.chunk_idx.copy_within(range, dst);
    }

    fn len(&self) -> usize {
        self.timestamps.len()
    }
}

impl OutputChunk for TPropsOutputWithSecondary {
    type Value = ((i64, u64), EitherIndex);
    type Chunk<'a>
        = TPropsOutputRefWithSecondary<'a>
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.timestamps.truncate(len);
        self.secondary.truncate(len);
        self.chunk_idx.truncate(len);
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        Self {
            timestamps: vec![0; len],
            secondary: vec![0; len],
            chunk_idx: vec![EitherIndex::Empty; len],
        }
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        TPropsOutputRefWithSecondary {
            timestamps: &mut self.timestamps,
            secondary: &mut self.secondary,
            chunk_idx: &mut self.chunk_idx,
        }
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.timestamps
            .par_drain_tail(start)
            .zip(self.secondary.par_drain_tail(start))
            .zip(self.chunk_idx.par_drain_tail(start))
    }
}

#[derive(Copy, Clone, Debug)]
struct MergePropertyIndex<'a> {
    index: usize,
    reduced_index: usize,
    timestamps: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    index_map: &'a [usize],
    index_type: fn(usize) -> EitherIndex,
}

impl<'a> MergePropertyIndex<'a> {
    fn new<Index>(
        timestamps: &'a ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
        index_map: &'a [usize],
        index_type: fn(usize) -> EitherIndex,
    ) -> Self
    where
        usize: From<Index>,
    {
        Self {
            index: 0,
            reduced_index: 0,
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
    type Value = ((usize, i64), EitherIndex);

    #[inline]
    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.timestamps.values().len() {
            let reduced_index = self.reduced(index);
            Some((
                (
                    self.index_map[reduced_index],
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

        ExplodedIndexIter::new_from_index_range(
            self.timestamps.offsets(),
            start_index,
            start_offset,
            end_index,
            end_offset,
        )
        .map(|i| self.index_map[i])
        .zip(self.timestamps.values().slice(start_offset..end_offset))
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

struct TPropsOutput {
    timestamps: Vec<i64>,
    chunk_idx: Vec<EitherIndex>,
}

struct TPropsOutputRef<'a> {
    timestamps: &'a mut [i64],
    chunk_idx: &'a mut [EitherIndex],
}

impl<'b> MutChunk for TPropsOutputRef<'b> {
    type Value = (i64, EitherIndex);
    type Chunk<'a>
        = TPropsOutputRef<'a>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        self.timestamps
            .iter_mut()
            .zip(self.chunk_idx.iter_mut())
            .zip(values)
            .for_each(|((t_entry, idx_entry), (t, idx))| {
                *t_entry = t;
                *idx_entry = idx;
            });
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.timestamps
            .par_iter_mut()
            .zip(self.chunk_idx.par_iter_mut())
            .zip(values)
            .for_each(|((t_entry, idx_entry), (t, idx))| {
                *t_entry = t;
                *idx_entry = idx;
            });
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>> {
        self.timestamps
            .par_chunks_mut(chunk_size)
            .zip(self.chunk_idx.par_chunks_mut(chunk_size))
            .map(|(timestamps, chunk_idx)| TPropsOutputRef {
                timestamps,
                chunk_idx,
            })
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        TPropsOutputRef {
            timestamps: self.timestamps.tail(start),
            chunk_idx: self.chunk_idx.tail(start),
        }
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        self.timestamps.copy_within(range.clone(), dst);
        self.chunk_idx.copy_within(range, dst);
    }

    fn len(&self) -> usize {
        self.timestamps.len()
    }
}

impl OutputChunk for TPropsOutput {
    type Value = (i64, EitherIndex);
    type Chunk<'a>
        = TPropsOutputRef<'a>
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.timestamps.truncate(len);
        self.chunk_idx.truncate(len);
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        Self {
            timestamps: vec![0; len],
            chunk_idx: vec![EitherIndex::Empty; len],
        }
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        TPropsOutputRef {
            timestamps: &mut self.timestamps,
            chunk_idx: &mut self.chunk_idx,
        }
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.timestamps
            .par_drain_tail(start)
            .zip(self.chunk_idx.par_drain_tail(start))
    }
}

fn merge_temp_properties<Index>(
    graph_dir: &Path,
    left: &Properties<Index>,
    left_map: &[usize],
    right: &Properties<Index>,
    right_map: &[usize],
    num_nodes: usize,
) -> Result<Vec<TemporalProps<Index>>, RAError>
where
    usize: From<Index>,
{
    let left_fields = get_fields(left);
    let right_fields = get_fields(right);

    let mut node_t_props = vec![];

    let node_t_props_path = graph_dir.join(GraphPaths::NodeTProps.as_ref());

    for (l_id, (fields, col_idx, (l, r))) in resolve_merge_t_props(&left_fields, &right_fields)?
        .into_iter()
        .enumerate()
    {
        let sparse_props_path = node_t_props_path.join(l_id.to_string());
        std::fs::create_dir_all(&sparse_props_path)?;

        let left = l.and_then(|l| left.temporal_props().get(l));
        let right = r.and_then(|r| right.temporal_props().get(r));

        match (left, right) {
            (Some(left), Some(right)) => {
                let offsets = merge_node_offsets(
                    left.temporal_props().offsets(),
                    left_map,
                    right.temporal_props().offsets(),
                    right_map,
                    &sparse_props_path,
                    GraphPaths::NodeTPropsOffsets,
                )?;

                let prop_chunk_size = left
                    .temporal_props()
                    .values()
                    .chunk_size()
                    .max(right.temporal_props().values().chunk_size());

                let mut props = MutChunkedStructArray::new_persisted(
                    prop_chunk_size,
                    &sparse_props_path,
                    GraphPaths::NodeTProps,
                    fields.clone(),
                );
                let mut timestamps = MutPrimitiveChunkedArray::new_persisted(
                    prop_chunk_size,
                    &sparse_props_path,
                    GraphPaths::NodeTPropsTimestamps,
                );

                match (left.secondary_index_col(), right.secondary_index_col()) {
                    (None, None) => {
                        for chunk in
                            MergePropertyIndex::new(left.time_col(), left_map, EitherIndex::Left)
                                .merge_chunks_by::<TPropsOutput>(
                                MergePropertyIndex::new(
                                    right.time_col(),
                                    right_map,
                                    EitherIndex::Right,
                                ),
                                prop_chunk_size,
                                |(l, _), (r, _)| l.cmp(r),
                                |((_, t), v)| (t, v),
                            )
                        {
                            timestamps.push_chunk(chunk.timestamps)?;
                            let props_chunk = merge_props_chunk(
                                left.temporal_props().values(),
                                right.temporal_props().values(),
                                &fields,
                                &col_idx,
                                &chunk.chunk_idx,
                            );
                            props.push_chunk(props_chunk)?;
                        }
                        node_t_props.push(TemporalProps::new(
                            offsets,
                            props.finish()?,
                            timestamps.finish()?,
                            None,
                        ));
                    }
                    (Some(left_sc_col), None) => {
                        let mut secondary = MutPrimitiveChunkedArray::new_persisted(
                            prop_chunk_size,
                            &sparse_props_path,
                            GraphPaths::NodeTPropsSecondaryIndex,
                        );
                        merge_with_secondary(
                            MergePropertyIndexWithSecondary::new(
                                left.time_col(),
                                left_sc_col,
                                left_map,
                                EitherIndex::Left,
                            ),
                            left.temporal_props().values(),
                            MergePropertyIndex::new(
                                right.time_col(),
                                right_map,
                                EitherIndex::Right,
                            )
                            .map(|((n, t), idx)| ((n, (t, u64::MAX)), idx)),
                            right.temporal_props().values(),
                            &mut props,
                            &mut timestamps,
                            &mut secondary,
                            fields,
                            col_idx,
                        )?;
                        node_t_props.push(TemporalProps::new(
                            offsets,
                            props.finish()?,
                            timestamps.finish()?,
                            Some(secondary.finish()?),
                        ));
                    }
                    (None, Some(right_sc_col)) => {
                        let mut secondary = MutPrimitiveChunkedArray::new_persisted(
                            prop_chunk_size,
                            &sparse_props_path,
                            GraphPaths::NodeTPropsSecondaryIndex,
                        );
                        merge_with_secondary(
                            MergePropertyIndex::new(left.time_col(), left_map, EitherIndex::Left)
                                .map(|((n, t), idx)| ((n, (t, u64::MIN)), idx)),
                            left.temporal_props().values(),
                            MergePropertyIndexWithSecondary::new(
                                right.time_col(),
                                right_sc_col,
                                right_map,
                                EitherIndex::Right,
                            ),
                            right.temporal_props().values(),
                            &mut props,
                            &mut timestamps,
                            &mut secondary,
                            fields,
                            col_idx,
                        )?;
                        node_t_props.push(TemporalProps::new(
                            offsets,
                            props.finish()?,
                            timestamps.finish()?,
                            Some(secondary.finish()?),
                        ))
                    }
                    (Some(left_sc_col), Some(right_sc_col)) => {
                        let mut secondary = MutPrimitiveChunkedArray::new_persisted(
                            prop_chunk_size,
                            &sparse_props_path,
                            GraphPaths::NodeTPropsSecondaryIndex,
                        );
                        merge_with_secondary(
                            MergePropertyIndexWithSecondary::new(
                                left.time_col(),
                                left_sc_col,
                                left_map,
                                EitherIndex::Left,
                            ),
                            left.temporal_props().values(),
                            MergePropertyIndexWithSecondary::new(
                                right.time_col(),
                                right_sc_col,
                                right_map,
                                EitherIndex::Right,
                            ),
                            right.temporal_props().values(),
                            &mut props,
                            &mut timestamps,
                            &mut secondary,
                            fields,
                            col_idx,
                        )?;
                        node_t_props.push(TemporalProps::new(
                            offsets,
                            props.finish()?,
                            timestamps.finish()?,
                            Some(secondary.finish()?),
                        ));
                    }
                }
            }
            (None, Some(right_offsets)) => {
                let props = copy_and_reindex_temp_properties(
                    &sparse_props_path,
                    right_offsets,
                    right_map,
                    num_nodes,
                    &fields,
                )?;
                node_t_props.push(props);
            }
            (Some(left_offsets), None) => {
                let props = copy_and_reindex_temp_properties(
                    &sparse_props_path,
                    left_offsets,
                    left_map,
                    num_nodes,
                    &fields,
                )?;
                node_t_props.push(props);
            }
            (None, None) => {
                unreachable!("merge temporal properties without offsets on either side?")
            }
        };
    }
    Ok(node_t_props)
}

fn merge_with_secondary(
    left: impl IndexedView<Value = ((usize, (i64, u64)), EitherIndex)>,
    left_props: &ChunkedArray<StructArray>,
    right: impl IndexedView<Value = ((usize, (i64, u64)), EitherIndex)>,
    right_props: &ChunkedArray<StructArray>,
    props: &mut MutChunkedStructArray,
    timestamps: &mut MutPrimitiveChunkedArray<i64>,
    secondary: &mut MutPrimitiveChunkedArray<u64>,
    fields: Vec<Field>,
    col_idx: Vec<EitherOrBoth<usize>>,
) -> Result<(), RAError> {
    let prop_chunk_size = props.chunk_size();
    for chunk in left.merge_chunks_by::<TPropsOutputWithSecondary>(
        right,
        prop_chunk_size,
        |(l, _), (r, _)| l.cmp(r),
        |((_, t), v)| (t, v),
    ) {
        timestamps.push_chunk(chunk.timestamps)?;
        secondary.push_chunk(chunk.secondary)?;
        let props_chunk =
            merge_props_chunk(left_props, right_props, &fields, &col_idx, &chunk.chunk_idx);
        props.push_chunk(props_chunk)?;
    }
    Ok(())
}

fn copy_and_reindex_temp_properties<Index>(
    graph_dir: &Path,
    props: &TemporalProps<Index>,
    map: &[usize],
    num_nodes: usize,
    projection: &[Field],
) -> Result<TemporalProps<Index>, RAError>
where
    usize: From<Index>,
{
    let mut iter_chunks = props.temporal_props().values().iter_chunks().peekable();
    let projection = iter_chunks.peek().map(|first| {
        first
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(id, field)| {
                projection
                    .iter()
                    .position(|f| &f.name == &field.name)
                    .map(|_| id)
            })
            .collect::<Vec<_>>()
    });

    let fields = projection
        .as_ref()
        .map(|proj| {
            proj.iter()
                .map(|id| props.prop_dtypes()[*id].clone())
                .collect()
        })
        .unwrap_or_else(|| props.prop_dtypes().to_vec());

    let mut props_values = MutChunkedStructArray::new_persisted(
        props.temporal_props().values().chunk_size(),
        graph_dir,
        GraphPaths::NodeTProps,
        fields,
    );

    for chunk in iter_chunks {
        let columns = projection
            .as_ref()
            .map(|proj| {
                proj.iter()
                    .map(|id| chunk.values()[*id].clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| chunk.values().to_vec());
        props_values.push_chunk(columns)?;
    }
    let mut t_values = MutPrimitiveChunkedArray::new_persisted(
        props.time_col().values().chunk_size(),
        graph_dir,
        GraphPaths::NodeTPropsTimestamps,
    );
    for chunk in props.time_col().values().iter_chunks() {
        t_values.push_chunk(chunk.to_vec())?;
    }
    let s_values = if let Some(sc_index) = props.secondary_index_col() {
        let mut s_values = MutPrimitiveChunkedArray::new_persisted(
            sc_index.values().chunk_size(),
            graph_dir,
            GraphPaths::NodeTPropsSecondaryIndex,
        );
        for chunk in sc_index.values().iter_chunks() {
            s_values.push_chunk(chunk.to_vec())?;
        }
        Some(s_values.finish()?)
    } else {
        None
    };

    let offsets = ChunkedOffsetsBuilder::new_persisted(
        props.time_col().offsets().chunk_size(),
        graph_dir,
        GraphPaths::NodeTPropsOffsets,
    );
    let offsets = reindex_node_offsets(offsets, map, props.time_col().offsets(), num_nodes)?;

    let props_values = props_values.finish()?;
    let t_values = t_values.finish()?;

    Ok(TemporalProps::new(
        offsets,
        props_values,
        t_values,
        s_values,
    ))
}

#[derive(Debug, Copy, Clone)]
struct NodeIndexView<'a> {
    node_map: &'a [usize],
    index: usize,
}

impl<'a> NodeIndexView<'a> {
    fn new(node_map: &'a [usize]) -> Self {
        Self { node_map, index: 0 }
    }

    fn index(&self, index: usize) -> usize {
        self.index + index
    }
}

impl<'a> IndexedView for NodeIndexView<'a> {
    // (node, index)
    type Value = (usize, usize);

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        self.node_map.get(index).map(|&node| (node, index))
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        let start = self.index(range.start);
        let end = self.index(range.end);
        self.node_map[start..end].iter().copied().zip(start..end)
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.node_map.len())
    }

    fn len(&self) -> usize {
        self.node_map.len() - self.index
    }
}

fn merge_const_properties<Index>(
    graph_dir: &Path,
    left: &ConstProps<Index>,
    left_map: &[usize],
    right: &ConstProps<Index>,
    right_map: &[usize],
) -> Result<ConstProps<Index>, RAError>
where
    usize: From<Index>,
{
    // prioritises the right side in case the property already existed
    let chunk_size = left.props().chunk_size().max(right.props().chunk_size());
    let (fields, col_idx) = merge_fields(left.prop_dtypes(), right.prop_dtypes())?;
    let mut prop_values = MutChunkedStructArray::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::NodeConstProps,
        fields.clone(),
    );
    for chunk_idx in NodeIndexView::new(left_map).merge_join_chunks_by::<Vec<_>>(
        NodeIndexView::new(right_map),
        chunk_size,
        |(l, _), (r, _)| l.cmp(r),
        |merged| EitherOrBothIndex::from(merged.map_any(|(_, l)| l, |(_, r)| r)),
    ) {
        let props_chunk =
            merge_props_chunk(left.props(), right.props(), &fields, &col_idx, &chunk_idx);
        prop_values.push_chunk(props_chunk)?;
    }
    Ok(ConstProps::new(prop_values.finish()?))
}

fn copy_and_reindex_props<Index>(
    graph_dir: &Path,
    props: &ConstProps<Index>,
    map: &[usize],
    num_nodes: usize,
) -> Result<ConstProps<Index>, RAError>
where
    usize: From<Index>,
{
    let chunk_size = props.props().chunk_size();
    let num_chunks = num_nodes.div_ceil(chunk_size);
    let fields = props.prop_dtypes().to_vec();
    let mut prop_values = MutChunkedStructArray::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::NodeConstProps,
        fields.clone(),
    );

    let values = props.props();
    let mut start_idx = 0;
    for chunk_id in 0..num_chunks {
        let chunk_end = ((chunk_id + 1) * chunk_size).min(num_nodes);
        let chunk_start = chunk_id * chunk_size;
        let end_idx = map[start_idx..].partition_point(|&v| v < chunk_end) + start_idx;
        let mut cols = Vec::with_capacity(fields.len());
        fields
            .par_iter()
            .enumerate()
            .map(|(col, field)| {
                let dst_idx = map[start_idx..end_idx]
                    .iter()
                    .map(|v| v - chunk_id * chunk_size);
                match field.data_type().to_physical_type() {
                    PhysicalType::Primitive(dt) => match dt {
                        PrimitiveType::Int8 => reindex_primitive_col::<i8>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::Int16 => reindex_primitive_col::<i16>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::Int32 => reindex_primitive_col::<i32>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::Int64 => reindex_primitive_col::<i64>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::UInt8 => reindex_primitive_col::<u8>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::UInt16 => reindex_primitive_col::<u16>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::UInt32 => reindex_primitive_col::<u32>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::UInt64 => reindex_primitive_col::<u64>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::Float32 => reindex_primitive_col::<f32>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        PrimitiveType::Float64 => reindex_primitive_col::<f64>(
                            values,
                            col,
                            start_idx..end_idx,
                            dst_idx,
                            chunk_end - chunk_start,
                        )
                        .to_boxed(),
                        _ => {
                            unimplemented!("{:?} not supported as property type", field.data_type())
                        }
                    },
                    PhysicalType::Utf8 => reindex_utf8_col::<i32>(
                        values,
                        col,
                        start_idx..end_idx,
                        dst_idx,
                        chunk_end - chunk_start,
                    )
                    .to_boxed(),
                    PhysicalType::LargeUtf8 => reindex_utf8_col::<i64>(
                        values,
                        col,
                        start_idx..end_idx,
                        dst_idx,
                        chunk_end - chunk_start,
                    )
                    .to_boxed(),
                    _ => unimplemented!("{:?} not supported as property type", field.data_type()),
                }
            })
            .collect_into_vec(&mut cols);
        prop_values.push_chunk(cols)?;
        start_idx = end_idx;
    }
    Ok(ConstProps::new(prop_values.finish()?))
}

fn merge_node_offsets(
    left_offsets: &ChunkedOffsets,
    left_map: &[usize],
    right_offsets: &ChunkedOffsets,
    right_map: &[usize],
    graph_dir: &Path,
    offset_type: GraphPaths,
) -> Result<ChunkedOffsets, RAError> {
    let chunk_size = left_offsets.chunk_size().max(right_offsets.chunk_size());
    let num_nodes = right_map.last().unwrap().max(left_map.last().unwrap()) + 1;
    let num_chunks = num_nodes.div_ceil(chunk_size);
    let mut last_offset = 0;
    let mut offsets_builder = MutChunkedOffsets::new(
        chunk_size,
        Some((offset_type, graph_dir.to_path_buf())),
        true,
    );
    let mut left_start = 0;
    let mut right_start = 0;
    for chunk_id in 0..num_chunks {
        let start_id = chunk_id * chunk_size;
        let end_id = ((chunk_id + 1) * chunk_size).min(num_nodes);
        let len = end_id - start_id;
        let mut chunk = vec![0; len + 1];

        chunk[0] = last_offset;
        let mut_chunk = atomic_usize_from_mut_slice(&mut chunk[1..]);
        let end_id = (chunk_id + 1) * chunk_size;
        let left_end = left_map.partition_point(|x| x < &end_id);
        let right_end = right_map.partition_point(|x| x < &end_id);
        (left_start..left_end).into_par_iter().for_each(|i| {
            let (s, e) = left_offsets.start_end(i);
            mut_chunk[left_map[i] - start_id].fetch_add(e - s, Ordering::Relaxed);
        });
        (right_start..right_end).into_par_iter().for_each(|i| {
            let (s, e) = right_offsets.start_end(i);
            mut_chunk[right_map[i] - start_id].fetch_add(e - s, Ordering::Relaxed);
        });
        par_cum_sum(&mut chunk);
        last_offset = *chunk.last().unwrap();
        offsets_builder.push_chunk(chunk)?;
        left_start = left_end;
        right_start = right_end;
    }
    offsets_builder.finish()
}

fn reindex_primitive_col<T: NativeType>(
    values: &ChunkedArray<StructArray>,
    col: usize,
    src_range: Range<usize>,
    dst_index: impl Iterator<Item = usize>,
    chunk_size: usize,
) -> Box<dyn Array> {
    let col = values.primitive_col::<T>(col).unwrap().sliced(src_range);
    let mut values = vec![T::default(); chunk_size];
    let mut validity = MutableBitmap::from_len_zeroed(chunk_size);
    for (v, idx) in col.zip(dst_index) {
        if let Some(v) = v {
            values[idx] = v;
            validity.set(idx, true);
        }
    }
    let res = PrimitiveArray::new(
        col.data_type().unwrap().clone(),
        values.into(),
        Some(validity.freeze()),
    );
    res.to_boxed()
}

fn reindex_utf8_col<O: Offset>(
    values: &ChunkedArray<StructArray>,
    col: usize,
    src_range: Range<usize>,
    dst_index: impl Iterator<Item = usize>,
    chunk_size: usize,
) -> Box<dyn Array> {
    let col = values.into_utf8_col::<O>(col).unwrap().sliced(src_range);
    let mut values = vec![""; chunk_size];
    let mut validity = MutableBitmap::from_len_zeroed(chunk_size);
    for (v, idx) in col.zip(dst_index) {
        if let Some(v) = v {
            values[idx] = v;
            validity.set(idx, true);
        }
    }
    let mut res = Utf8Array::<O>::from_slice(&values);
    res.set_validity(Some(validity.freeze()));
    res.to_boxed()
}

#[cfg(test)]
mod test {
    use crate::{
        chunked_array::chunked_array::ChunkedArray,
        merge::merge_props::merge_node_props::merge_const_properties, properties::ConstProps,
    };
    use itertools::{EitherOrBoth, Itertools};
    use polars_arrow::{
        array::{Array, PrimitiveArray, StructArray},
        datatypes::{ArrowDataType, Field},
    };
    use raphtory_api::core::entities::VID;
    use tempfile::TempDir;

    use super::resolve_merge_t_props;

    #[test]
    fn resolve_merge_t_props_1_prop_full_match() {
        let a = Field::new("a", ArrowDataType::Int32, true);
        let l_fields = vec![vec![a.clone()]];
        let r_fields = vec![vec![a.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![(
            vec![a.clone()],
            vec![EitherOrBoth::Both(0, 0)],
            (Some(0), Some(0)),
        )];

        assert_eq!(actual, expected);
    }

    #[test]
    fn resolve_merge_t_props_full_match() {
        // all t_props on the left are matched with the right on the same layer

        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let l_fields = vec![vec![a.clone(), b.clone()]];
        let r_fields = vec![vec![a.clone(), b.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![(
            vec![a.clone(), b.clone()],
            vec![EitherOrBoth::Both(0, 0), EitherOrBoth::Both(1, 1)],
            (Some(0), Some(0)),
        )];

        assert_eq!(actual, expected);
    }

    #[test]
    fn resolve_merge_t_props_no_match() {
        // none of the t_props on the left are matched with the right on the same layer

        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let l_fields = vec![vec![b.clone()]];
        let r_fields = vec![vec![a.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![
            (
                vec![b.clone()],
                vec![EitherOrBoth::Left(0)],
                (Some(0), None),
            ),
            (
                vec![a.clone()],
                vec![EitherOrBoth::Right(0)],
                (None, Some(0)),
            ),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn resolve_merge_t_props_overlap() {
        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let c = Field::new("c", ArrowDataType::UInt16, true);
        let l_fields = vec![vec![a.clone(), b.clone()]];
        let r_fields = vec![vec![b.clone(), c.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        // things that are merged go first
        let expected = vec![
            (
                vec![b.clone()],
                vec![EitherOrBoth::Both(1, 0)],
                (Some(0), Some(0)),
            ),
            (
                vec![a.clone()],
                vec![EitherOrBoth::Left(0)],
                (Some(0), None),
            ),
            (
                vec![c.clone()],
                vec![EitherOrBoth::Right(1)],
                (None, Some(0)),
            ),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn resolve_merge_t_props_2_layers_match_1() {
        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let l_fields = vec![vec![a.clone()], vec![b.clone()]];
        let r_fields = vec![vec![a.clone()], vec![b.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![
            (
                vec![a.clone()],
                vec![EitherOrBoth::Both(0, 0)],
                (Some(0), Some(0)),
            ),
            (
                vec![b.clone()],
                vec![EitherOrBoth::Both(0, 0)],
                (Some(1), Some(1)),
            ),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn resolve_merge_t_props_2_layers_match_2() {
        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let c = Field::new("c", ArrowDataType::UInt16, true);
        let d = Field::new("d", ArrowDataType::UInt16, true);

        let l_fields = vec![vec![a.clone(), b.clone()], vec![c.clone(), d.clone()]];
        let r_fields = vec![vec![a.clone(), b.clone()], vec![c.clone(), d.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![
            (
                vec![a.clone(), b.clone()],
                vec![EitherOrBoth::Both(0, 0), EitherOrBoth::Both(1, 1)],
                (Some(0), Some(0)),
            ),
            (
                vec![c.clone(), d.clone()],
                vec![EitherOrBoth::Both(0, 0), EitherOrBoth::Both(1, 1)],
                (Some(1), Some(1)),
            ),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn resolve_merge_2_layers_inverse_overlap_2() {
        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let c = Field::new("c", ArrowDataType::UInt16, true);
        let d = Field::new("d", ArrowDataType::UInt16, true);

        let l_fields = vec![vec![a.clone(), b.clone()], vec![c.clone(), d.clone()]];
        let r_fields = vec![vec![c.clone(), d.clone()], vec![a.clone(), b.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![
            (
                vec![a.clone(), b.clone()],
                vec![EitherOrBoth::Both(0, 0), EitherOrBoth::Both(1, 1)],
                (Some(0), Some(1)),
            ),
            (
                vec![c.clone(), d.clone()],
                vec![EitherOrBoth::Both(0, 0), EitherOrBoth::Both(1, 1)],
                (Some(1), Some(0)),
            ),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn no_overlap() {
        let a = Field::new("a", ArrowDataType::Int32, true);
        let b = Field::new("b", ArrowDataType::UInt16, true);
        let c = Field::new("c", ArrowDataType::UInt16, true);
        let d = Field::new("d", ArrowDataType::UInt16, true);
        let e = Field::new("e", ArrowDataType::UInt16, true);
        let f = Field::new("f", ArrowDataType::UInt16, true);

        let l_fields = vec![vec![a.clone()], vec![b.clone()], vec![c.clone()]];
        let r_fields = vec![vec![d.clone()], vec![e.clone()], vec![f.clone()]];

        let actual = resolve_merge_t_props(&l_fields, &r_fields).unwrap();

        let expected = vec![
            (
                vec![a.clone()],
                vec![EitherOrBoth::Left(0)],
                (Some(0), None),
            ),
            (
                vec![b.clone()],
                vec![EitherOrBoth::Left(0)],
                (Some(1), None),
            ),
            (
                vec![c.clone()],
                vec![EitherOrBoth::Left(0)],
                (Some(2), None),
            ),
            (
                vec![d.clone()],
                vec![EitherOrBoth::Right(0)],
                (None, Some(0)),
            ),
            (
                vec![e.clone()],
                vec![EitherOrBoth::Right(0)],
                (None, Some(1)),
            ),
            (
                vec![f.clone()],
                vec![EitherOrBoth::Right(0)],
                (None, Some(2)),
            ),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn merge_const_props() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let dt_l = ArrowDataType::Struct(vec![Field::new("test", ArrowDataType::Int32, true)]);
        let left: ChunkedArray<StructArray> = vec![StructArray::new(
            dt_l,
            vec![PrimitiveArray::<i32>::from_iter([None, Some(1)]).to_boxed()],
            None,
        )]
        .into();

        let dt_r = ArrowDataType::Struct(vec![Field::new("test2", ArrowDataType::Int32, true)]);
        let right: ChunkedArray<StructArray> = vec![StructArray::new(
            dt_r,
            vec![PrimitiveArray::<i32>::from_iter([Some(1), None]).to_boxed()],
            None,
        )]
        .into();
        let left_props = ConstProps::<VID>::new(left);
        let right_props = ConstProps::<VID>::new(right);

        let test_dir = TempDir::new().unwrap();

        let merged = pool.install(|| {
            merge_const_properties(test_dir.path(), &left_props, &[0, 1], &right_props, &[0, 1])
                .unwrap()
        });

        assert_eq!(
            merged.prop_dtypes().iter().map(|f| &f.name).collect_vec(),
            ["test", "test2"]
        );
        assert_eq!(merged.prop_native::<i32>(VID(0), 0), None);
        assert_eq!(merged.prop_native::<i32>(VID(1), 0), Some(1));
        assert_eq!(merged.prop_native::<i32>(VID(0), 1), Some(1));
        assert_eq!(merged.prop_native::<i32>(VID(1), 1), None);
    }
}
