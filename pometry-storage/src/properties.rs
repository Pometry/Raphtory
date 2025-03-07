use std::{
    marker::PhantomData,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use crate::{
    arrow2::{
        array::{PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
    },
    prelude::Chunked,
    tprops::DiskTProp,
    Time,
};
use itertools::Itertools;
use polars_arrow::{
    array::Array,
    datatypes::{ArrowDataType, TimeUnit},
    types::{NativeType, Offset},
};
use raphtory_api::{
    compute::par_cum_sum,
    core::{entities::VID, storage::timeindex::AsTime},
};
use rayon::prelude::*;

use super::load::{list_parquet_files, parquet_reader::ParquetReader};
use crate::{
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::ChunkedOffsets,
        list_array::ChunkedListArray,
        mutable_chunked_array::{
            MutChunkedOffsets, MutChunkedStructArray, MutPrimitiveChunkedArray,
        },
    },
    file_prefix::GraphPaths,
    prelude::{ArrayOps, BaseArrayOps, IntoUtf8Col, PrimitiveCol},
    timestamps::TimeStamps,
    RAError,
};

#[derive(Debug, Default, Clone)]
pub struct Properties<Index> {
    pub const_props: Option<ConstProps<Index>>,
    pub temporal_props: Vec<TemporalProps<Index>>, // why Option?
}

impl<Index> PartialEq for Properties<Index>
where
    usize: From<Index>,
{
    fn eq(&self, other: &Self) -> bool {
        for (l_l, l_f, l_field) in self.fields() {
            if let Some((r_l, r_f, _)) = other.fields().find(|(_, _, f)| &l_field.name == &f.name) {
                let check = || -> Option<bool> {
                    let l_prop = self.temporal_props().get(l_l)?;
                    let r_prop = other.temporal_props().get(r_l)?;

                    (l_prop.prop_dtype(l_f) == r_prop.prop_dtype(r_f)).then(|| true)?;

                    match l_prop.prop_dtype(l_f).data_type() {
                        ArrowDataType::UInt64 => {
                            check_native::<_, u64>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::UInt32 => {
                            check_native::<_, u32>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::UInt16 => {
                            check_native::<_, u16>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::UInt8 => {
                            check_native::<_, u8>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Float32 => {
                            check_native::<_, f32>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Float64 => {
                            check_native::<_, f64>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Int64 => {
                            check_native::<_, i64>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Int32 => {
                            check_native::<_, i32>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Int16 => {
                            check_native::<_, i16>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Int8 => {
                            check_native::<_, i8>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Utf8 => {
                            check_utf8::<_, i32>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::LargeUtf8 => {
                            check_utf8::<_, i64>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                            check_native::<_, i64>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Date64 => {
                            check_native::<_, i64>(l_prop, l_f, r_prop, r_f)?;
                        }
                        ArrowDataType::Boolean => {
                            check_bool::<_>(l_prop, l_f, r_prop, r_f)?;
                        }
                        dt => {
                            unimplemented!("NOT YET {dt:?}")
                        }
                    }

                    Some(true)
                };
                return check().unwrap_or(false);
            } else {
                return false;
            }
        }

        self.const_props == other.const_props
    }
}

fn check_native<Index, T: NativeType + PartialEq>(
    l_prop: &TemporalProps<Index>,
    l_f: usize,
    r_prop: &TemporalProps<Index>,
    r_f: usize,
) -> Option<bool> {
    let l_col = l_prop.props.primitive_col::<T>(l_f)?;
    let r_col = r_prop.props.primitive_col::<T>(r_f)?;
    let x = l_col
        .values()
        .iter()
        .flatten()
        .zip_eq(r_col.values().iter().flatten())
        .all(|(l, r)| l == r)
        .then(|| true);
    x
}

fn check_utf8<Index, I: Offset>(
    l_prop: &TemporalProps<Index>,
    l_f: usize,
    r_prop: &TemporalProps<Index>,
    r_f: usize,
) -> Option<bool> {
    let l_col = l_prop.props.utf8_col::<I>(l_f)?;
    let r_col = r_prop.props.utf8_col::<I>(r_f)?;
    let x = l_col
        .values()
        .iter()
        .flatten()
        .zip_eq(r_col.values().iter().flatten())
        .all(|(l, r)| l == r)
        .then(|| true);
    x
}

fn check_bool<Index>(
    l_prop: &TemporalProps<Index>,
    l_f: usize,
    r_prop: &TemporalProps<Index>,
    r_f: usize,
) -> Option<bool> {
    let l_col = l_prop.props.bool_col(l_f)?;
    let r_col = r_prop.props.bool_col(r_f)?;
    let x = l_col
        .values()
        .iter()
        .flatten()
        .zip_eq(r_col.values().iter().flatten())
        .all(|(l, r)| l == r)
        .then(|| true);
    x
}

impl<Index> Properties<Index> {
    pub fn new(
        const_props: Option<ConstProps<Index>>,
        temporal_props: impl IntoIterator<Item = TemporalProps<Index>>,
    ) -> Self {
        Self {
            const_props,
            temporal_props: temporal_props.into_iter().collect(),
        }
    }

    pub fn const_props(&self) -> Option<&ConstProps<Index>> {
        self.const_props.as_ref()
    }

    pub fn temporal_props(&self) -> &[TemporalProps<Index>] {
        &self.temporal_props
    }

    pub fn add_temporal_props(&mut self, temporal_props: TemporalProps<Index>) {
        self.temporal_props.push(temporal_props);
    }
}

impl<Index> Properties<Index>
where
    usize: From<Index>,
{
    pub fn fields(&self) -> impl Iterator<Item = (usize, usize, &Field)> {
        self.temporal_props
            .iter()
            .enumerate()
            .flat_map(|(l_id, t)| {
                t.prop_dtypes()
                    .into_iter()
                    .enumerate()
                    .map(move |(f_id, field)| (l_id, f_id, field))
            })
    }
}

#[derive(Debug, Clone)]
pub struct ConstProps<Index> {
    props: ChunkedArray<StructArray>,
    _index: PhantomData<Index>,
}

impl<Index> PartialEq for ConstProps<Index> {
    fn eq(&self, other: &Self) -> bool {
        props_eq(&self.props, &other.props)
    }
}

impl<Index> ConstProps<Index>
where
    usize: From<Index>,
{
    pub fn new(props: ChunkedArray<StructArray>) -> Self {
        Self {
            props,
            _index: PhantomData,
        }
    }

    pub fn empty() -> Self {
        Self::new(ChunkedArray::empty())
    }

    pub fn prop_dtype(&self, id: usize) -> &Field {
        &self.prop_dtypes()[id]
    }

    pub fn prop_dtypes(&self) -> &[Field] {
        match self.props.data_type() {
            None => &[],
            Some(DataType::Struct(fields)) => fields,
            _ => unreachable!(),
        }
    }

    pub fn prop_native<T: NativeType>(&self, index: Index, id: usize) -> Option<T> {
        self.props.get(index.into()).primitive_value(id).copied()
    }

    pub fn prop_bool(&self, index: Index, id: usize) -> Option<bool> {
        self.props.get(index.into()).bool_value(id)
    }

    pub fn prop_str(&self, index: Index, id: usize) -> Option<&str> {
        self.props.get(index.into()).str_value(id)
    }

    pub fn num_props(&self) -> usize {
        self.props.num_cols()
    }

    pub fn has_prop(&self, index: Index, id: usize) -> bool {
        self.props.get(index.into()).is_valid(id)
    }

    pub fn props(&self) -> &ChunkedArray<StructArray> {
        &self.props
    }
}

#[derive(Debug, Clone, Default)]
pub struct TemporalProps<Index> {
    props: ChunkedListArray<'static, ChunkedArray<StructArray>>,
    timestamps: ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    secondary_index: Option<ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>>,
    _index: PhantomData<Index>,
}

impl<Index> PartialEq for TemporalProps<Index> {
    fn eq(&self, other: &Self) -> bool {
        props_eq(self.props.values(), other.props.values())
            && self.timestamps == other.timestamps
            && self.secondary_index == other.secondary_index
            && self.props.offsets() == other.props.offsets()
    }
}

impl<Index> TemporalProps<Index>
where
    usize: From<Index>,
{
    pub fn new(
        offsets: ChunkedOffsets,
        props: ChunkedArray<StructArray>,
        timestamps: ChunkedArray<PrimitiveArray<i64>, NonNull>,
        secondary_index: Option<ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    ) -> Self {
        TemporalProps {
            props: ChunkedListArray::new_from_parts(props, offsets.clone()),
            timestamps: ChunkedListArray::new_from_parts(timestamps, offsets.clone()),
            secondary_index: secondary_index
                .map(|secondary_index| ChunkedListArray::new_from_parts(secondary_index, offsets)),
            _index: Default::default(),
        }
    }

    pub fn from_structs(
        temporal_props: ChunkedArray<StructArray>,
        temporal_props_offsets: ChunkedOffsets,
    ) -> Self {
        let time_col_values = temporal_props
            .primitive_col::<i64>(0)
            .unwrap()
            .iter_chunks()
            .cloned()
            .collect::<Vec<_>>();

        let time_col_values = ChunkedArray::from_non_nulls(time_col_values);
        let time_col =
            ChunkedListArray::new_from_parts(time_col_values, temporal_props_offsets.clone());

        Self {
            props: ChunkedListArray::new_from_parts(temporal_props, temporal_props_offsets),
            timestamps: time_col,
            secondary_index: None,
            _index: Default::default(),
        }
    }

    pub fn with_new_offsets(&self, offsets: ChunkedOffsets) -> Self {
        let time_col = self.time_col().values().clone();
        let temporal_props = self.temporal_props().values().clone();
        let secondary_index = self
            .secondary_index
            .as_ref()
            .map(|secondary_index| secondary_index.values().clone());
        Self {
            timestamps: ChunkedListArray::new_from_parts(time_col, offsets.clone()),
            props: ChunkedListArray::new_from_parts(temporal_props, offsets.clone()),
            secondary_index: secondary_index
                .map(|secondary_index| ChunkedListArray::new_from_parts(secondary_index, offsets)),
            _index: Default::default(),
        }
    }

    pub fn empty() -> Self {
        TemporalProps::new(
            ChunkedOffsets::default(),
            ChunkedArray::empty(),
            ChunkedArray::empty(),
            None,
        )
    }
    pub fn prop_dtype(&self, id: usize) -> &Field {
        &self.prop_dtypes()[id]
    }

    pub fn prop_dtypes(&self) -> &[Field] {
        match self.props.data_type() {
            None => &[],
            Some(DataType::Struct(fields)) => fields,
            _ => unreachable!(),
        }
    }

    pub fn timestamps<T: AsTime>(&self, index: Index) -> TimeStamps<T> {
        let i = index.into();
        TimeStamps::new(
            self.timestamps.value(i),
            self.secondary_index_col().map(|index| index.value(i)),
        )
    }

    pub fn prop_for_ts<'a, T: AsTime>(
        &'a self,
        ts: TimeStamps<'a, T>,
        id: usize,
    ) -> DiskTProp<'a, T> {
        let dtype = self.prop_dtype(id);
        let props = self.props.values().slice(ts.timestamps().range());
        DiskTProp::new(dtype.data_type(), ts, props, id)
    }

    pub fn prop<T: AsTime>(&self, index: Index, id: usize) -> DiskTProp<T> {
        let dtype = self.prop_dtype(id);
        let i: usize = index.into();
        let timestamps = TimeStamps::new(
            self.timestamps.value(i),
            self.secondary_index_col().map(|index| index.value(i)),
        );
        let props = self.props.value(i);
        DiskTProp::new(dtype.data_type(), timestamps, props, id)
    }

    pub fn temporal_props(&self) -> &ChunkedListArray<'static, ChunkedArray<StructArray>> {
        &self.props
    }

    pub fn time_col(
        &self,
    ) -> &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>> {
        &self.timestamps
    }

    pub fn secondary_index_col(
        &self,
    ) -> Option<&ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>> {
        self.secondary_index.as_ref()
    }

    pub fn earliest_latest(&self) -> (Time, Time) {
        let timestamps = self.time_col();

        if timestamps.is_empty() {
            return (i64::MAX, i64::MIN);
        }
        timestamps
            .values()
            .par_iter()
            .fold_with((i64::MAX, i64::MIN), |(min, max), t| {
                (t.min(min), t.max(max))
            })
            .reduce(
                || (i64::MAX, i64::MIN),
                |(earliest_time, latest_time), (time1, time2)| {
                    (earliest_time.min(time1), latest_time.max(time2))
                },
            )
    }

    pub fn t_props_chunk_size(&self) -> usize {
        self.temporal_props().values().chunk_size()
    }

    pub fn property_id(&self, name: &str) -> Option<usize> {
        match self
            .prop_dtypes()
            .iter()
            .position(|field| &field.name == name)
        {
            Some(id) => Some(id),
            None => None,
        }
    }

    pub fn data_type_arrow(&self) -> Option<&DataType> {
        self.props.values().data_type()
    }

    pub fn has_t_props(&self, index: Index) -> bool {
        self.props.value(index.into()).len() > 0
    }

    pub fn t_len(&self) -> usize {
        self.temporal_props().values().len()
    }
}

pub(crate) fn props_eq(
    left: &ChunkedArray<StructArray>,
    right: &ChunkedArray<StructArray>,
) -> bool {
    if !(left.data_type() == right.data_type()) {
        return false;
    }
    if let Some(ArrowDataType::Struct(fields)) = left.data_type() {
        fields
            .iter()
            .enumerate()
            .all(|(col, field)| match field.data_type() {
                ArrowDataType::Int8 => left
                    .primitive_col::<i8>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<i8>(col).unwrap().iter()),
                ArrowDataType::Int16 => left
                    .primitive_col::<i16>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<i16>(col).unwrap().iter()),
                ArrowDataType::Int32 => left
                    .primitive_col::<i32>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<i32>(col).unwrap().iter()),
                ArrowDataType::Int64 => left
                    .primitive_col::<i64>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<i64>(col).unwrap().iter()),
                ArrowDataType::UInt8 => left
                    .primitive_col::<u8>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<u8>(col).unwrap().iter()),
                ArrowDataType::UInt16 => left
                    .primitive_col::<u16>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<u16>(col).unwrap().iter()),
                ArrowDataType::UInt32 => left
                    .primitive_col::<u32>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<u32>(col).unwrap().iter()),
                ArrowDataType::UInt64 => left
                    .primitive_col::<u64>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<u64>(col).unwrap().iter()),
                ArrowDataType::Float32 => left
                    .primitive_col::<f32>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<f32>(col).unwrap().iter()),
                ArrowDataType::Float64 => left
                    .primitive_col::<f64>(col)
                    .unwrap()
                    .iter()
                    .eq(right.primitive_col::<f64>(col).unwrap().iter()),
                ArrowDataType::Utf8 => left
                    .into_utf8_col::<i32>(col)
                    .unwrap()
                    .iter()
                    .eq(right.into_utf8_col::<i32>(col).unwrap().iter()),
                ArrowDataType::LargeUtf8 => left
                    .into_utf8_col::<i64>(col)
                    .unwrap()
                    .iter()
                    .eq(right.into_utf8_col::<i64>(col).unwrap().iter()),
                v => panic!("unsupported type {v:?}"),
            })
    } else {
        true
    }
}

#[derive(Debug)]
pub struct NodePropsBuilder<T> {
    node_count: usize,
    node_timestamps: Vec<T>,
    offsets: Vec<usize>,
    t_props: Vec<Box<dyn Array>>,
    t_fields: Vec<Field>,

    c_props: Vec<Box<dyn Array>>,
    c_fields: Vec<Field>,

    graph_dir: PathBuf,
}

impl<T: AsTime> NodePropsBuilder<T> {
    pub fn new(node_count: usize, graph_dir: impl AsRef<Path>) -> Self {
        NodePropsBuilder {
            node_count,
            node_timestamps: Vec::with_capacity(node_count),
            offsets: Vec::with_capacity(node_count + 1),
            t_props: Vec::new(),
            t_fields: Vec::new(),

            c_props: Vec::new(),
            c_fields: Vec::new(),

            graph_dir: graph_dir.as_ref().to_path_buf(),
        }
    }

    pub fn with_timestamps(self, node_ts_fn: impl Fn(VID) -> Vec<T> + Send + Sync) -> Self {
        let mut t_offsets = vec![0; self.node_count + 1];
        let node_timestamps: Vec<_> = t_offsets[1..]
            .par_iter_mut()
            .enumerate()
            .flat_map(|(vid, count)| {
                let ts: Vec<_> = node_ts_fn(VID(vid));
                *count = ts.len();
                ts
            })
            .collect();
        par_cum_sum(&mut t_offsets);

        NodePropsBuilder {
            node_count: self.node_count,
            node_timestamps,
            offsets: t_offsets,
            t_props: self.t_props,
            t_fields: self.t_fields,
            c_props: self.c_props,
            c_fields: self.c_fields,
            graph_dir: self.graph_dir,
        }
    }

    pub fn with_temporal_props<
        F: Fn(usize, &str, &[T], &[usize]) -> Option<(Field, Box<dyn Array>)> + Send + Sync,
    >(
        self,
        prop_keys: Vec<String>,
        node_prop_fn: F,
    ) -> Self {
        let (t_fields, t_props): (Vec<_>, Vec<_>) = prop_keys
            .par_iter()
            .enumerate()
            .map(|(id, key)| node_prop_fn(id, key, &self.node_timestamps, &self.offsets))
            .flatten()
            .unzip();

        NodePropsBuilder {
            node_count: self.node_count,
            node_timestamps: self.node_timestamps,
            offsets: self.offsets,
            t_props,
            t_fields,
            c_props: self.c_props,
            c_fields: self.c_fields,
            graph_dir: self.graph_dir,
        }
    }

    pub fn with_const_props<F: Fn(usize, &str) -> Option<(Field, Box<dyn Array>)> + Send + Sync>(
        self,
        prop_keys: Vec<String>,
        node_prop_fn: F,
    ) -> Self {
        let (c_fields, c_props): (Vec<_>, Vec<_>) = prop_keys
            .par_iter()
            .enumerate()
            .map(|(id, key)| node_prop_fn(id, key))
            .flatten()
            .unzip();

        NodePropsBuilder {
            node_count: self.node_count,
            node_timestamps: self.node_timestamps,
            offsets: self.offsets,
            t_props: self.t_props,
            t_fields: self.t_fields,
            c_props,
            c_fields,
            graph_dir: self.graph_dir,
        }
    }

    pub fn build<Index>(self) -> Result<Properties<Index>, RAError>
    where
        usize: From<Index>,
    {
        let const_props = if self.c_props.is_empty() {
            ConstProps::empty()
        } else {
            let mut const_props_builder = MutChunkedStructArray::new_persisted(
                self.node_count,
                &self.graph_dir,
                GraphPaths::NodeConstProps,
                self.c_fields,
            );
            const_props_builder.push_chunk(self.c_props)?;
            ConstProps::new(const_props_builder.finish()?)
        };

        let temporal_props = if self.t_props.is_empty() {
            TemporalProps::empty()
        } else {
            let num_timestamps = self.node_timestamps.len();
            let n = self.node_count;

            let node_t_props_path = self
                .graph_dir
                .join(GraphPaths::NodeTProps.as_ref())
                .join("0");
            std::fs::create_dir_all(&node_t_props_path)?;

            let mut offsets_builder = MutChunkedOffsets::new(
                n,
                Some((
                    GraphPaths::NodeTPropsOffsets,
                    node_t_props_path.to_path_buf(),
                )),
                true,
            );
            offsets_builder.push_chunk(self.offsets)?;
            let mut tprops_builder = MutChunkedStructArray::new_persisted(
                num_timestamps,
                &node_t_props_path,
                GraphPaths::NodeTProps,
                self.t_fields,
            );
            tprops_builder.push_chunk(self.t_props)?;

            let mut tprops_timestamp_builder = MutPrimitiveChunkedArray::new_persisted(
                num_timestamps,
                &node_t_props_path,
                GraphPaths::NodeTPropsTimestamps,
            );
            let mut tprops_secondary_index_builder = MutPrimitiveChunkedArray::new_persisted(
                num_timestamps,
                &node_t_props_path,
                GraphPaths::NodeTPropsSecondaryIndex,
            );
            let mut timestamps = Vec::with_capacity(num_timestamps);
            let mut secondary_index = Vec::with_capacity(num_timestamps);
            self.node_timestamps
                .into_par_iter()
                .map(|t| (t.t(), t.i() as u64))
                .unzip_into_vecs(&mut timestamps, &mut secondary_index);
            tprops_timestamp_builder.push_chunk(timestamps)?;
            tprops_secondary_index_builder.push_chunk(secondary_index)?;

            let temporal_props_values = tprops_builder.finish()?;
            let timestamps = tprops_timestamp_builder.finish()?;
            let secondary_index = tprops_secondary_index_builder.finish()?;
            let t_offsets = offsets_builder.finish()?;

            TemporalProps::new(
                t_offsets,
                temporal_props_values,
                timestamps,
                Some(secondary_index),
            )
        };

        Ok(Properties::new(Some(const_props), vec![temporal_props]))
    }
}

pub fn node_ts<'a, T>(vid: VID, offsets: &'a [usize], node_timestamps: &'a [T]) -> &'a [T] {
    let start = offsets[vid.0];
    let end = offsets[vid.0 + 1];
    &node_timestamps[start..end]
}

pub(crate) fn load_node_properties_from_parquet(
    graph_dir: impl AsRef<Path> + Send + Sync,
    parquet_path: impl AsRef<Path>,
    num_threads: NonZeroUsize,
    chunk_size: usize,
    id_col: Option<&str>,
) -> Result<Properties<VID>, RAError> {
    let files = list_parquet_files(parquet_path)?;

    let reader = match id_col {
        Some(id_col) => ParquetReader::new_from_filelist(
            graph_dir,
            files,
            None,
            GraphPaths::NodeConstProps,
            |name| name != id_col,
        )?,
        None => ParquetReader::new_from_filelist(
            graph_dir,
            files,
            None,
            GraphPaths::NodeConstProps,
            |_| true,
        )?,
    };

    let values = reader.load_values(num_threads, chunk_size)?;

    let chunked_arrays: ChunkedArray<StructArray> = values.into();
    let const_props = ConstProps::new(chunked_arrays);

    Ok(Properties::new(Some(const_props), vec![]))
}

pub(crate) fn load_node_properties_from_dir(
    graph_dir: impl AsRef<Path>,
    mmap: bool,
) -> Result<Properties<VID>, RAError> {
    let graph_dir = graph_dir.as_ref();

    let node_t_props_path = graph_dir.join(GraphPaths::NodeTProps.as_ref());

    let mut temporal_props: Vec<TemporalProps<VID>> = vec![];

    for entry in 0usize..
    // make sure these are loaded in order 0, 1, 2, 3, ..
    {
        let entry = node_t_props_path.join(entry.to_string());

        if !entry.exists() {
            break;
        }

        let graph_dir = entry.as_path();
        let t_props_offsets =
            unsafe { ChunkedOffsets::mmap_from_dir(graph_dir, GraphPaths::NodeTPropsOffsets) }?;
        let t_props_timestamps = unsafe {
            ChunkedArray::<PrimitiveArray<i64>, NonNull>::load_from_dir(
                graph_dir,
                GraphPaths::NodeTPropsTimestamps,
                mmap,
            )
        }?;
        let t_props_secondary_index = unsafe {
            ChunkedArray::<PrimitiveArray<u64>, NonNull>::load_from_dir(
                graph_dir,
                GraphPaths::NodeTPropsSecondaryIndex,
                mmap,
            )
        }?;
        let t_props_values = unsafe {
            ChunkedArray::<StructArray>::load_from_dir(graph_dir, GraphPaths::NodeTProps, mmap)
        }?;
        temporal_props.push(TemporalProps::new(
            t_props_offsets,
            t_props_values,
            t_props_timestamps,
            (!t_props_secondary_index.is_empty()).then_some(t_props_secondary_index),
        ));
    }

    let const_props_values =
        unsafe { ChunkedArray::load_from_dir(graph_dir, GraphPaths::NodeConstProps, mmap) }?;
    let const_props = if const_props_values.is_empty() {
        None
    } else {
        Some(ConstProps::new(const_props_values))
    };

    Ok(Properties::new(const_props, temporal_props))
}
