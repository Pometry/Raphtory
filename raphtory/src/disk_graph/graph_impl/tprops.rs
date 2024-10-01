use crate::{
    arrow2::{
        datatypes::{ArrowDataType as DataType, Field},
        types::{NativeType, Offset},
    },
    core::storage::timeindex::TimeIndexIntoOps,
    db::api::{storage::graph::tprop_storage_ops::TPropOps, view::IntoDynBoxed},
    prelude::Prop,
};
use polars_arrow::array::Array;
use pometry_storage::{
    chunked_array::{bool_col::ChunkedBoolCol, col::ChunkedPrimitiveCol, utf8_col::StringCol},
    edge::Edge,
    prelude::{ArrayOps, BaseArrayOps},
    timestamps::TimeStamps,
    tprops::{DiskTProp, EmptyTProp, TPropColumn},
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use rayon::prelude::*;
use std::{iter, ops::Range};

impl<'a> TPropOps<'a> for TPropColumn<'a, ChunkedBoolCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        let (t, t_index) = timestamps.last_before(t)?;
        let v = props.get(t_index)?;
        Some((t, v.into()))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        let start = timestamps.position(&r.start);
        let end = timestamps.position(&r.end);
        timestamps
            .sliced(start..end)
            .into_iter()
            .zip(props.sliced(start..end))
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let (props, timestamps) = self.into_inner();
        let t_index = timestamps.position(ti);
        if t_index < timestamps.len() {
            timestamps
                .get(t_index)
                .eq(ti)
                .then(|| props.get(t_index).map(|v| v.into()))?
        } else {
            None
        }
    }

    fn len(self) -> usize {
        let (props, _) = self.into_inner();
        props
            .iter_chunks()
            .map(|chunk| chunk.len() - chunk.null_count())
            .sum()
    }
}

impl<'a, T: NativeType + Into<Prop>> TPropOps<'a>
    for TPropColumn<'a, ChunkedPrimitiveCol<'a, T>, TimeIndexEntry>
{
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        let (t, t_index) = timestamps.last_before(t)?;
        let v = props.get(t_index)?;
        Some((t, v.into()))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        let start = timestamps.position(&r.start);
        let end = timestamps.position(&r.end);
        timestamps
            .sliced(start..end)
            .into_iter()
            .zip(props.sliced(start..end))
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let (props, timestamps) = self.into_inner();
        let t_index = timestamps.position(ti);
        if t_index < timestamps.len() {
            timestamps
                .get(t_index)
                .eq(ti)
                .then(|| props.get(t_index).map(|v| v.into()))?
        } else {
            None
        }
    }

    fn len(self) -> usize {
        let (props, _) = self.into_inner();
        props
            .iter_chunks()
            .map(|chunk| chunk.len() - chunk.null_count())
            .sum()
    }

    fn is_empty(self) -> bool {
        let (props, _) = self.into_inner();
        props.par_iter().any(|v| v.is_some())
    }
}

impl<'a, I: Offset> TPropOps<'a> for TPropColumn<'a, StringCol<'a, I>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        let (t, t_index) = timestamps.last_before(t)?;
        let v = props.get(t_index)?;
        Some((t, v.into()))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        let start = timestamps.position(&r.start);
        let end = timestamps.position(&r.end);
        timestamps
            .sliced(start..end)
            .into_iter()
            .zip(props.sliced(start..end))
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let (props, timestamps) = self.into_inner();
        let t_index = timestamps.position(ti);
        if t_index < timestamps.len() {
            timestamps
                .get(t_index)
                .eq(ti)
                .then(|| props.get(t_index).map(|v| v.into()))?
        } else {
            None
        }
    }

    fn len(self) -> usize {
        let (props, _) = self.into_inner();
        props
            .iter_chunks()
            .map(|chunk| chunk.len() - chunk.null_count())
            .sum()
    }

    fn is_empty(self) -> bool {
        let (props, _) = self.into_inner();
        props.par_iter().any(|v| v.is_some())
    }
}

fn new_tprop_column<T: NativeType>(
    edge: Edge,
    id: usize,
) -> Option<TPropColumn<ChunkedPrimitiveCol<T>, TimeIndexEntry>>
where
    Prop: From<T>,
{
    let props = edge.prop_values::<T>(id)?;
    let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
    Some(TPropColumn::new(props, timestamps))
}

pub fn read_tprop_column(id: usize, field: Field, edge: Edge) -> Option<DiskTProp<TimeIndexEntry>> {
    match field.data_type() {
        DataType::Boolean => {
            let props = edge.prop_bool_values(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(DiskTProp::Bool(TPropColumn::new(props, timestamps)))
        }
        DataType::Int64 => new_tprop_column::<i64>(edge, id).map(DiskTProp::I64),
        DataType::Int32 => new_tprop_column::<i32>(edge, id).map(DiskTProp::I32),
        DataType::UInt32 => new_tprop_column::<u32>(edge, id).map(DiskTProp::U32),
        DataType::UInt64 => new_tprop_column::<u64>(edge, id).map(DiskTProp::U64),
        DataType::Float32 => new_tprop_column::<f32>(edge, id).map(DiskTProp::F32),
        DataType::Float64 => new_tprop_column::<f64>(edge, id).map(DiskTProp::F64),
        DataType::Utf8 => {
            let props = edge.prop_str_values::<i32>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(DiskTProp::Str32(TPropColumn::new(props, timestamps)))
        }
        DataType::LargeUtf8 => {
            let props = edge.prop_str_values::<i64>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(DiskTProp::Str64(TPropColumn::new(props, timestamps)))
        }
        _ => todo!(),
    }
}

impl<'a> TPropOps<'a> for EmptyTProp {
    fn last_before(&self, _t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        None
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        iter::empty()
    }

    fn iter_window(
        self,
        _r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        iter::empty()
    }

    fn iter_window_te(
        self,
        _r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        iter::empty()
    }

    fn at(self, _ti: &TimeIndexEntry) -> Option<Prop> {
        None
    }

    fn len(self) -> usize {
        0
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            DiskTProp::Empty($pattern) => $result,
            DiskTProp::Bool($pattern) => $result,
            DiskTProp::Str64($pattern) => $result,
            DiskTProp::Str32($pattern) => $result,
            DiskTProp::I32($pattern) => $result,
            DiskTProp::I64($pattern) => $result,
            DiskTProp::U8($pattern) => $result,
            DiskTProp::U16($pattern) => $result,
            DiskTProp::U32($pattern) => $result,
            DiskTProp::U64($pattern) => $result,
            DiskTProp::F32($pattern) => $result,
            DiskTProp::F64($pattern) => $result,
        }
    };
}

impl<'a> TPropOps<'a> for DiskTProp<'a, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, v => v.last_before(t))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all!(self, v => v.iter().into_dyn_boxed())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all!(self, v => v.iter_window(r).into_dyn_boxed())
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, v => v.at(ti))
    }

    fn len(self) -> usize {
        for_all!(self, v => v.len())
    }
}
