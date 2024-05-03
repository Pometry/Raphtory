use std::{iter, ops::Range};

use arrow2::{
    array::StructArray,
    datatypes::{DataType, Field},
    types::{NativeType, Offset},
};
use rayon::prelude::*;

use crate::{
    arrow::{
        chunked_array::{
            array_ops::{ArrayOps, BaseArrayOps},
            chunked_array::ChunkedArray,
            col::ChunkedPrimitiveCol,
            utf8_col::StringCol,
            ChunkedArraySlice,
        },
        edge::Edge,
        timestamps::TimeStamps,
    },
    core::storage::timeindex::{TimeIndexIntoOps, TimeIndexOps},
    db::api::{storage::tprop_storage_ops::TPropOps, view::IntoDynBoxed},
    prelude::{Prop, TimeIndexEntry},
};

#[derive(Debug, Copy, Clone)]
pub struct TPropColumn<'a, A> {
    props: ChunkedArraySlice<'a, A>,
    timestamps: TimeStamps<'a, TimeIndexEntry>,
}

pub fn new_primitive_tprop_column<'a, T: NativeType + Into<Prop>>(
    timestamps: TimeStamps<'a, TimeIndexEntry>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, ChunkedPrimitiveCol<'a, T>>> {
    let props = props.into_primitive_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}

pub fn new_str_tprop_column<'a, I: Offset>(
    timestamps: TimeStamps<'a, TimeIndexEntry>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, StringCol<'a, I>>> {
    let props = props.into_utf8_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}

impl<'a, T: NativeType + Into<Prop>> TPropOps<'a> for TPropColumn<'a, ChunkedPrimitiveCol<'a, T>> {
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
        let (t, t_index) = self.timestamps.last_before(t)?;
        let v = self.props.get(t_index)?;
        Some((t, v.into()))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.timestamps
            .into_iter()
            .zip(self.props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let start = self.timestamps.position(&r.start);
        let end = self.timestamps.position(&r.end);
        self.timestamps
            .sliced(start..end)
            .into_iter()
            .zip(self.props.sliced(start..end).into_iter())
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let t_index = self.timestamps.position(ti);
        self.props.get(t_index).map(|v| v.into())
    }

    fn len(self) -> usize {
        self.props.par_iter().flatten().count()
    }

    fn is_empty(self) -> bool {
        self.props.par_iter().any(|v| v.is_some())
    }
}

impl<'a, I: Offset> TPropOps<'a> for TPropColumn<'a, StringCol<'a, I>> {
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
        let (t, t_index) = self.timestamps.last_before(t)?;
        let v = self.props.get(t_index)?;
        Some((t, v.into()))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.timestamps
            .into_iter()
            .zip(self.props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let start = self.timestamps.position(&r.start);
        let end = self.timestamps.position(&r.end);
        self.timestamps
            .sliced(start..end)
            .into_iter()
            .zip(self.props.sliced(start..end).into_iter())
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let t_index = self.timestamps.position(ti);
        self.props.get(t_index).map(|v| v.into())
    }

    fn len(self) -> usize {
        self.props.par_iter().flatten().count()
    }

    fn is_empty(self) -> bool {
        self.props.par_iter().any(|v| v.is_some())
    }
}

fn new_tprop_column<T: NativeType>(
    edge: Edge,
    id: usize,
) -> Option<TPropColumn<ChunkedPrimitiveCol<T>>>
where
    Prop: From<T>,
{
    let props = edge.prop_values::<T>(id)?;
    let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
    Some(TPropColumn { props, timestamps })
}

pub fn read_tprop_column(id: usize, field: Field, edge: Edge) -> Option<ArrowTProp> {
    match field.data_type() {
        DataType::Int64 => new_tprop_column::<i64>(edge, id).map(ArrowTProp::I64),
        DataType::Int32 => new_tprop_column::<i32>(edge, id).map(ArrowTProp::I32),
        DataType::UInt32 => new_tprop_column::<u32>(edge, id).map(ArrowTProp::U32),
        DataType::UInt64 => new_tprop_column::<u64>(edge, id).map(ArrowTProp::U64),
        DataType::Float32 => new_tprop_column::<f32>(edge, id).map(ArrowTProp::F32),
        DataType::Float64 => new_tprop_column::<f64>(edge, id).map(ArrowTProp::F64),
        DataType::Utf8 => {
            let props = edge.prop_str_values::<i32>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(ArrowTProp::Str32(TPropColumn { props, timestamps }))
        }
        DataType::LargeUtf8 => {
            let props = edge.prop_str_values::<i64>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(ArrowTProp::Str64(TPropColumn { props, timestamps }))
        }
        _ => todo!(),
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EmptyTProp();

impl<'a> TPropOps<'a> for EmptyTProp {
    fn last_before(self, _t: i64) -> Option<(TimeIndexEntry, Prop)> {
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

#[derive(Copy, Clone, Debug)]
pub enum ArrowTProp<'a> {
    Empty(EmptyTProp),
    Str64(TPropColumn<'a, StringCol<'a, i64>>),
    Str32(TPropColumn<'a, StringCol<'a, i32>>),
    I32(TPropColumn<'a, ChunkedPrimitiveCol<'a, i32>>),
    I64(TPropColumn<'a, ChunkedPrimitiveCol<'a, i64>>),
    U8(TPropColumn<'a, ChunkedPrimitiveCol<'a, u8>>),
    U16(TPropColumn<'a, ChunkedPrimitiveCol<'a, u16>>),
    U32(TPropColumn<'a, ChunkedPrimitiveCol<'a, u32>>),
    U64(TPropColumn<'a, ChunkedPrimitiveCol<'a, u64>>),
    F32(TPropColumn<'a, ChunkedPrimitiveCol<'a, f32>>),
    F64(TPropColumn<'a, ChunkedPrimitiveCol<'a, f64>>),
}

impl<'a> ArrowTProp<'a> {
    pub fn empty() -> Self {
        ArrowTProp::Empty(EmptyTProp())
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            ArrowTProp::Empty($pattern) => $result,
            ArrowTProp::Str64($pattern) => $result,
            ArrowTProp::Str32($pattern) => $result,
            ArrowTProp::I32($pattern) => $result,
            ArrowTProp::I64($pattern) => $result,
            ArrowTProp::U8($pattern) => $result,
            ArrowTProp::U16($pattern) => $result,
            ArrowTProp::U32($pattern) => $result,
            ArrowTProp::U64($pattern) => $result,
            ArrowTProp::F32($pattern) => $result,
            ArrowTProp::F64($pattern) => $result,
        }
    };
}

impl<'a> TPropOps<'a> for ArrowTProp<'a> {
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
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
