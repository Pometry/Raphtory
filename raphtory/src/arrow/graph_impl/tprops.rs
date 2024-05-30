use crate::{
    arrow2::{
        datatypes::{ArrowDataType as DataType, Field},
        types::{NativeType, Offset},
    },
    core::storage::timeindex::TimeIndexIntoOps,
    db::api::{storage::tprop_storage_ops::TPropOps, view::IntoDynBoxed},
    prelude::Prop,
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use raphtory_arrow::{
    chunked_array::{col::ChunkedPrimitiveCol, utf8_col::StringCol},
    edge::Edge,
    prelude::{ArrayOps, BaseArrayOps},
    timestamps::TimeStamps,
    tprops::{ArrowTProp, EmptyTProp, TPropColumn},
};
use rayon::prelude::*;
use std::{iter, ops::Range};

impl<'a, T: NativeType + Into<Prop>> TPropOps<'a>
    for TPropColumn<'a, ChunkedPrimitiveCol<'a, T>, TimeIndexEntry>
{
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
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
        props.get(t_index).map(|v| v.into())
    }

    fn len(self) -> usize {
        let (props, _) = self.into_inner();
        props.par_iter().flatten().count()
    }

    fn is_empty(self) -> bool {
        let (props, _) = self.into_inner();
        props.par_iter().any(|v| v.is_some())
    }
}

impl<'a, I: Offset> TPropOps<'a> for TPropColumn<'a, StringCol<'a, I>, TimeIndexEntry> {
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
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
        props.get(t_index).map(|v| v.into())
    }

    fn len(self) -> usize {
        let (props, _) = self.into_inner();
        props.par_iter().flatten().count()
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

pub fn read_tprop_column(
    id: usize,
    field: Field,
    edge: Edge,
) -> Option<ArrowTProp<TimeIndexEntry>> {
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
            Some(ArrowTProp::Str32(TPropColumn::new(props, timestamps)))
        }
        DataType::LargeUtf8 => {
            let props = edge.prop_str_values::<i64>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(ArrowTProp::Str64(TPropColumn::new(props, timestamps)))
        }
        _ => todo!(),
    }
}

// #[derive(Copy, Clone, Debug)]
// pub struct EmptyTProp();

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

impl<'a> TPropOps<'a> for ArrowTProp<'a, TimeIndexEntry> {
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
