use crate::{
    arrow2::types::{NativeType, Offset},
    db::api::storage::graph::tprop_storage_ops::TPropOps,
    prelude::Prop,
};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use polars_arrow::datatypes::ArrowDataType;
use pometry_storage::{
    chunked_array::{
        bool_col::ChunkedBoolCol, col::ChunkedPrimitiveCol, utf8_col::StringCol,
        utf8_view_col::StringViewCol,
    },
    prelude::ArrayOps,
    tprops::{DiskTProp, EmptyTProp, TPropColumn},
};
use raphtory_api::{
    core::storage::timeindex::TimeIndexEntry,
    iter::{BoxedLDIter, IntoDynDBoxed},
};
use std::{iter, ops::Range};

impl<'a> TPropOps<'a> for TPropColumn<'a, ChunkedBoolCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
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
}

fn scale(dt: Option<&ArrowDataType>) -> Option<u32> {
    if let ArrowDataType::Decimal(_, scale) = dt? {
        Some((*scale).try_into().unwrap())
    } else {
        panic!("Expected decimal type")
    }
}

impl<'a> TPropOps<'a> for TPropColumn<'a, ChunkedPrimitiveCol<'a, i128>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        let scale = scale(self.data_type())?;
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(move |(t, v)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        let scale = scale(self.data_type());
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(move |(t, v)| {
                v.zip(scale)
                    .map(|(v, scale)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
            })
            .into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        let scale = scale(self.data_type());
        self.iter_window_inner(r)
            .filter_map(move |(t, v)| {
                v.zip(scale)
                    .map(|(v, scale)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
            })
            .into_dyn_dboxed()
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        let scale = scale(self.data_type())?;
        let (props, timestamps) = self.into_inner();
        let t_index = timestamps.position(ti);
        if t_index < timestamps.len() {
            timestamps.get(t_index).eq(ti).then(|| {
                props
                    .get(t_index)
                    .map(|v| BigDecimal::new(BigInt::from(v), scale.into()).into())
            })?
        } else {
            None
        }
    }
}

impl<'a, T: NativeType + Into<Prop>> TPropOps<'a>
    for TPropColumn<'a, ChunkedPrimitiveCol<'a, T>, TimeIndexEntry>
{
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
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
}

impl<'a, I: Offset> TPropOps<'a> for TPropColumn<'a, StringCol<'a, I>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
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
}

impl<'a> TPropOps<'a> for TPropColumn<'a, StringViewCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
            .into_dyn_dboxed()
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
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
}

impl<'a> TPropOps<'a> for EmptyTProp {
    fn last_before(&self, _t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        None
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        iter::empty().into_dyn_dboxed()
    }

    fn iter_window(&self, _r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        iter::empty().into_dyn_dboxed()
    }

    fn at(&self, _ti: &TimeIndexEntry) -> Option<Prop> {
        None
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            DiskTProp::Empty($pattern) => $result,
            DiskTProp::Bool($pattern) => $result,
            DiskTProp::Str64($pattern) => $result,
            DiskTProp::Str32($pattern) => $result,
            DiskTProp::Str($pattern) => $result,
            DiskTProp::I32($pattern) => $result,
            DiskTProp::I64($pattern) => $result,
            DiskTProp::I128($pattern) => $result,
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

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        for_all!(self, v => v.iter())
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        for_all!(self, v => v.iter_window(r))
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, v => v.at(ti))
    }
}
