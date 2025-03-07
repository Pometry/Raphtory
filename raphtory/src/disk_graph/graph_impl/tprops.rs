use crate::{
    arrow2::types::{NativeType, Offset},
    db::api::storage::graph::tprop_storage_ops::TPropOps,
    prelude::Prop,
};
use pometry_storage::{
    chunked_array::{bool_col::ChunkedBoolCol, col::ChunkedPrimitiveCol, utf8_col::StringCol},
    prelude::ArrayOps,
    tprops::{DiskTProp, EmptyTProp, TPropColumn},
};
use raphtory_api::{core::storage::timeindex::TimeIndexEntry, iter::IntoDynDBoxed};
use std::{iter, ops::Range};

impl<'a> TPropOps<'a> for TPropColumn<'a, ChunkedBoolCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.iter_window_inner(r)
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.iter_window_inner(r)
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
}

impl<'a, I: Offset> TPropOps<'a> for TPropColumn<'a, StringCol<'a, I>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.iter_window_inner(r)
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
}

impl<'a> TPropOps<'a> for EmptyTProp {
    fn last_before(&self, _t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        None
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        iter::empty()
    }

    fn iter_window(
        self,
        _r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all!(self, v => v.iter().into_dyn_dboxed())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all!(self, v => v.iter_window(r).into_dyn_dboxed())
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, v => v.at(ti))
    }
}
