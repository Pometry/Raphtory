use crate::{
    arrow2::types::{NativeType, Offset},
    db::api::storage::graph::tprop_storage_ops::TPropOps,
    prelude::Prop,
};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use polars_arrow::datatypes::ArrowDataType;
use pometry_storage::{
    chunked_array::{
        bool_col::ChunkedBoolCol, col::ChunkedPrimitiveCol, utf8_col::StringCol,
        utf8_view_col::StringViewCol,
    },
    prelude::ArrayOps,
    tprops::{DiskTProp, EmptyTProp, TPropColumn},
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use std::{iter, ops::Range};

impl<'a> TPropOps<'a> for TPropColumn<'a, ChunkedBoolCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let scale = scale(self.data_type());
        let (props, timestamps) = self.into_inner();
        timestamps.into_iter().zip(props).filter_map(move |(t, v)| {
            v.zip(scale)
                .map(|(v, scale)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
        })
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let scale = scale(self.data_type());
        self.iter_window_inner(r).filter_map(move |(t, v)| {
            v.zip(scale)
                .map(|(v, scale)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
        })
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let (props, timestamps) = self.into_inner();
        timestamps
            .into_iter()
            .zip(props)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        self.iter_window_inner(r)
            .filter_map(|(t, v)| v.map(|v| (t, v.into())))
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

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter::empty()
    }

    fn iter_window(
        self,
        _r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter::empty()
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

#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator)]
pub enum DiskTPropVariants<
    Empty,
    Bool,
    Str64,
    Str32,
    Str,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
> {
    Empty(Empty),
    Bool(Bool),
    Str64(Str64),
    Str32(Str32),
    Str(Str),
    I32(I32),
    I64(I64),
    I128(I128),
    U8(U8),
    U16(U16),
    U32(U32),
    U64(U64),
    F32(F32),
    F64(F64),
}

macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            DiskTProp::Empty($pattern) => DiskTPropVariants::Empty($result),
            DiskTProp::Bool($pattern) => DiskTPropVariants::Bool($result),
            DiskTProp::Str64($pattern) => DiskTPropVariants::Str64($result),
            DiskTProp::Str32($pattern) => DiskTPropVariants::Str32($result),
            DiskTProp::Str($pattern) => DiskTPropVariants::Str($result),
            DiskTProp::I32($pattern) => DiskTPropVariants::I32($result),
            DiskTProp::I64($pattern) => DiskTPropVariants::I64($result),
            DiskTProp::I128($pattern) => DiskTPropVariants::I128($result),
            DiskTProp::U8($pattern) => DiskTPropVariants::U8($result),
            DiskTProp::U16($pattern) => DiskTPropVariants::U16($result),
            DiskTProp::U32($pattern) => DiskTPropVariants::U32($result),
            DiskTProp::U64($pattern) => DiskTPropVariants::U64($result),
            DiskTProp::F32($pattern) => DiskTPropVariants::F32($result),
            DiskTProp::F64($pattern) => DiskTPropVariants::F64($result),
        }
    };
}

impl<'a> TPropOps<'a> for DiskTProp<'a, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, v => v.last_before(t))
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all_iter!(self, v => v.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all_iter!(self, v => v.iter_window(r))
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, v => v.at(ti))
    }
}
