use crate::{
    arrow2::types::{NativeType, Offset},
    core::PropUnwrap,
    db::api::storage::graph::tprop_storage_ops::TPropOps,
    prelude::Prop,
};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use chrono::DateTime;
use either::Either;
use polars_arrow::datatypes::ArrowDataType;
use pometry_storage::{
    chunked_array::{
        bool_col::ChunkedBoolCol, col::ChunkedPrimitiveCol, utf8_col::StringCol,
        utf8_view_col::StringViewCol,
    },
    prelude::{ArrayOps, Chunked},
    tprops::{DTTPropColumn, DiskTProp, EmptyTProp, TPropColumn},
};
use raphtory_api::{core::storage::timeindex::TimeIndexEntry, iter::IntoDynBoxed};
use std::{iter, ops::Range};

impl<'a> TPropOps<'a> for TPropColumn<'a, ChunkedBoolCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner(self, range)
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner_rev(self, range)
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

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let scale = scale(self.data_type());
        match range {
            Some(w) => Either::Left(self.iter_window_inner(w).filter_map(move |(t, v)| {
                v.zip(scale)
                    .map(|(v, scale)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
            })),
            None => {
                let (props, timestamps) = self.into_inner();
                Either::Right(timestamps.into_iter().zip(props).filter_map(move |(t, v)| {
                    v.zip(scale).map(|(v, scale)| {
                        (t, BigDecimal::new(BigInt::from(v), scale.into()).into())
                    })
                }))
            }
        }
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let scale = scale(self.data_type());
        match range {
            Some(w) => Either::Left(self.iter_window_inner(w).rev().filter_map(move |(t, v)| {
                v.zip(scale)
                    .map(|(v, scale)| (t, BigDecimal::new(BigInt::from(v), scale.into()).into()))
            })),
            None => {
                let (props, timestamps) = self.into_inner();
                Either::Right(
                    timestamps
                        .into_iter()
                        .zip(props)
                        .rev()
                        .filter_map(move |(t, v)| {
                            v.zip(scale).map(|(v, scale)| {
                                (t, BigDecimal::new(BigInt::from(v), scale.into()).into())
                            })
                        }),
                )
            }
        }
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
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

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner(self, range)
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner_rev(self, range)
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

fn as_d_type_prop(v: Prop, zone: Option<&str>) -> Prop {
    let v = v.into_i64().unwrap();
    match zone {
        Some(_) => Prop::DTime(DateTime::from_timestamp_millis(v).unwrap()),
        None => Prop::NDTime(DateTime::from_timestamp_millis(v).unwrap().naive_utc()),
    }
}

impl<'a> TPropOps<'a> for DTTPropColumn<'a, ChunkedPrimitiveCol<'a, i64>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.col()
            .last_before(t)
            .map(|(t, v)| (t, as_d_type_prop(v, self.zone())))
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let DTTPropColumn { col, zone } = self;
        let iter = col.iter_inner(range);
        iter.map(move |(t, v)| (t, as_d_type_prop(v, zone)))
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let DTTPropColumn { col, zone } = self;
        let iter = col.iter_inner_rev(range);
        iter.map(move |(t, v)| (t, as_d_type_prop(v, zone)))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let DTTPropColumn { col, zone } = self;
        col.at(ti).map(|v| as_d_type_prop(v, zone))
    }
}

impl<'a, I: Offset> TPropOps<'a> for TPropColumn<'a, StringCol<'a, I>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner(self, range)
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner_rev(self, range)
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

impl<'a> TPropOps<'a> for TPropColumn<'a, StringViewCol<'a>, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t)
            .filter_map(|(t, v)| v.map(|v| (t, v)))
            .next_back()
            .map(|(t, v)| (t, v.into()))
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner(self, range)
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter_inner_rev(self, range)
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

    fn iter_inner(
        self,
        _range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        iter::empty()
    }

    fn iter_inner_rev(
        self,
        _range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
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
            DiskTProp::Str($pattern) => $result,
            DiskTProp::I32($pattern) => $result,
            DiskTProp::I64($pattern) => $result,
            DiskTProp::Decimal($pattern) => $result,
            DiskTProp::U8($pattern) => $result,
            DiskTProp::U16($pattern) => $result,
            DiskTProp::U32($pattern) => $result,
            DiskTProp::U64($pattern) => $result,
            DiskTProp::F32($pattern) => $result,
            DiskTProp::F64($pattern) => $result,
            DiskTProp::DateTime($pattern) => $result,
            DiskTProp::NDateTime($pattern) => $result,
        }
    };
}

impl<'a> TPropOps<'a> for DiskTProp<'a, TimeIndexEntry> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, v => v.last_before(t))
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all!(self, v => v.iter_inner(range).into_dyn_boxed())
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all!(self, v => v.iter_inner_rev(range).into_dyn_boxed())
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, v => v.at(ti))
    }
}

trait CheatMap<A>: Send {
    fn map<B>(self, mapper: impl Fn(A) -> B) -> Option<B>;
}

impl<T: Send + Sync> CheatMap<T> for Option<T> {
    fn map<B>(self, mapper: impl Fn(T) -> B) -> Option<B> {
        self.map(|v| mapper(v))
    }
}

fn iter_inner<
    'a,
    P: Into<Prop>,
    A: std::fmt::Debug + ArrayOps<'a> + Chunked<Chunk = <A as ArrayOps<'a>>::Chunk>,
>(
    col: TPropColumn<'a, A, TimeIndexEntry>,
    range: Option<Range<TimeIndexEntry>>,
) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a
where
    A::IntoValue: CheatMap<P>,
{
    match range {
        Some(w) => Either::Left(
            col.iter_window_inner(w)
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        ),
        None => {
            let (props, timestamps) = col.into_inner();
            Either::Right(
                timestamps
                    .into_iter()
                    .zip(props)
                    .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
            )
        }
    }
}

fn iter_inner_rev<
    'a,
    P: Into<Prop>,
    A: ArrayOps<'a> + Chunked<Chunk = <A as ArrayOps<'a>>::Chunk>,
>(
    col: TPropColumn<'a, A, TimeIndexEntry>,
    range: Option<Range<TimeIndexEntry>>,
) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a
where
    A::IntoValue: CheatMap<P>,
{
    match range {
        Some(w) => Either::Left(
            col.iter_window_inner(w)
                .rev()
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        ),
        None => {
            let (props, timestamps) = col.into_inner();
            Either::Right(
                timestamps
                    .into_iter()
                    .zip(props)
                    .rev()
                    .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
            )
        }
    }
}
