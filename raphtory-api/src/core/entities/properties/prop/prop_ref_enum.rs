use crate::core::{
    entities::properties::prop::{
        prop_col::{MapCol, PropCol},
        validate_bd, ArrowRow, InvalidBigDecimal, Prop, PropArray, PropType, PropUnwrap,
        SerdeArrowList, SerdeArrowMap,
    },
    storage::arc_str::ArcStr,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use num_traits::ToPrimitive;
use rustc_hash::FxHashMap;
use serde::Serialize;
use std::{borrow::Cow, sync::Arc};

#[derive(Debug, Clone)]
pub enum PropRef<'a> {
    Str(&'a str),
    Num(PropNum),
    Bool(bool),
    List(Cow<'a, PropArray>),
    Map(PropMapRef<'a>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Decimal { num: i128, scale: i8 },
}

impl PropRef<'_> {
    pub fn as_map_ref(&self) -> Option<PropMapRef<'_>> {
        if let PropRef::Map(m) = self {
            Some(*m)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PropMapRef<'a> {
    Mem(&'a Arc<FxHashMap<ArcStr, Prop>>),
    PropCol { map: &'a MapCol, i: usize },
    Arrow(ArrowRow<'a>),
}

impl<'a> PropMapRef<'a> {
    pub fn into_prop(self) -> Option<Prop> {
        match self {
            PropMapRef::Mem(map) => Some(Prop::Map(map.clone())),
            PropMapRef::PropCol { map, i } => map.get(i),
            PropMapRef::Arrow(row) => row.into_prop(),
        }
    }

    pub fn as_map(&self) -> Option<&'a Arc<FxHashMap<ArcStr, Prop>>> {
        if let PropMapRef::Mem(m) = self {
            Some(*m)
        } else {
            None
        }
    }

    pub fn as_mem(&self) -> Arc<FxHashMap<ArcStr, Prop>> {
        match self {
            PropMapRef::Mem(m) => (*m).clone(),
            PropMapRef::PropCol { map, i } => map.get(*i).unwrap_map(),
            PropMapRef::Arrow(row) => row.into_prop().unwrap_map(),
        }
    }
}

impl<T: Into<PropNum>> From<T> for PropRef<'static> {
    fn from(n: T) -> Self {
        PropRef::Num(n.into())
    }
}

impl<'a> From<bool> for PropRef<'a> {
    fn from(b: bool) -> Self {
        PropRef::Bool(b)
    }
}

impl<'a> From<&'a str> for PropRef<'a> {
    fn from(s: &'a str) -> Self {
        PropRef::Str(s)
    }
}

impl From<NaiveDateTime> for PropRef<'_> {
    fn from(dt: NaiveDateTime) -> Self {
        PropRef::NDTime(dt)
    }
}

impl From<DateTime<Utc>> for PropRef<'_> {
    fn from(dt: DateTime<Utc>) -> Self {
        PropRef::DTime(dt)
    }
}

impl<'a> From<&'a BigDecimal> for PropRef<'a> {
    fn from(decimal: &'a BigDecimal) -> Self {
        let (num, scale) = decimal.as_bigint_and_exponent();
        let num = num.to_i128().unwrap_or_else(|| {
            panic!(
                "BigDecimal value {} is out of range for i128 representation",
                decimal
            )
        });
        PropRef::Decimal {
            num,
            scale: scale as i8,
        }
    }
}

impl<'a> From<ArrowRow<'a>> for PropRef<'a> {
    fn from(row: ArrowRow<'a>) -> Self {
        PropRef::Map(PropMapRef::Arrow(row))
    }
}

impl<'a> From<&'a Arc<FxHashMap<ArcStr, Prop>>> for PropRef<'a> {
    fn from(map: &'a Arc<FxHashMap<ArcStr, Prop>>) -> Self {
        PropRef::Map(PropMapRef::Mem(map))
    }
}

#[derive(Debug, PartialEq, Clone, Copy, derive_more::From)]
pub enum PropNum {
    U8(u8),
    U16(u16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

/// A trait for types that can be cheaply viewed as a [`PropRef`].
pub trait AsPropRef {
    fn as_prop_ref(&self) -> PropRef<'_>;
}

impl<'a> AsPropRef for PropRef<'a> {
    #[inline]
    fn as_prop_ref(&self) -> PropRef<'_> {
        self.clone()
    }
}

impl AsPropRef for Prop {
    fn as_prop_ref(&self) -> PropRef<'_> {
        match self {
            Prop::Str(s) => PropRef::Str(s),
            Prop::U8(v) => PropRef::Num(PropNum::U8(*v)),
            Prop::U16(v) => PropRef::Num(PropNum::U16(*v)),
            Prop::I32(v) => PropRef::Num(PropNum::I32(*v)),
            Prop::I64(v) => PropRef::Num(PropNum::I64(*v)),
            Prop::U32(v) => PropRef::Num(PropNum::U32(*v)),
            Prop::U64(v) => PropRef::Num(PropNum::U64(*v)),
            Prop::F32(v) => PropRef::Num(PropNum::F32(*v)),
            Prop::F64(v) => PropRef::Num(PropNum::F64(*v)),
            Prop::Bool(b) => PropRef::Bool(*b),
            Prop::List(lst) => PropRef::List(std::borrow::Cow::Borrowed(lst)),
            Prop::Map(map) => PropRef::Map(PropMapRef::Mem(map)),
            Prop::NDTime(dt) => PropRef::NDTime(*dt),
            Prop::DTime(dt) => PropRef::DTime(*dt),
            Prop::Decimal(bd) => PropRef::from(bd),
        }
    }
}

impl<'a> PropRef<'a> {
    pub fn as_str(&self) -> Option<&'a str> {
        if let PropRef::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn try_from_bd(bd: BigDecimal) -> Result<Self, InvalidBigDecimal> {
        validate_bd(&bd)?;
        let (num, scale) = bd.as_bigint_and_exponent();
        let num = num.to_i128().unwrap();
        Ok(PropRef::Decimal {
            num,
            scale: scale as i8,
        })
    }
}

impl<'a> Serialize for PropMapRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PropMapRef::Mem(map) => SerdeArrowMap(map).serialize(serializer),
            PropMapRef::PropCol { map, i } => match map.get_ref(*i) {
                Some(prop) => prop.serialize(serializer),
                None => serializer.serialize_none(),
            },
            PropMapRef::Arrow(row) => row.serialize(serializer),
        }
    }
}

impl<'a> Serialize for PropRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PropRef::Str(s) => serializer.serialize_str(s),
            PropRef::Num(n) => match n {
                PropNum::U8(v) => serializer.serialize_u8(*v),
                PropNum::U16(v) => serializer.serialize_u16(*v),
                PropNum::I32(v) => serializer.serialize_i32(*v),
                PropNum::I64(v) => serializer.serialize_i64(*v),
                PropNum::U32(v) => serializer.serialize_u32(*v),
                PropNum::U64(v) => serializer.serialize_u64(*v),
                PropNum::F32(v) => serializer.serialize_f32(*v),
                PropNum::F64(v) => serializer.serialize_f64(*v),
            },
            PropRef::Bool(b) => serializer.serialize_bool(*b),
            PropRef::List(lst) => SerdeArrowList(lst).serialize(serializer),
            PropRef::Map(map_ref) => map_ref.serialize(serializer),
            PropRef::NDTime(dt) => serializer.serialize_i64(dt.and_utc().timestamp_millis()),
            PropRef::DTime(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            PropRef::Decimal { num, scale } => {
                let decimal = BigDecimal::new((*num).into(), (*scale).into());
                decimal.serialize(serializer)
            }
        }
    }
}
