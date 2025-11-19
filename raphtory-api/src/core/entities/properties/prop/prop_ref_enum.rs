use crate::core::{
    entities::properties::prop::{Prop, SerdeList, SerdeMap},
    storage::arc_str::ArcStr,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use num_traits::ToPrimitive;
use rustc_hash::FxHashMap;
use serde::Serialize;
use std::{borrow::Cow, sync::Arc};

use crate::core::entities::properties::prop::{ArrowRow, PropArray};

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PropMapRef<'a> {
    Mem(&'a Arc<FxHashMap<ArcStr, Prop>>),
    Arrow(ArrowRow<'a>),
}

impl<'a> PropMapRef<'a> {
    pub fn into_prop(self) -> Option<Prop> {
        match self {
            PropMapRef::Mem(map) => Some(Prop::Map(map.clone())),
            PropMapRef::Arrow(row) => row.into_prop(),
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

impl<'a> PropRef<'a> {
    pub fn as_str(&self) -> Option<&'a str> {
        if let PropRef::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

impl<'a> Serialize for PropMapRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PropMapRef::Mem(map) => SerdeMap(map).serialize(serializer),
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
            PropRef::List(lst) => SerdeList(lst).serialize(serializer),
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
