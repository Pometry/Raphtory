use num_traits::ToPrimitive;
use serde::Serialize;
use std::sync::Arc;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use rustc_hash::FxHashMap;

#[cfg(feature = "arrow")]
use crate::core::entities::properties::prop::PropArray;
use crate::core::{
    entities::properties::prop::{ArrowRow, Prop, SedeList, SerdeMap},
    storage::arc_str::ArcStr,
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PropRef<'a> {
    Str(&'a str),
    Num(PropNum),
    Bool(bool),
    List(&'a Arc<Vec<Prop>>),
    Map(PropMapRef<'a>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    #[cfg(feature = "arrow")]
    Array(&'a PropArray),
    Decimal {
        num: i128,
        scale: i8,
    },
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

impl From<u8> for PropRef<'_> {
    fn from(n: u8) -> Self {
        PropRef::Num(PropNum::U8(n))
    }
}

impl From<u16> for PropRef<'_> {
    fn from(n: u16) -> Self {
        PropRef::Num(PropNum::U16(n))
    }
}

impl From<i32> for PropRef<'_> {
    fn from(n: i32) -> Self {
        PropRef::Num(PropNum::I32(n))
    }
}

impl From<i64> for PropRef<'_> {
    fn from(n: i64) -> Self {
        PropRef::Num(PropNum::I64(n))
    }
}

impl From<u32> for PropRef<'_> {
    fn from(n: u32) -> Self {
        PropRef::Num(PropNum::U32(n))
    }
}

impl From<u64> for PropRef<'_> {
    fn from(n: u64) -> Self {
        PropRef::Num(PropNum::U64(n))
    }
}

impl From<f32> for PropRef<'_> {
    fn from(n: f32) -> Self {
        PropRef::Num(PropNum::F32(n))
    }
}

impl From<f64> for PropRef<'_> {
    fn from(n: f64) -> Self {
        PropRef::Num(PropNum::F64(n))
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

#[derive(Debug, PartialEq, Clone, Copy)]
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
            PropRef::List(lst) => SedeList(lst).serialize(serializer),
            PropRef::Map(map_ref) => map_ref.serialize(serializer),
            PropRef::NDTime(dt) => serializer.serialize_i64(dt.and_utc().timestamp_millis()),
            PropRef::DTime(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            #[cfg(feature = "arrow")]
            PropRef::Array(arr) => arr.serialize(serializer),
            PropRef::Decimal { num, scale } => {
                let decimal = BigDecimal::new((*num).into(), (*scale).into());
                decimal.serialize(serializer)
            }
        }
    }
}
