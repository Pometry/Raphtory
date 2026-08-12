use crate::core::{
    entities::{
        properties::prop::{
            prop_array::*, prop_ref_enum::PropRef, ArrowRow, PropNum, PropType, PropUnwrap,
        },
        GidRef,
    },
    storage::arc_str::ArcStr,
};
use arrow_array::{
    cast::AsArray,
    types::{
        Date32Type, Date64Type, Decimal128Type, DecimalType, Float32Type, Float64Type, Int32Type,
        Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    Array, ArrayRef, LargeListArray, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, TimeUnit, DECIMAL128_MAX_PRECISION};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use chrono::{DateTime, NaiveDateTime, Utc};
use indexmap::IndexMap;
use itertools::Itertools;
use num_traits::{Bounded, FromPrimitive, ToPrimitive, Zero};
use rustc_hash::{FxBuildHasher, FxHashMap};
use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Deserialize, Serialize, Serializer,
};
use serde_arrow::ArrayBuilder;
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
    hash::{DefaultHasher, Hash, Hasher},
    num::Wrapping,
    sync::Arc,
};
use thiserror::Error;

// Equivalent to parquet decimal(38, 0).
pub const DECIMAL_MAX: i128 = 99999999999999999999999999999999999999i128;

/// Insertion-ordered map used for `Prop::Map` values, so map properties keep a
/// deterministic key order through serialization round-trips.
pub type PropMap = IndexMap<ArcStr, Prop, FxBuildHasher>;

#[derive(Error, Debug)]
#[error("Decimal {0} too large.")]
pub struct InvalidBigDecimal(BigDecimal);

#[derive(Debug, PartialEq, Clone, Serialize)]
#[serde(transparent)]
pub struct PropUntagged(#[serde(with = "PropUntaggedDef")] pub Prop);

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
#[serde(remote = "Prop")]
enum PropUntaggedDef {
    Str(ArcStr),
    U8(u8),
    U16(u16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F64(f64),
    F32(f32),
    Bool(bool),
    List(PropArray),
    Map(Arc<PropMap>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Decimal(BigDecimal),
}

impl From<Prop> for PropUntagged {
    fn from(p: Prop) -> Self {
        PropUntagged(p)
    }
}

impl From<PropUntagged> for Prop {
    fn from(p: PropUntagged) -> Self {
        p.0
    }
}

impl<'de> Deserialize<'de> for PropUntagged {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum PropUntaggedHelper {
            Bool(bool),
            U8(u8),
            U16(u16),
            U32(u32),
            U64(u64),
            I32(i32),
            I64(i64),
            F64(f64),
            F32(f32),
            Str(ArcStr),
            List(Vec<PropUntagged>), // recursively uses PropUntagged
            Map(IndexMap<ArcStr, PropUntagged, FxBuildHasher>), // recursively uses PropUntagged
            NDTime(NaiveDateTime),
            DTime(DateTime<Utc>),
            Decimal(BigDecimal),
        }

        let helper = PropUntaggedHelper::deserialize(deserializer)?;
        let prop = match helper {
            PropUntaggedHelper::Bool(v) => Prop::Bool(v),
            PropUntaggedHelper::U8(v) => Prop::U8(v),
            PropUntaggedHelper::U16(v) => Prop::U16(v),
            PropUntaggedHelper::U32(v) => Prop::U32(v),
            PropUntaggedHelper::U64(v) => Prop::U64(v),
            PropUntaggedHelper::I32(v) => Prop::I32(v),
            PropUntaggedHelper::I64(v) => Prop::I64(v),
            PropUntaggedHelper::F64(v) => Prop::F64(v),
            PropUntaggedHelper::F32(v) => Prop::F32(v),
            PropUntaggedHelper::Str(v) => Prop::Str(v),
            PropUntaggedHelper::List(v) => Prop::list(v),
            PropUntaggedHelper::Map(v) => {
                Prop::Map(Arc::new(v.into_iter().map(|(k, p)| (k, p.0)).collect()))
            }
            PropUntaggedHelper::NDTime(v) => Prop::NDTime(v),
            PropUntaggedHelper::DTime(v) => Prop::DTime(v),
            PropUntaggedHelper::Decimal(v) => Prop::Decimal(v),
        };
        Ok(PropUntagged(prop))
    }
}

impl PartialEq<Prop> for PropUntagged {
    fn eq(&self, other: &Prop) -> bool {
        self.0
            .clone()
            .try_cast(other.dtype())
            .is_some_and(|p| p == *other)
    }
}

/// Denotes the types of properties allowed to be stored in the graph.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, derive_more::From)]
pub enum Prop {
    Str(ArcStr),
    U8(u8),
    U16(u16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F64(f64),
    F32(f32),
    Bool(bool),
    List(PropArray),
    Map(Arc<PropMap>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Decimal(BigDecimal),
}

impl From<GidRef<'_>> for Prop {
    fn from(value: GidRef<'_>) -> Self {
        match value {
            GidRef::U64(n) => Prop::U64(n),
            GidRef::Str(s) => Prop::str(s),
        }
    }
}

impl<'a> From<PropRef<'a>> for Prop {
    fn from(value: PropRef<'a>) -> Self {
        match value {
            PropRef::Str(s) => Prop::Str(s.into()),
            PropRef::Num(n) => match n {
                PropNum::U8(u) => Prop::U8(u),
                PropNum::U16(u) => Prop::U16(u),
                PropNum::I32(i) => Prop::I32(i),
                PropNum::I64(i) => Prop::I64(i),
                PropNum::U32(u) => Prop::U32(u),
                PropNum::U64(u) => Prop::U64(u),
                PropNum::F32(f) => Prop::F32(f),
                PropNum::F64(f) => Prop::F64(f),
            },
            PropRef::Bool(b) => Prop::Bool(b),
            PropRef::List(v) => Prop::List(v.as_ref().clone()),
            PropRef::Map(m) => m
                .into_prop()
                .unwrap_or_else(|| Prop::Map(Arc::new(Default::default()))),
            PropRef::NDTime(dt) => Prop::NDTime(dt),
            PropRef::DTime(dt) => Prop::DTime(dt),
            PropRef::Decimal { num, scale } => {
                Prop::Decimal(BigDecimal::from_bigint(num.into(), scale as i64))
            }
        }
    }
}

impl Hash for Prop {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Prop::Str(s) => s.hash(state),
            Prop::U8(u) => u.hash(state),
            Prop::U16(u) => u.hash(state),
            Prop::I32(i) => i.hash(state),
            Prop::I64(i) => i.hash(state),
            Prop::U32(u) => u.hash(state),
            Prop::U64(u) => u.hash(state),
            Prop::F32(f) => {
                let bits = f.to_bits();
                bits.hash(state);
            }
            Prop::F64(f) => {
                let bits = f.to_bits();
                bits.hash(state);
            }
            Prop::Bool(b) => b.hash(state),
            Prop::NDTime(dt) => dt.hash(state),
            Prop::DTime(dt) => dt.hash(state),
            Prop::List(v) => {
                for prop in v.iter() {
                    prop.hash(state);
                }
            }
            Prop::Map(m) => {
                // Based on python set hash
                let mut hash = Wrapping(1927868237u64);
                hash *= (m.len() as u64).wrapping_add(1);
                for v in m.iter() {
                    let mut inner_hasher = DefaultHasher::new();
                    v.hash(&mut inner_hasher);
                    let inner_hash = Wrapping(inner_hasher.finish());
                    hash ^= (inner_hash ^ (inner_hash << 16) ^ Wrapping(89869747u64))
                        * Wrapping(3644798167u64);
                }
                hash ^= (hash >> 11) ^ (hash >> 25);
                hash *= 69069;
                hash += 907133923;
                state.write_u64(hash.0);
            }
            Prop::Decimal(d) => d.hash(state),
        }
    }
}

impl Eq for Prop {}

impl PartialOrd for Prop {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Prop::Str(a), Prop::Str(b)) => a.partial_cmp(b),
            (Prop::U8(a), Prop::U8(b)) => a.partial_cmp(b),
            (Prop::U16(a), Prop::U16(b)) => a.partial_cmp(b),
            (Prop::I32(a), Prop::I32(b)) => a.partial_cmp(b),
            (Prop::I64(a), Prop::I64(b)) => a.partial_cmp(b),
            (Prop::U32(a), Prop::U32(b)) => a.partial_cmp(b),
            (Prop::U64(a), Prop::U64(b)) => a.partial_cmp(b),
            (Prop::F32(a), Prop::F32(b)) => a.partial_cmp(b),
            (Prop::F64(a), Prop::F64(b)) => a.partial_cmp(b),
            (Prop::Bool(a), Prop::Bool(b)) => a.partial_cmp(b),
            (Prop::NDTime(a), Prop::NDTime(b)) => a.partial_cmp(b),
            (Prop::DTime(a), Prop::DTime(b)) => a.partial_cmp(b),
            (Prop::List(a), Prop::List(b)) => a.partial_cmp(b),
            (Prop::Decimal(a), Prop::Decimal(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

pub struct SerdeArrowProp<'a>(pub &'a Prop);

#[derive(Clone, Copy, Debug)]
pub struct SerdeArrowList<'a>(pub &'a PropArray);

#[derive(Clone, Copy, Debug)]
pub struct SerdeArrowArray<'a>(pub &'a ArrayRef);

#[derive(Clone, Copy)]
pub struct SerdeArrowMap<'a>(pub &'a PropMap);

#[derive(Clone, Copy, Serialize)]
pub struct SerdeRow<P: Serialize> {
    value: Option<P>,
}

impl<'a> Serialize for SerdeArrowList<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0 {
            PropArray::Vec(list) => {
                let mut state = serializer.serialize_seq(Some(self.0.len()))?;

                for prop in list.iter() {
                    state.serialize_element(&SerdeArrowProp(prop))?;
                }

                state.end()
            }
            PropArray::Array(array) => SerdeArrowArray(array).serialize(serializer),
        }
    }
}

impl<'a> Serialize for SerdeArrowMap<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.iter() {
            state.serialize_entry(k, &SerdeArrowProp(v))?;
        }
        state.end()
    }
}

impl<'a> Serialize for SerdeArrowProp<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Prop::I32(i) => serializer.serialize_i32(*i),
            Prop::I64(i) => serializer.serialize_i64(*i),
            Prop::F32(f) => serializer.serialize_f32(*f),
            Prop::F64(f) => serializer.serialize_f64(*f),
            Prop::U8(u) => serializer.serialize_u8(*u),
            Prop::U16(u) => serializer.serialize_u16(*u),
            Prop::U32(u) => serializer.serialize_u32(*u),
            Prop::U64(u) => serializer.serialize_u64(*u),
            Prop::Str(s) => serializer.serialize_str(s),
            Prop::Bool(b) => serializer.serialize_bool(*b),
            Prop::DTime(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            Prop::NDTime(dt) => serializer.serialize_i64(dt.and_utc().timestamp_millis()),
            Prop::List(l) => SerdeArrowList(l).serialize(serializer),
            Prop::Map(m) => SerdeArrowMap(m).serialize(serializer),
            Prop::Decimal(dec) => {
                // Serialize BigDecimal as string manually to match
                // the Arrow Decimal128 format.
                let (num, scale) = dec.as_bigint_and_scale();

                let num_i128 = num.to_i128().ok_or_else(|| {
                    serde::ser::Error::custom(format!(
                        "decimal value {dec} is out of range for i128 representation"
                    ))
                })?;

                let num_formatted =
                    Decimal128Type::format_decimal(num_i128, DECIMAL128_MAX_PRECISION, scale as i8);

                serializer.serialize_str(&num_formatted)
            }
        }
    }
}

impl<'a> Serialize for SerdeArrowArray<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let dtype = self.0.data_type();
        let len = self.0.len();
        let mut state = serializer.serialize_seq(Some(len))?;

        match dtype {
            DataType::Boolean => {
                for v in self.0.as_boolean().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Int32 => {
                for v in self.0.as_primitive::<Int32Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Int64 => {
                for v in self.0.as_primitive::<Int64Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::UInt8 => {
                for v in self.0.as_primitive::<UInt8Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::UInt16 => {
                for v in self.0.as_primitive::<UInt16Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::UInt32 => {
                for v in self.0.as_primitive::<UInt32Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::UInt64 => {
                for v in self.0.as_primitive::<UInt64Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Float32 => {
                for v in self.0.as_primitive::<Float32Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Float64 => {
                for v in self.0.as_primitive::<Float64Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    for v in self.0.as_primitive::<TimestampSecondType>().iter() {
                        state.serialize_element(&v)?;
                    }
                }
                TimeUnit::Millisecond => {
                    for v in self.0.as_primitive::<TimestampMillisecondType>().iter() {
                        state.serialize_element(&v)?;
                    }
                }
                TimeUnit::Microsecond => {
                    for v in self.0.as_primitive::<TimestampMicrosecondType>().iter() {
                        state.serialize_element(&v)?;
                    }
                }
                TimeUnit::Nanosecond => {
                    for v in self.0.as_primitive::<TimestampNanosecondType>().iter() {
                        state.serialize_element(&v)?;
                    }
                }
            },
            DataType::Date32 => {
                for v in self.0.as_primitive::<Date32Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Date64 => {
                for v in self.0.as_primitive::<Date64Type>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Utf8 => {
                for v in self.0.as_string::<i32>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::LargeUtf8 => {
                for v in self.0.as_string::<i64>().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Utf8View => {
                for v in self.0.as_string_view().iter() {
                    state.serialize_element(&v)?;
                }
            }
            DataType::Decimal128(precision, scale) => {
                for v in self.0.as_primitive::<Decimal128Type>().iter() {
                    // i128 is not supported directly by serde_arrow,
                    // so we format as string manually.
                    let element = v.map(|v| Decimal128Type::format_decimal(v, *precision, *scale));

                    state.serialize_element(&element)?
                }
            }
            DataType::Struct(_) => {
                let struct_array = self.0.as_struct();
                match struct_array.nulls() {
                    None => {
                        for i in 0..struct_array.len() {
                            state.serialize_element(&ArrowRow::new(struct_array, i))?;
                        }
                    }
                    Some(nulls) => {
                        for (i, is_valid) in nulls.iter().enumerate() {
                            state.serialize_element(
                                &is_valid.then_some(ArrowRow::new(struct_array, i)),
                            )?;
                        }
                    }
                }
            }
            DataType::List(_) => {
                let list = self.0.as_list::<i32>();
                for array in list.iter() {
                    state.serialize_element(&array.as_ref().map(SerdeArrowArray))?;
                }
            }
            DataType::LargeList(_) => {
                let list = self.0.as_list::<i64>();
                for array in list.iter() {
                    state.serialize_element(&array.as_ref().map(SerdeArrowArray))?;
                }
            }
            DataType::Null => {
                for _ in 0..self.0.len() {
                    state.serialize_element(&None::<()>)?;
                }
            }
            dtype => Err(Error::custom(format!("unsuported data type {dtype:?}")))?,
        }
        state.end()
    }
}

pub fn validate_bd(bd: &BigDecimal) -> Result<(), InvalidBigDecimal> {
    let (bint, scale) = bd.as_bigint_and_exponent();
    if bint <= BigInt::from(DECIMAL_MAX) && scale <= 38 {
        Ok(())
    } else {
        Err(InvalidBigDecimal(bd.clone()))
    }
}

impl Prop {
    // auxiliary function to help with numerical conversion
    pub fn cast_num<T>(self) -> Option<T>
    where
        T: FromPrimitive + Bounded,
    {
        match self {
            Prop::U8(v) => T::from_u8(v),
            Prop::U16(v) => T::from_u16(v),
            Prop::I32(v) => T::from_i32(v),
            Prop::I64(v) => T::from_i64(v),
            Prop::U32(v) => T::from_u32(v),
            Prop::U64(v) => T::from_u64(v),
            Prop::F32(v) => T::from_f32(v),
            Prop::F64(v) => T::from_f64(v),
            _ => None,
        }
    }

    /// convert prop into another prop type (primarily for numerical conversions)
    pub fn try_cast(self, prop_type: PropType) -> Option<Prop> {
        // Early return if casting to the same type
        if self.dtype() == prop_type {
            return Some(self);
        }

        match self {
            Prop::Str(v) => match prop_type {
                PropType::Str => Some(Prop::Str(v)),
                PropType::U8 => v.parse::<u8>().map(Prop::U8).ok(),
                PropType::U16 => v.parse::<u16>().map(Prop::U16).ok(),
                PropType::I32 => v.parse::<i32>().map(Prop::I32).ok(),
                PropType::I64 => v.parse::<i64>().map(Prop::I64).ok(),
                PropType::U32 => v.parse::<u32>().map(Prop::U32).ok(),
                PropType::U64 => v.parse::<u64>().map(Prop::U64).ok(),
                PropType::F32 => v.parse::<f32>().map(Prop::F32).ok(),
                PropType::F64 => v.parse::<f64>().map(Prop::F64).ok(),
                PropType::Bool => v.parse::<bool>().map(Prop::Bool).ok(),
                PropType::NDTime => v.parse::<NaiveDateTime>().map(Prop::NDTime).ok(),
                PropType::DTime => v.parse::<DateTime<Utc>>().map(Prop::DTime).ok(),
                PropType::Decimal { scale } => v
                    .parse::<BigDecimal>()
                    .map(|v| Prop::Decimal(v.with_scale(scale)))
                    .ok(),
                _ => None,
            },
            Prop::Bool(v) => match prop_type {
                PropType::Str => Some(Prop::Str(v.to_string().into())),
                PropType::U8 => Some(Prop::U8(v as _)),
                PropType::U16 => Some(Prop::U16(v as _)),
                PropType::I32 => Some(Prop::I32(v as _)),
                PropType::I64 => Some(Prop::I64(v as _)),
                PropType::U32 => Some(Prop::U32(v as _)),
                PropType::U64 => Some(Prop::U64(v as _)),
                PropType::F32 => Some(Prop::F32(if v { 1.0 } else { 0.0 })),
                PropType::F64 => Some(Prop::F64(if v { 1.0 } else { 0.0 })),
                PropType::Bool => unreachable!("Same type case handled above"),
                PropType::Decimal { scale } => {
                    let val = if v {
                        BigDecimal::from(1)
                    } else {
                        BigDecimal::from(0)
                    };
                    Some(Prop::Decimal(val.with_scale(scale)))
                }
                _ => None,
            },
            Prop::List(_v) => None,
            Prop::Map(_v) => None,
            Prop::NDTime(v) => match prop_type {
                PropType::Str => Some(Prop::Str(v.to_string().into())),
                PropType::I64 => Some(Prop::I64(v.and_utc().timestamp())),
                PropType::U64 => {
                    let ts = v.and_utc().timestamp();
                    if ts >= 0 {
                        Some(Prop::U64(ts as u64))
                    } else {
                        None
                    }
                }
                PropType::DTime => Some(Prop::DTime(v.and_utc())),
                PropType::NDTime => unreachable!("Same type case handled above"),
                _ => None,
            },
            Prop::DTime(v) => match prop_type {
                PropType::Str => Some(Prop::Str(v.to_rfc3339().into())),
                PropType::I64 => Some(Prop::I64(v.timestamp())),
                PropType::U64 => {
                    let ts = v.timestamp();
                    if ts >= 0 {
                        Some(Prop::U64(ts as u64))
                    } else {
                        None
                    }
                }
                PropType::NDTime => Some(Prop::NDTime(v.naive_utc())),
                PropType::DTime => unreachable!("Same type case handled above"),
                _ => None,
            },
            Prop::Decimal(v) => match prop_type {
                PropType::Str => Some(Prop::Str(v.to_string().into())),
                PropType::U8 => {
                    let as_i64 = v.to_i64()?;
                    u8::from_i64(as_i64).map(Prop::U8)
                }
                PropType::U16 => {
                    let as_i64 = v.to_i64()?;
                    u16::from_i64(as_i64).map(Prop::U16)
                }
                PropType::I32 => {
                    let as_i64 = v.to_i64()?;
                    i32::from_i64(as_i64).map(Prop::I32)
                }
                PropType::I64 => v.to_i64().map(Prop::I64),
                PropType::U32 => {
                    let as_i64 = v.to_i64()?;
                    u32::from_i64(as_i64).map(Prop::U32)
                }
                PropType::U64 => {
                    let as_i64 = v.to_i64()?;
                    u64::from_i64(as_i64).map(Prop::U64)
                }
                PropType::F32 => v.to_f32().map(Prop::F32),
                PropType::F64 => v.to_f64().map(Prop::F64),
                PropType::Bool => {
                    let as_i64 = v.to_i64()?;
                    Some(Prop::Bool(as_i64 != 0))
                }
                PropType::Decimal { scale } => Some(Prop::Decimal(v.with_scale(scale))),
                _ => None,
            },
            _ => match prop_type {
                // Numeric conversions using num_traits
                PropType::U8 => self.cast_num::<u8>().map(Prop::U8),
                PropType::U16 => self.cast_num::<u16>().map(Prop::U16),
                PropType::I32 => self.cast_num::<i32>().map(Prop::I32),
                PropType::I64 => self.cast_num::<i64>().map(Prop::I64),
                PropType::U32 => self.cast_num::<u32>().map(Prop::U32),
                PropType::U64 => self.cast_num::<u64>().map(Prop::U64),
                PropType::F32 => self.cast_num::<f32>().map(Prop::F32),
                PropType::F64 => self.cast_num::<f64>().map(Prop::F64),
                _ => None,
            },
        }
    }

    /// Losslessly widen unsigned integer variants to u64.
    #[inline]
    pub fn as_u64_lossless(&self) -> Option<u64> {
        match self {
            Prop::U8(v) => Some(*v as u64),
            Prop::U16(v) => Some(*v as u64),
            Prop::U32(v) => Some(*v as u64),
            Prop::U64(v) => Some(*v),
            _ => None,
        }
    }

    /// Losslessly widen signed integer variants to i64.
    #[inline]
    pub fn as_i64_lossless(&self) -> Option<i64> {
        match self {
            Prop::I32(v) => Some(*v as i64),
            Prop::I64(v) => Some(*v),
            _ => None,
        }
    }

    /// Losslessly widen float variants to f64.
    #[inline]
    pub fn as_f64_lossless(&self) -> Option<f64> {
        match self {
            Prop::F32(v) => Some(*v as f64),
            Prop::F64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn try_from_bd(bd: BigDecimal) -> Result<Prop, InvalidBigDecimal> {
        validate_bd(&bd)?;
        Ok(Prop::Decimal(bd))
    }

    pub fn map(vals: impl IntoIterator<Item = (impl Into<ArcStr>, impl Into<Prop>)>) -> Self {
        let h_map: PropMap = vals
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Prop::Map(h_map.into())
    }

    pub fn as_map(&self) -> Option<SerdeArrowMap<'_>> {
        match self {
            Prop::Map(map) => Some(SerdeArrowMap(map)),
            _ => None,
        }
    }

    pub fn dtype(&self) -> PropType {
        match self {
            Prop::Str(_) => PropType::Str,
            Prop::U8(_) => PropType::U8,
            Prop::U16(_) => PropType::U16,
            Prop::I32(_) => PropType::I32,
            Prop::I64(_) => PropType::I64,
            Prop::U32(_) => PropType::U32,
            Prop::U64(_) => PropType::U64,
            Prop::F32(_) => PropType::F32,
            Prop::F64(_) => PropType::F64,
            Prop::Bool(_) => PropType::Bool,
            Prop::List(list) => PropType::List(Box::new(list.dtype())),
            Prop::Map(map) => PropType::map(map.iter().map(|(k, v)| (k, v.dtype()))),
            Prop::NDTime(_) => PropType::NDTime,
            Prop::DTime(_) => PropType::DTime,
            Prop::Decimal(d) => PropType::Decimal {
                scale: d.as_bigint_and_scale().1,
            },
        }
    }

    pub fn str<S: Into<ArcStr>>(s: S) -> Prop {
        Prop::Str(s.into())
    }

    pub fn list<P: Into<Prop>, I: IntoIterator<Item = P>>(vals: I) -> Prop {
        Prop::List(PropArray::Vec(
            vals.into_iter().map_into().collect::<Vec<_>>().into(),
        ))
    }

    /// An exact unsigned value as the widest unsigned prop (`U64`), or `Decimal` once it exceeds
    /// `u64`. Cross-type results widen up to the family's widest type — never down to a narrower one.
    fn u64_or_decimal(v: u128) -> Prop {
        if v <= u64::MAX as u128 {
            Prop::U64(v as u64)
        } else {
            Prop::Decimal(BigDecimal::from(v))
        }
    }

    /// An exact signed value as the widest signed prop (`I64`), or `Decimal` once it exceeds `i64`.
    fn i64_or_decimal(v: i128) -> Prop {
        if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
            Prop::I64(v as i64)
        } else {
            Prop::Decimal(BigDecimal::from(v))
        }
    }

    /// Consume a numeric prop into a `BigDecimal` — exact for integers and existing decimals, the
    /// nearest decimal for floats. `None` for non-numerics (and non-finite floats).
    fn into_big_decimal(self) -> Option<BigDecimal> {
        match self {
            Prop::Decimal(d) => Some(d),
            Prop::F32(v) => BigDecimal::from_f64(v as f64),
            Prop::F64(v) => BigDecimal::from_f64(v),
            other => other.as_i128().map(BigDecimal::from),
        }
    }

    fn is_unsigned_int(&self) -> bool {
        matches!(
            self,
            Prop::U8(_) | Prop::U16(_) | Prop::U32(_) | Prop::U64(_)
        )
    }

    /// Add two props. Same-type integer sums keep their type, bumping one size up on overflow
    /// (`u8`→`u16`→…→`u64`→`Decimal`, `i32`→`i64`→`Decimal`). A *cross-type* numeric pair widens to
    /// the widest type in the common family — two unsigned → `U64`, any signed → `I64`, any float →
    /// `F64`, any `Decimal` → `Decimal` — spilling to `Decimal` past `u64`/`i64`, so the result is
    /// never narrower than either operand. Strings and lists concatenate; other types return `None`.
    pub fn add(self, other: Prop) -> Option<Prop> {
        use Prop::*;
        match (self, other) {
            // Same integer type: checked add is the fast path; on overflow the sum always fits
            // exactly one size up (two `u16`s fit a `u32`, …), so bump deterministically — no range
            // checks. `u64`/`i64` have no wider integer, so they spill to exact `Decimal`.
            (U8(a), U8(b)) => Some(match a.checked_add(b) {
                Some(v) => U8(v),
                None => U16(a as u16 + b as u16),
            }),
            (U16(a), U16(b)) => Some(match a.checked_add(b) {
                Some(v) => U16(v),
                None => U32(a as u32 + b as u32),
            }),
            (U32(a), U32(b)) => Some(match a.checked_add(b) {
                Some(v) => U32(v),
                None => U64(a as u64 + b as u64),
            }),
            (U64(a), U64(b)) => Some(match a.checked_add(b) {
                Some(v) => U64(v),
                None => Decimal(BigDecimal::from(a as u128 + b as u128)),
            }),
            (I32(a), I32(b)) => Some(match a.checked_add(b) {
                Some(v) => I32(v),
                None => I64(a as i64 + b as i64),
            }),
            (I64(a), I64(b)) => Some(match a.checked_add(b) {
                Some(v) => I64(v),
                None => Decimal(BigDecimal::from(a as i128 + b as i128)),
            }),
            (F32(a), F32(b)) => Some(F32(a + b)),
            (F64(a), F64(b)) => Some(F64(a + b)),
            (Str(a), Str(b)) => Some(Str((a.to_string() + b.as_ref()).into())),
            (Decimal(a), Decimal(b)) => Some(Decimal(a + b)),
            (List(a), List(b)) => Some(List(PropArray::Vec(
                a.iter().chain(b.iter()).collect::<Vec<_>>().into(),
            ))),
            // Cross-type numeric pair: widen to the widest type in the common family (never
            // narrower than either operand), spilling to `Decimal` past `u64`/`i64`. Non-numeric
            // pairs are unaddable.
            (a, b) => {
                if matches!(a, Decimal(_)) || matches!(b, Decimal(_)) {
                    Some(Decimal(a.into_big_decimal()? + b.into_big_decimal()?))
                } else if matches!(a, F32(_) | F64(_)) || matches!(b, F32(_) | F64(_)) {
                    Some(F64(a.as_f64()? + b.as_f64()?))
                } else if a.is_unsigned_int() && b.is_unsigned_int() {
                    Some(Self::u64_or_decimal(a.as_u128()? + b.as_u128()?))
                } else {
                    Some(Self::i64_or_decimal(a.as_i128()? + b.as_i128()?))
                }
            }
        }
    }

    /// Subtract two numeric props, widening to the common family like [`add`](Prop::add). An
    /// unsigned difference that goes negative drops to the signed family. `None` for non-numerics.
    pub fn sub(self, other: Prop) -> Option<Prop> {
        use Prop::*;
        if matches!(self, Decimal(_)) || matches!(other, Decimal(_)) {
            Some(Decimal(
                self.into_big_decimal()? - other.into_big_decimal()?,
            ))
        } else if matches!(self, F32(_) | F64(_)) || matches!(other, F32(_) | F64(_)) {
            Some(F64(self.as_f64()? - other.as_f64()?))
        } else if self.is_unsigned_int() && other.is_unsigned_int() {
            let d = self.as_i128()? - other.as_i128()?;
            Some(if d >= 0 {
                Self::u64_or_decimal(d as u128)
            } else {
                Self::i64_or_decimal(d)
            })
        } else {
            Some(Self::i64_or_decimal(self.as_i128()? - other.as_i128()?))
        }
    }

    /// Multiply two numeric props, widening to the common family like [`add`](Prop::add) (spilling
    /// to an exact `Decimal` past `u64`/`i64`). `None` for non-numerics.
    pub fn mul(self, other: Prop) -> Option<Prop> {
        use Prop::*;
        if matches!(self, Decimal(_)) || matches!(other, Decimal(_)) {
            Some(Decimal(
                self.into_big_decimal()? * other.into_big_decimal()?,
            ))
        } else if matches!(self, F32(_) | F64(_)) || matches!(other, F32(_) | F64(_)) {
            Some(F64(self.as_f64()? * other.as_f64()?))
        } else if self.is_unsigned_int() && other.is_unsigned_int() {
            Some(Self::u64_or_decimal(self.as_u128()? * other.as_u128()?))
        } else {
            Some(Self::i64_or_decimal(self.as_i128()? * other.as_i128()?))
        }
    }

    /// Divide two numeric props by true division: integers divide as `f64` (`5 / 2 == 2.5`),
    /// decimals divide exactly in `Decimal`. Division by zero is `None` for every family. `None`
    /// for non-numerics.
    pub fn div(self, other: Prop) -> Option<Prop> {
        use Prop::*;
        if matches!(self, Decimal(_)) || matches!(other, Decimal(_)) {
            let (a, b) = (self.into_big_decimal()?, other.into_big_decimal()?);
            (!b.is_zero()).then(|| Decimal(a / b))
        } else {
            let (a, b) = (self.as_f64()?, other.as_f64()?);
            (b != 0.0).then(|| F64(a / b))
        }
    }

    pub fn min(self, other: Prop) -> Option<Prop> {
        self.compare(&other).map(|ord| match ord {
            Ordering::Less | Ordering::Equal => self,
            Ordering::Greater => other,
        })
    }

    pub fn max(self, other: Prop) -> Option<Prop> {
        self.compare(&other).map(|ord| match ord {
            Ordering::Less => other,
            Ordering::Equal | Ordering::Greater => self,
        })
    }

    /// The numeric value as `i128` (all integer variants fit), or `None` for non-integers.
    fn as_i128(&self) -> Option<i128> {
        Some(match self {
            Prop::U8(v) => *v as i128,
            Prop::U16(v) => *v as i128,
            Prop::U32(v) => *v as i128,
            Prop::U64(v) => *v as i128,
            Prop::I32(v) => *v as i128,
            Prop::I64(v) => *v as i128,
            _ => return None,
        })
    }

    /// The value as `u128`, or `None` for non-unsigned-integer variants.
    fn as_u128(&self) -> Option<u128> {
        Some(match self {
            Prop::U8(v) => *v as u128,
            Prop::U16(v) => *v as u128,
            Prop::U32(v) => *v as u128,
            Prop::U64(v) => *v as u128,
            _ => return None,
        })
    }

    /// True for every numeric variant (integers, floats and `Decimal`); false otherwise.
    fn is_numeric(&self) -> bool {
        matches!(
            self,
            Prop::U8(_)
                | Prop::U16(_)
                | Prop::U32(_)
                | Prop::U64(_)
                | Prop::I32(_)
                | Prop::I64(_)
                | Prop::F32(_)
                | Prop::F64(_)
                | Prop::Decimal(_)
        )
    }

    /// Order two props, widening across every numeric variant: two integers compare exactly (via
    /// `i128`), a `Decimal` on either side compares exactly in `Decimal`, and any remaining mix
    /// compares via `f64`. Non-numeric variants fall back to the same-type ordering ([`PartialOrd`]);
    /// incomparable pairs return `None`.
    pub fn compare(&self, other: &Prop) -> Option<Ordering> {
        if let (Some(a), Some(b)) = (self.as_i128(), other.as_i128()) {
            return a.partial_cmp(&b);
        }
        // A `Decimal` on either side: compare exactly in `Decimal` rather than losing precision
        // through `f64`.
        if matches!(self, Prop::Decimal(_)) || matches!(other, Prop::Decimal(_)) {
            if let (Some(a), Some(b)) = (
                self.clone().into_big_decimal(),
                other.clone().into_big_decimal(),
            ) {
                return a.partial_cmp(&b);
            }
        }
        if let (Some(a), Some(b)) = (self.as_f64(), other.as_f64()) {
            return a.partial_cmp(&b);
        }
        self.partial_cmp(other)
    }

    /// True if two props are equal. Every numeric variant is widened (`1i64 == 1.0f64 ==
    /// Decimal(1)`); every other type uses structural equality ([`PartialEq`]), so strings, bools,
    /// lists and maps compare as usual.
    pub fn equals(&self, other: &Prop) -> bool {
        if self.is_numeric() && other.is_numeric() {
            self.compare(other) == Some(Ordering::Equal)
        } else {
            self == other
        }
    }

    /// The mean of numeric `props` as an `F64` prop, or `None` if empty or any value is non-numeric.
    /// Folds [`add`](Prop::add), so the running sum stays exact (widening and spilling to `Decimal`
    /// rather than drifting in `f64`), and converts once for the final division.
    pub fn mean<'a>(props: impl IntoIterator<Item = &'a Prop>) -> Option<Prop> {
        let mut it = props.into_iter();
        let mut sum = it.next()?.clone();
        let mut count = 1u64;
        for p in it {
            sum = sum.add(p.clone())?;
            count += 1;
        }
        Some(Prop::F64(sum.as_f64()? / count as f64))
    }

    /// The median of numeric `props` as an `F64` prop (mean of the two middle values on even
    /// length), or `None` if empty or any value is non-numeric. Sorts exactly via
    /// [`compare`](Prop::compare) — so values beyond `2^53` order correctly, unlike an `f64` sort —
    /// then converts only the one or two middle values.
    pub fn median<'a>(props: impl IntoIterator<Item = &'a Prop>) -> Option<Prop> {
        let mut vals: Vec<&Prop> = props.into_iter().collect();
        if vals.is_empty() || !vals.iter().all(|p| p.is_numeric()) {
            return None;
        }
        vals.sort_by(|a, b| a.compare(b).unwrap_or(Ordering::Equal));
        let n = vals.len();
        let m = if n % 2 == 1 {
            vals[n / 2].as_f64()?
        } else {
            (vals[n / 2 - 1].as_f64()? + vals[n / 2].as_f64()?) / 2.0
        };
        Some(Prop::F64(m))
    }
}

pub fn list_array_from_props<P: Serialize + fmt::Debug + Clone>(
    dt: &DataType,
    props: impl IntoIterator<Item = Option<P>>,
) -> Result<LargeListArray, serde_arrow::Error> {
    let fields: Fields = vec![Field::new("value", dt.clone(), true)].into();
    let mut builder = ArrayBuilder::from_arrow(&fields)?;

    for value in props {
        builder.push(SerdeRow { value })?;
    }

    let arrays = builder.to_arrow()?;

    Ok(arrays.first().unwrap().as_list::<i64>().clone())
}

pub fn struct_array_from_props<P: Serialize>(
    dt: &DataType,
    props: impl IntoIterator<Item = Option<P>>,
) -> Result<StructArray, serde_arrow::Error> {
    let fields = [FieldRef::new(Field::new("value", dt.clone(), true))];
    let mut builder = ArrayBuilder::from_arrow(&fields)?;

    for p in props {
        builder.push(SerdeRow { value: p })?
    }

    let arrays = builder.to_arrow()?;

    Ok(arrays.first().unwrap().as_struct().clone())
}

impl Display for Prop {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Prop::Str(value) => write!(f, "{}", value),
            Prop::U8(value) => write!(f, "{}", value),
            Prop::U16(value) => write!(f, "{}", value),
            Prop::I32(value) => write!(f, "{}", value),
            Prop::I64(value) => write!(f, "{}", value),
            Prop::U32(value) => write!(f, "{}", value),
            Prop::U64(value) => write!(f, "{}", value),
            Prop::F32(value) => write!(f, "{}", value),
            Prop::F64(value) => write!(f, "{}", value),
            Prop::Bool(value) => write!(f, "{}", value),
            Prop::DTime(value) => write!(f, "{}", value),
            Prop::NDTime(value) => write!(f, "{}", value),
            Prop::List(value) => {
                write!(
                    f,
                    "[{}]",
                    value
                        .iter()
                        .map(|item| {
                            match item {
                                Prop::Str(_) => {
                                    format!("\"{}\"", item)
                                }
                                _ => {
                                    format!("{}", item)
                                }
                            }
                        })
                        .join(", ")
                )
            }
            Prop::Map(value) => {
                write!(
                    f,
                    "{{{}}}",
                    value
                        .iter()
                        .map(|(key, val)| {
                            match val {
                                Prop::Str(_) => {
                                    format!("\"{}\": \"{}\"", key, val)
                                }
                                _ => {
                                    format!("\"{}\": {}", key, val)
                                }
                            }
                        })
                        .join(", ")
                )
            }
            Prop::Decimal(d) => write!(f, "Decimal({})", d.as_bigint_and_scale().1),
        }
    }
}

impl From<&str> for Prop {
    fn from(s: &str) -> Self {
        Prop::Str(s.into())
    }
}

impl From<String> for Prop {
    fn from(s: String) -> Self {
        Prop::Str(s.into())
    }
}

impl From<HashMap<ArcStr, Prop>> for Prop {
    fn from(value: HashMap<ArcStr, Prop>) -> Self {
        Prop::Map(Arc::new(value.into_iter().collect()))
    }
}

impl From<FxHashMap<ArcStr, Prop>> for Prop {
    fn from(value: FxHashMap<ArcStr, Prop>) -> Self {
        Prop::Map(Arc::new(value.into_iter().collect()))
    }
}

impl From<PropMap> for Prop {
    fn from(value: PropMap) -> Self {
        Prop::Map(Arc::new(value))
    }
}

impl From<Vec<Prop>> for Prop {
    fn from(value: Vec<Prop>) -> Self {
        Prop::List(value.into())
    }
}

impl From<&Prop> for Prop {
    fn from(value: &Prop) -> Self {
        value.clone()
    }
}

impl From<ArrayRef> for Prop {
    fn from(value: ArrayRef) -> Self {
        Prop::List(PropArray::from(value))
    }
}

pub trait IntoPropMap {
    fn into_prop_map(self) -> Prop;
}

impl<I: IntoIterator<Item = (K, V)>, K: Into<ArcStr>, V: Into<Prop>> IntoPropMap for I {
    fn into_prop_map(self) -> Prop {
        Prop::Map(Arc::new(
            self.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        ))
    }
}

pub trait IntoPropList {
    fn into_prop_list(self) -> Prop;
}

impl<I: IntoIterator<Item = K>, K: Into<Prop>> IntoPropList for I {
    fn into_prop_list(self) -> Prop {
        let vec = self.into_iter().map(|v| v.into()).collect::<Vec<_>>();
        Prop::List(vec.into())
    }
}

pub trait IntoProp {
    fn into_prop(self) -> Prop;
}

impl<T: Into<Prop>> IntoProp for T {
    fn into_prop(self) -> Prop {
        self.into()
    }
}

pub fn sort_comparable_props(props: Vec<&Prop>) -> Vec<&Prop> {
    // Filter out non-comparable props
    let mut comparable_props: Vec<_> = props
        .into_iter()
        .filter(|p| {
            matches!(
                p,
                Prop::Str(_)
                    | Prop::U8(_)
                    | Prop::U16(_)
                    | Prop::I32(_)
                    | Prop::I64(_)
                    | Prop::U32(_)
                    | Prop::U64(_)
                    | Prop::F32(_)
                    | Prop::F64(_)
                    | Prop::Bool(_)
                    | Prop::NDTime(_)
                    | Prop::DTime(_)
            )
        })
        .collect();

    // Sort the comparable props
    comparable_props.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    comparable_props
}

#[cfg(test)]
mod agg_arith_tests {
    use super::*;

    #[test]
    fn add_keeps_type_without_overflow() {
        assert_eq!(Prop::U8(1).add(Prop::U8(2)), Some(Prop::U8(3)));
        assert_eq!(Prop::I64(10).add(Prop::I64(5)), Some(Prop::I64(15)));
        assert_eq!(Prop::F64(1.5).add(Prop::F64(2.25)), Some(Prop::F64(3.75)));
    }

    #[test]
    fn add_widens_integer_on_overflow_then_spills_to_decimal() {
        assert_eq!(Prop::U8(200).add(Prop::U8(200)), Some(Prop::U16(400)));
        assert_eq!(
            Prop::U16(u16::MAX).add(Prop::U16(1)),
            Some(Prop::U32(65536))
        );
        assert_eq!(
            Prop::U32(u32::MAX).add(Prop::U32(1)),
            Some(Prop::U64(4294967296))
        );
        // Past `u64`/`i64` there is no wider integer, so the exact sum spills to `Decimal`, never
        // to a lossy `f64`.
        assert_eq!(
            Prop::U64(u64::MAX).add(Prop::U64(1)),
            Some(Prop::Decimal(BigDecimal::from(u64::MAX as u128 + 1)))
        );
        assert_eq!(
            Prop::I32(i32::MAX).add(Prop::I32(1)),
            Some(Prop::I64(2147483648))
        );
        assert_eq!(
            Prop::I64(i64::MAX).add(Prop::I64(1)),
            Some(Prop::Decimal(BigDecimal::from(i64::MAX as i128 + 1)))
        );
    }

    #[test]
    fn add_mixed_integer_widths() {
        // Cross-type widens to the widest type in the family (U64 for two unsigned) — never
        // narrower than either operand.
        assert_eq!(Prop::U16(400).add(Prop::U8(200)), Some(Prop::U64(600)));
    }

    #[test]
    fn add_keeps_decimal_accumulator_exact_in_fold() {
        // A sum that spills to `Decimal` on the first overflow must keep adding exactly in
        // `Decimal` as the fold continues — never drop back to a lossy `f64`.
        let sum = [
            Prop::U64(u64::MAX),
            Prop::U64(u64::MAX),
            Prop::U64(u64::MAX),
        ]
        .into_iter()
        .reduce(|a, b| a.add(b).expect("integer sum is always addable"));
        assert_eq!(
            sum,
            Some(Prop::Decimal(BigDecimal::from(3u128 * u64::MAX as u128)))
        );
        // Signed side, and Decimal on the left-hand side too.
        assert_eq!(
            Prop::Decimal(BigDecimal::from(i64::MAX as i128 * 2)).add(Prop::I64(i64::MAX)),
            Some(Prop::Decimal(BigDecimal::from(i64::MAX as i128 * 3)))
        );
    }

    #[test]
    fn add_mixes_integer_and_float_as_f64() {
        assert_eq!(Prop::I64(1).add(Prop::F64(2.5)), Some(Prop::F64(3.5)));
        assert_eq!(Prop::F64(2.5).add(Prop::U8(1)), Some(Prop::F64(3.5)));
        assert_eq!(Prop::F32(1.5).add(Prop::F64(2.0)), Some(Prop::F64(3.5)));
        assert_eq!(Prop::F64(1.0).add(Prop::str("x")), None);
    }

    #[test]
    fn sub_widens_and_flips_sign_when_negative() {
        // Widest-family: two unsigned → U64 (a non-negative diff), I64 once it goes negative.
        assert_eq!(Prop::U8(5).sub(Prop::U8(3)), Some(Prop::U64(2)));
        assert_eq!(Prop::U8(3).sub(Prop::U8(5)), Some(Prop::I64(-2)));
        assert_eq!(Prop::I64(10).sub(Prop::I64(4)), Some(Prop::I64(6)));
        assert_eq!(Prop::F64(2.5).sub(Prop::U8(1)), Some(Prop::F64(1.5)));
        assert_eq!(Prop::Bool(true).sub(Prop::U8(1)), None);
    }

    #[test]
    fn mul_widens_and_spills_to_decimal() {
        // Widest-family: two unsigned → U64, any signed → I64.
        assert_eq!(Prop::U8(20).mul(Prop::U8(20)), Some(Prop::U64(400)));
        assert_eq!(
            Prop::I32(1000).mul(Prop::I32(1000)),
            Some(Prop::I64(1_000_000))
        );
        // Product past u64 has no wider integer, so it spills to exact Decimal.
        assert_eq!(
            Prop::U64(u64::MAX).mul(Prop::U64(2)),
            Some(Prop::Decimal(BigDecimal::from(u64::MAX as u128 * 2)))
        );
        assert_eq!(Prop::F32(2.0).mul(Prop::I64(3)), Some(Prop::F64(6.0)));
    }

    #[test]
    fn div_is_true_division() {
        // Integers divide as f64 (no truncation), so 5 / 2 == 2.5.
        assert_eq!(Prop::I64(5).div(Prop::I64(2)), Some(Prop::F64(2.5)));
        assert_eq!(Prop::U8(9).div(Prop::U8(4)), Some(Prop::F64(2.25)));
        assert_eq!(Prop::I64(5).div(Prop::I64(0)), None);
        // Decimals divide exactly, staying Decimal.
        assert_eq!(
            Prop::Decimal(BigDecimal::from(10)).div(Prop::Decimal(BigDecimal::from(4))),
            Some(Prop::Decimal(BigDecimal::from(10) / BigDecimal::from(4)))
        );
        assert_eq!(
            Prop::Decimal(BigDecimal::from(1)).div(Prop::Decimal(BigDecimal::from(0))),
            None
        );
    }

    #[test]
    fn decimal_converts_to_f64() {
        // Root guard for the algorithm weight bugs: a `Decimal` must read as its value, not `None`
        // (which callers like `balance`/`pagerank`/`dijkstra` treat as missing or `.unwrap()`).
        assert_eq!(Prop::Decimal(BigDecimal::from(3)).as_f64(), Some(3.0));
        assert_eq!(
            Prop::Decimal(BigDecimal::from_f64(2.5).unwrap()).as_f64(),
            Some(2.5)
        );
    }

    #[test]
    fn add_concatenates_strings_and_lists() {
        assert_eq!(
            Prop::str("ab").add(Prop::str("cd")),
            Some(Prop::str("abcd"))
        );
        let a = Prop::list([Prop::I64(1), Prop::I64(2)]);
        let b = Prop::list([Prop::I64(3)]);
        assert_eq!(
            a.add(b),
            Some(Prop::list([Prop::I64(1), Prop::I64(2), Prop::I64(3)]))
        );
    }

    #[test]
    fn add_rejects_non_additive_types() {
        assert_eq!(Prop::Bool(true).add(Prop::Bool(false)), None);
    }

    #[test]
    fn compare_widens_across_numeric_types() {
        assert_eq!(Prop::I64(1).compare(&Prop::F64(1.0)), Some(Ordering::Equal));
        assert_eq!(Prop::U8(2).compare(&Prop::I64(5)), Some(Ordering::Less));
        assert_eq!(
            Prop::F32(2.5).compare(&Prop::I32(2)),
            Some(Ordering::Greater)
        );
        assert_eq!(Prop::I64(1).compare(&Prop::str("x")), None);
    }

    #[test]
    fn equals_widens_numerics_but_not_other_types() {
        assert!(Prop::I64(1).equals(&Prop::F64(1.0)));
        assert!(!Prop::I64(1).equals(&Prop::F64(1.5)));
        assert!(!Prop::I64(1).equals(&Prop::str("1")));
        assert!(Prop::Bool(true).equals(&Prop::Bool(true)));
    }

    #[test]
    fn mean_and_median_are_f64() {
        let xs = [Prop::I64(3), Prop::I64(5), Prop::I64(8), Prop::I64(2)];
        assert_eq!(Prop::mean(&xs), Some(Prop::F64(4.5)));
        assert_eq!(Prop::median(&xs), Some(Prop::F64(4.0)));
        assert_eq!(Prop::mean(std::iter::empty::<&Prop>()), None);
        assert_eq!(Prop::mean(&[Prop::str("x")]), None);
    }

    #[test]
    fn mean_accumulates_integers_exactly_without_overflow() {
        // Four `i64::MAX`s overflow an i64 sum; accumulated in i128 the mean is exact (= i64::MAX).
        let big = Prop::I64(i64::MAX);
        assert_eq!(
            Prop::mean(&[big.clone(), big.clone(), big.clone(), big.clone()]),
            Some(Prop::F64(i64::MAX as f64))
        );
    }
}
