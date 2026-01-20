use crate::core::{entities::properties::prop::PropType, storage::arc_str::ArcStr};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use num_traits::{Bounded, FromPrimitive, ToPrimitive};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fmt::{Display, Error, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};
use thiserror::Error;

#[cfg(feature = "arrow")]
use crate::core::entities::properties::prop::prop_array::*;
use crate::core::entities::properties::prop::unify_types;

pub const DECIMAL_MAX: i128 = 99999999999999999999999999999999999999i128; // equivalent to parquet decimal(38, 0)

#[derive(Error, Debug)]
#[error("Decimal {0} too large.")]
pub struct InvalidBigDecimal(BigDecimal);

/// Denotes the types of properties allowed to be stored in the graph.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum Prop {
    Str(ArcStr),
    U8(u8),
    U16(u16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
    #[cfg(feature = "arrow")]
    Array(PropArray),
    List(Arc<Vec<Prop>>),
    Map(Arc<FxHashMap<ArcStr, Prop>>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Decimal(BigDecimal),
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
            #[cfg(feature = "arrow")]
            Prop::Array(b) => b.hash(state),
            Prop::DTime(dt) => dt.hash(state),
            Prop::List(v) => {
                for prop in v.iter() {
                    prop.hash(state);
                }
            }
            Prop::Map(m) => {
                for (key, prop) in m.iter() {
                    key.hash(state);
                    prop.hash(state);
                }
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

pub fn validate_prop(prop: Prop) -> Result<Prop, InvalidBigDecimal> {
    match prop {
        Prop::Decimal(ref bd) => {
            let (bint, scale) = bd.as_bigint_and_exponent();
            if bint <= BigInt::from(DECIMAL_MAX) && scale <= 38 {
                Ok(prop)
            } else {
                Err(InvalidBigDecimal(bd.clone()))
            }
        }
        _ => Ok(prop),
    }
}

fn float_to_int<T>(val: f64) -> Result<T, String>
where
    T: FromPrimitive + Bounded + ToPrimitive,
{
    if val.is_nan() {
        return Err("Cannot convert NaN to integer".into());
    }

    if val.is_infinite() {
        return Err("Cannot convert infinite value to integer".into());
    }

    // Try to convert using num_traits
    T::from_f64(val).ok_or_else(|| format!("Value is out of bounds for target type: {}", val))
}

impl Prop {
    fn try_into_int<T>(self) -> Result<T, Error>
    where
        T: FromPrimitive + Bounded,
    {
        match self {
            Prop::U8(v) => T::from_u8(v).ok_or(Error),
            Prop::U16(v) => T::from_u16(v).ok_or(Error),
            Prop::I32(v) => T::from_i32(v).ok_or(Error),
            Prop::I64(v) => T::from_i64(v).ok_or(Error),
            Prop::U32(v) => T::from_u32(v).ok_or(Error),
            Prop::U64(v) => T::from_u64(v).ok_or(Error),
            Prop::F32(v) => {
                let as_f64 = v as f64;
                float_to_int::<i64>(as_f64)
                    .map_err(|_| Error)
                    .and_then(|i| T::from_i64(i).ok_or(Error))
            }
            Prop::F64(v) => float_to_int::<i64>(v)
                .map_err(|_| Error)
                .and_then(|i| T::from_i64(i).ok_or(Error)),
            _ => Err(Error),
        }
    }

    fn into_f64(self) -> Result<f64, Error> {
        let result = match self {
            Prop::U8(v) => v.to_f64(),
            Prop::U16(v) => v.to_f64(),
            Prop::I32(v) => v.to_f64(),
            Prop::I64(v) => v.to_f64(),
            Prop::U32(v) => v.to_f64(),
            Prop::U64(v) => v.to_f64(),
            Prop::F32(v) => v.to_f64(),
            Prop::F64(v) => Some(v),
            _ => None,
        };
        result.ok_or(Error)
    }

    fn try_into_f32(self) -> Result<Prop, Error> {
        let as_f32 = match self {
            Prop::U8(v) => v.to_f32(),
            Prop::U16(v) => v.to_f32(),
            Prop::I32(v) => v.to_f32(),
            Prop::I64(v) => v.to_f32(),
            Prop::U32(v) => v.to_f32(),
            Prop::U64(v) => v.to_f32(),
            Prop::F32(v) => Some(v),
            Prop::F64(v) => {
                // Check if f64 value fits in f32 range
                if v.is_finite()
                    && v.abs() <= f32::MAX as f64
                    && (v == 0.0 || v.abs() >= f32::MIN_POSITIVE as f64)
                {
                    Some(v as f32)
                } else if v.is_nan() || v.is_infinite() {
                    Some(v as f32) // Preserve NaN and infinity
                } else {
                    None
                }
            }
            _ => None,
        };
        as_f32.map(Prop::F32).ok_or(Error)
    }

    pub fn try_cast(self, prop_type: PropType) -> Result<Prop, Error> {
        // Early return if casting to the same type
        if self.dtype() == prop_type {
            return Ok(self);
        }

        match self {
            Prop::Str(v) => match prop_type {
                PropType::Str => Ok(Prop::Str(v)),
                PropType::U8 => v.parse::<u8>().map(Prop::U8).map_err(|_| Error),
                PropType::U16 => v.parse::<u16>().map(Prop::U16).map_err(|_| Error),
                PropType::I32 => v.parse::<i32>().map(Prop::I32).map_err(|_| Error),
                PropType::I64 => v.parse::<i64>().map(Prop::I64).map_err(|_| Error),
                PropType::U32 => v.parse::<u32>().map(Prop::U32).map_err(|_| Error),
                PropType::U64 => v.parse::<u64>().map(Prop::U64).map_err(|_| Error),
                PropType::F32 => v.parse::<f32>().map(Prop::F32).map_err(|_| Error),
                PropType::F64 => v.parse::<f64>().map(Prop::F64).map_err(|_| Error),
                PropType::Bool => v.parse::<bool>().map(Prop::Bool).map_err(|_| Error),
                PropType::NDTime => v
                    .parse::<NaiveDateTime>()
                    .map(Prop::NDTime)
                    .map_err(|_| Error),
                PropType::DTime => v
                    .parse::<DateTime<Utc>>()
                    .map(Prop::DTime)
                    .map_err(|_| Error),
                PropType::Decimal { scale } => v
                    .parse::<BigDecimal>()
                    .map(|v| Prop::Decimal(v.with_scale(scale)))
                    .map_err(|_| Error),
                _ => Err(Error),
            },
            Prop::Bool(v) => match prop_type {
                PropType::Str => Ok(Prop::Str(v.to_string().into())),
                PropType::U8 => Ok(Prop::U8(if v { 1 } else { 0 })),
                PropType::U16 => Ok(Prop::U16(if v { 1 } else { 0 })),
                PropType::I32 => Ok(Prop::I32(if v { 1 } else { 0 })),
                PropType::I64 => Ok(Prop::I64(if v { 1 } else { 0 })),
                PropType::U32 => Ok(Prop::U32(if v { 1 } else { 0 })),
                PropType::U64 => Ok(Prop::U64(if v { 1 } else { 0 })),
                PropType::F32 => Ok(Prop::F32(if v { 1.0 } else { 0.0 })),
                PropType::F64 => Ok(Prop::F64(if v { 1.0 } else { 0.0 })),
                PropType::Bool => unreachable!("Same type case handled above"),
                PropType::Decimal { scale } => {
                    let val = if v {
                        BigDecimal::from(1)
                    } else {
                        BigDecimal::from(0)
                    };
                    Ok(Prop::Decimal(val.with_scale(scale)))
                }
                _ => Err(Error),
            },
            Prop::List(_v) => Err(Error),
            Prop::Map(_v) => Err(Error),
            Prop::NDTime(v) => match prop_type {
                PropType::Str => Ok(Prop::Str(v.to_string().into())),
                PropType::I64 => Ok(Prop::I64(v.and_utc().timestamp())),
                PropType::U64 => {
                    let ts = v.and_utc().timestamp();
                    if ts >= 0 {
                        Ok(Prop::U64(ts as u64))
                    } else {
                        Err(Error)
                    }
                }
                PropType::DTime => Ok(Prop::DTime(v.and_utc())),
                PropType::NDTime => unreachable!("Same type case handled above"),
                _ => Err(Error),
            },
            Prop::DTime(v) => match prop_type {
                PropType::Str => Ok(Prop::Str(v.to_rfc3339().into())),
                PropType::I64 => Ok(Prop::I64(v.timestamp())),
                PropType::U64 => {
                    let ts = v.timestamp();
                    if ts >= 0 {
                        Ok(Prop::U64(ts as u64))
                    } else {
                        Err(Error)
                    }
                }
                PropType::NDTime => Ok(Prop::NDTime(v.naive_utc())),
                PropType::DTime => unreachable!("Same type case handled above"),
                _ => Err(Error),
            },
            #[cfg(feature = "arrow")]
            Prop::Array(_v) => Err(Error),
            Prop::Decimal(v) => match prop_type {
                PropType::Str => Ok(Prop::Str(v.to_string().into())),
                PropType::U8 => {
                    let as_i64 = v.to_i64().ok_or(Error)?;
                    u8::from_i64(as_i64).ok_or(Error).map(Prop::U8)
                }
                PropType::U16 => {
                    let as_i64 = v.to_i64().ok_or(Error)?;
                    u16::from_i64(as_i64).ok_or(Error).map(Prop::U16)
                }
                PropType::I32 => {
                    let as_i64 = v.to_i64().ok_or(Error)?;
                    i32::from_i64(as_i64).ok_or(Error).map(Prop::I32)
                }
                PropType::I64 => v.to_i64().ok_or(Error).map(Prop::I64),
                PropType::U32 => {
                    let as_i64 = v.to_i64().ok_or(Error)?;
                    u32::from_i64(as_i64).ok_or(Error).map(Prop::U32)
                }
                PropType::U64 => {
                    let as_i64 = v.to_i64().ok_or(Error)?;
                    u64::from_i64(as_i64).ok_or(Error).map(Prop::U64)
                }
                PropType::F32 => v.to_f32().ok_or(Error).map(Prop::F32),
                PropType::F64 => v.to_f64().ok_or(Error).map(Prop::F64),
                PropType::Bool => {
                    let as_i64 = v.to_i64().ok_or(Error)?;
                    Ok(Prop::Bool(as_i64 != 0))
                }
                PropType::Decimal { scale } => Ok(Prop::Decimal(v.with_scale(scale))),
                _ => Err(Error),
            },
            _ => match prop_type {
                // Numeric conversions using num_traits
                PropType::U8 => self.try_into_int::<u8>().map(Prop::U8),
                PropType::U16 => self.try_into_int::<u16>().map(Prop::U16),
                PropType::I32 => self.try_into_int::<i32>().map(Prop::I32),
                PropType::I64 => self.try_into_int::<i64>().map(Prop::I64),
                PropType::U32 => self.try_into_int::<u32>().map(Prop::U32),
                PropType::U64 => self.try_into_int::<u64>().map(Prop::U64),
                PropType::F32 => self.try_into_f32(),
                PropType::F64 => self.into_f64().map(Prop::F64),
                _ => Err(Error),
            },
        }
    }

    pub fn try_from_bd(bd: BigDecimal) -> Result<Prop, InvalidBigDecimal> {
        let prop = Prop::Decimal(bd);
        validate_prop(prop)
    }

    pub fn map(vals: impl IntoIterator<Item = (impl Into<ArcStr>, impl Into<Prop>)>) -> Self {
        let h_map: FxHashMap<_, _> = vals
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Prop::Map(h_map.into())
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
            Prop::List(list) => {
                let list_type = list
                    .iter()
                    .map(|p| Ok(p.dtype()))
                    .reduce(|a, b| unify_types(&a?, &b?, &mut false))
                    .transpose()
                    .map(|e| e.unwrap_or(PropType::Empty))
                    .unwrap_or_else(|e| panic!("Cannot unify types for list {:?}: {e:?}", list));
                PropType::List(Box::new(list_type))
            }
            Prop::Map(map) => PropType::map(map.iter().map(|(k, v)| (k, v.dtype()))),
            Prop::NDTime(_) => PropType::NDTime,
            #[cfg(feature = "arrow")]
            Prop::Array(arr) => {
                let arrow_dtype = arr
                    .as_array_ref()
                    .expect("Should not call dtype on empty PropArray")
                    .data_type();
                PropType::Array(Box::new(prop_type_from_arrow_dtype(arrow_dtype)))
            }
            Prop::DTime(_) => PropType::DTime,
            Prop::Decimal(d) => PropType::Decimal {
                scale: d.as_bigint_and_scale().1,
            },
        }
    }

    pub fn str<S: Into<ArcStr>>(s: S) -> Prop {
        Prop::Str(s.into())
    }

    pub fn add(self, other: Prop) -> Option<Prop> {
        match (self, other) {
            (Prop::U8(a), Prop::U8(b)) => Some(Prop::U8(a + b)),
            (Prop::U16(a), Prop::U16(b)) => Some(Prop::U16(a + b)),
            (Prop::I32(a), Prop::I32(b)) => Some(Prop::I32(a + b)),
            (Prop::I64(a), Prop::I64(b)) => Some(Prop::I64(a + b)),
            (Prop::U32(a), Prop::U32(b)) => Some(Prop::U32(a + b)),
            (Prop::U64(a), Prop::U64(b)) => Some(Prop::U64(a + b)),
            (Prop::F32(a), Prop::F32(b)) => Some(Prop::F32(a + b)),
            (Prop::F64(a), Prop::F64(b)) => Some(Prop::F64(a + b)),
            (Prop::Str(a), Prop::Str(b)) => Some(Prop::Str((a.to_string() + b.as_ref()).into())),
            (Prop::Decimal(a), Prop::Decimal(b)) => Some(Prop::Decimal(a + b)),
            _ => None,
        }
    }

    pub fn min(self, other: Prop) -> Option<Prop> {
        self.partial_cmp(&other).map(|ord| match ord {
            Ordering::Less => self,
            Ordering::Equal => self,
            Ordering::Greater => other,
        })
    }

    pub fn max(self, other: Prop) -> Option<Prop> {
        self.partial_cmp(&other).map(|ord| match ord {
            Ordering::Less => other,
            Ordering::Equal => self,
            Ordering::Greater => self,
        })
    }

    pub fn divide(self, other: Prop) -> Option<Prop> {
        match (self, other) {
            (Prop::U8(a), Prop::U8(b)) if b != 0 => Some(Prop::U8(a / b)),
            (Prop::U16(a), Prop::U16(b)) if b != 0 => Some(Prop::U16(a / b)),
            (Prop::I32(a), Prop::I32(b)) if b != 0 => Some(Prop::I32(a / b)),
            (Prop::I64(a), Prop::I64(b)) if b != 0 => Some(Prop::I64(a / b)),
            (Prop::U32(a), Prop::U32(b)) if b != 0 => Some(Prop::U32(a / b)),
            (Prop::U64(a), Prop::U64(b)) if b != 0 => Some(Prop::U64(a / b)),
            (Prop::F32(a), Prop::F32(b)) => Some(Prop::F32(a / b)),
            (Prop::F64(a), Prop::F64(b)) => Some(Prop::F64(a / b)),
            (Prop::Decimal(a), Prop::Decimal(b)) if b != BigDecimal::from(0) => {
                Some(Prop::Decimal(a / b))
            }
            _ => None,
        }
    }
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
            #[cfg(feature = "arrow")]
            Prop::Array(value) => write!(f, "{:?}", value),
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

impl From<ArcStr> for Prop {
    fn from(value: ArcStr) -> Self {
        Prop::Str(value)
    }
}

impl From<&ArcStr> for Prop {
    fn from(value: &ArcStr) -> Self {
        Prop::Str(value.clone())
    }
}

impl From<String> for Prop {
    fn from(value: String) -> Self {
        Prop::Str(value.into())
    }
}

impl From<&String> for Prop {
    fn from(s: &String) -> Self {
        Prop::Str(s.as_str().into())
    }
}

impl From<Arc<str>> for Prop {
    fn from(s: Arc<str>) -> Self {
        Prop::Str(s.into())
    }
}

impl From<&Arc<str>> for Prop {
    fn from(value: &Arc<str>) -> Self {
        Prop::Str(value.clone().into())
    }
}

impl From<&str> for Prop {
    fn from(s: &str) -> Self {
        Prop::Str(s.to_owned().into())
    }
}

impl From<i32> for Prop {
    fn from(i: i32) -> Self {
        Prop::I32(i)
    }
}

impl From<u8> for Prop {
    fn from(i: u8) -> Self {
        Prop::U8(i)
    }
}

impl From<u16> for Prop {
    fn from(i: u16) -> Self {
        Prop::U16(i)
    }
}

impl From<i64> for Prop {
    fn from(i: i64) -> Self {
        Prop::I64(i)
    }
}

impl From<BigDecimal> for Prop {
    fn from(d: BigDecimal) -> Self {
        Prop::Decimal(d)
    }
}

impl From<u32> for Prop {
    fn from(u: u32) -> Self {
        Prop::U32(u)
    }
}

impl From<u64> for Prop {
    fn from(u: u64) -> Self {
        Prop::U64(u)
    }
}

impl From<f32> for Prop {
    fn from(f: f32) -> Self {
        Prop::F32(f)
    }
}

impl From<f64> for Prop {
    fn from(f: f64) -> Self {
        Prop::F64(f)
    }
}

impl From<DateTime<Utc>> for Prop {
    fn from(f: DateTime<Utc>) -> Self {
        Prop::DTime(f)
    }
}

impl From<bool> for Prop {
    fn from(b: bool) -> Self {
        Prop::Bool(b)
    }
}

impl From<HashMap<ArcStr, Prop>> for Prop {
    fn from(value: HashMap<ArcStr, Prop>) -> Self {
        Prop::Map(Arc::new(value.into_iter().collect()))
    }
}

impl From<FxHashMap<ArcStr, Prop>> for Prop {
    fn from(value: FxHashMap<ArcStr, Prop>) -> Self {
        Prop::Map(Arc::new(value))
    }
}

impl From<Vec<Prop>> for Prop {
    fn from(value: Vec<Prop>) -> Self {
        Prop::List(Arc::new(value))
    }
}

impl From<&Prop> for Prop {
    fn from(value: &Prop) -> Self {
        value.clone()
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
        Prop::List(Arc::new(self.into_iter().map(|v| v.into()).collect()))
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
