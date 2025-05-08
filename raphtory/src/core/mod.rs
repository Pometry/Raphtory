//! # raphtory
//!
//! `raphtory` is the core module for the raphtory library.
//!
//! The raphtory library is a temporal graph analytics tool, which allows users to create
//! and analyze graph data with time.
//!
//! This crate provides the core data structures and functions for working with temporal graphs,
//! as well as building and evaluating algorithms.
//!
//! **Note** this module is not meant to be used as a standalone crate, but in conjunction with the
//! raphtory_db crate.
//!
//! For example code, please see the raphtory_db crate.
//!
//! ## Supported Platforms
//!
//! `raphtory` supports  support for the following platforms:
//!
//! **Note** they must have Rust 1.83 or later.
//!
//!    * `Linux`
//!    * `Windows`
//!    * `macOS`
//!

use arrow_array::{ArrayRef, ArrowPrimitiveType};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use entities::properties::props::validate_prop;
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};
use storage::lazy_vec::IllegalSet;
use utils::errors::GraphError;

use arrow_schema::{DataType, Field};
use rustc_hash::FxHashMap;

#[cfg(test)]
extern crate core;

pub mod entities;
pub mod prop_array;
pub mod state;
pub mod storage;
pub mod utils;

use crate::core::prop_array::PropArray;
pub use raphtory_api::core::*;

/// Denotes the types of properties allowed to be stored in the graph.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    List(Arc<Vec<Prop>>),
    Map(Arc<FxHashMap<ArcStr, Prop>>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Array(PropArray),
    Decimal(BigDecimal),
}

pub const DECIMAL_MAX: i128 = 99999999999999999999999999999999999999i128; // equivalent to parquet decimal(38, 0)

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

impl Prop {
    pub fn try_from_bd(bd: BigDecimal) -> Result<Prop, IllegalSet<Option<Prop>>> {
        let prop = Prop::Decimal(bd);
        validate_prop(0, prop)
    }

    pub fn map(vals: impl IntoIterator<Item = (impl Into<ArcStr>, impl Into<Prop>)>) -> Self {
        let h_map: FxHashMap<_, _> = vals
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Prop::Map(h_map.into())
    }

    pub fn from_arr<TT: ArrowPrimitiveType>(vals: Vec<TT::Native>) -> Self
    where
        arrow_array::PrimitiveArray<TT>: From<Vec<TT::Native>>,
    {
        let array = arrow_array::PrimitiveArray::<TT>::from(vals);
        Prop::Array(PropArray::Array(Arc::new(array)))
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
                    .expect(&format!("Cannot unify types for list {:?}", list));
                PropType::List(Box::new(list_type))
            }
            Prop::Map(map) => PropType::Map(
                map.iter()
                    .map(|(k, prop)| (k.to_string(), prop.dtype()))
                    .sorted_by(|(k1, _), (k2, _)| k1.cmp(k2))
                    .collect(),
            ),
            Prop::NDTime(_) => PropType::NDTime,
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

pub fn arrow_dtype_from_prop_type(prop_type: &PropType) -> Result<DataType, GraphError> {
    match prop_type {
        PropType::Str => Ok(DataType::LargeUtf8),
        PropType::U8 => Ok(DataType::UInt8),
        PropType::U16 => Ok(DataType::UInt16),
        PropType::I32 => Ok(DataType::Int32),
        PropType::I64 => Ok(DataType::Int64),
        PropType::U32 => Ok(DataType::UInt32),
        PropType::U64 => Ok(DataType::UInt64),
        PropType::F32 => Ok(DataType::Float32),
        PropType::F64 => Ok(DataType::Float64),
        PropType::Bool => Ok(DataType::Boolean),
        PropType::NDTime => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            None,
        )),
        PropType::DTime => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            Some("UTC".into()),
        )),
        PropType::Array(d_type) => Ok(DataType::List(
            Field::new("data", arrow_dtype_from_prop_type(&d_type)?, true).into(),
        )),

        PropType::List(d_type) => Ok(DataType::List(
            Field::new("data", arrow_dtype_from_prop_type(&d_type)?, true).into(),
        )),
        PropType::Map(d_type) => {
            let fields = d_type
                .iter()
                .map(|(k, v)| {
                    Ok::<_, GraphError>(Field::new(
                        k.to_string(),
                        arrow_dtype_from_prop_type(v)?,
                        true,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        // 38 comes from herehttps://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html
        PropType::Decimal { scale } => Ok(DataType::Decimal128(38, (*scale).try_into().unwrap())),
        PropType::Empty => {
            // this is odd, we'll just pick one and hope for the best
            Ok(DataType::Null)
        }
    }
}

pub fn prop_type_from_arrow_dtype(arrow_dtype: &DataType) -> PropType {
    match arrow_dtype {
        DataType::LargeUtf8 | DataType::Utf8 => PropType::Str,
        DataType::UInt8 => PropType::U8,
        DataType::UInt16 => PropType::U16,
        DataType::Int32 => PropType::I32,
        DataType::Int64 => PropType::I64,
        DataType::UInt32 => PropType::U32,
        DataType::UInt64 => PropType::U64,
        DataType::Float32 => PropType::F32,
        DataType::Float64 => PropType::F64,
        DataType::Boolean => PropType::Bool,
        DataType::Decimal128(_, scale) => PropType::Decimal {
            scale: *scale as i64,
        },
        DataType::List(field) => {
            let d_type = field.data_type();
            PropType::Array(Box::new(prop_type_from_arrow_dtype(&d_type)))
        }
        _ => panic!("{:?} not supported as disk_graph property", arrow_dtype),
    }
}

pub trait PropUnwrap: Sized {
    fn into_u8(self) -> Option<u8>;
    fn unwrap_u8(self) -> u8 {
        self.into_u8().unwrap()
    }

    fn into_u16(self) -> Option<u16>;
    fn unwrap_u16(self) -> u16 {
        self.into_u16().unwrap()
    }

    fn into_str(self) -> Option<ArcStr>;
    fn unwrap_str(self) -> ArcStr {
        self.into_str().unwrap()
    }

    fn into_i32(self) -> Option<i32>;
    fn unwrap_i32(self) -> i32 {
        self.into_i32().unwrap()
    }

    fn into_i64(self) -> Option<i64>;
    fn unwrap_i64(self) -> i64 {
        self.into_i64().unwrap()
    }

    fn into_u32(self) -> Option<u32>;
    fn unwrap_u32(self) -> u32 {
        self.into_u32().unwrap()
    }

    fn into_u64(self) -> Option<u64>;
    fn unwrap_u64(self) -> u64 {
        self.into_u64().unwrap()
    }

    fn into_f32(self) -> Option<f32>;
    fn unwrap_f32(self) -> f32 {
        self.into_f32().unwrap()
    }

    fn into_f64(self) -> Option<f64>;
    fn unwrap_f64(self) -> f64 {
        self.into_f64().unwrap()
    }

    fn into_bool(self) -> Option<bool>;
    fn unwrap_bool(self) -> bool {
        self.into_bool().unwrap()
    }

    fn into_list(self) -> Option<Arc<Vec<Prop>>>;
    fn unwrap_list(self) -> Arc<Vec<Prop>> {
        self.into_list().unwrap()
    }

    fn into_map(self) -> Option<Arc<FxHashMap<ArcStr, Prop>>>;
    fn unwrap_map(self) -> Arc<FxHashMap<ArcStr, Prop>> {
        self.into_map().unwrap()
    }

    fn into_ndtime(self) -> Option<NaiveDateTime>;
    fn unwrap_ndtime(self) -> NaiveDateTime {
        self.into_ndtime().unwrap()
    }

    fn into_array(self) -> Option<ArrayRef>;
    fn unwrap_array(self) -> ArrayRef {
        self.into_array().unwrap()
    }

    fn as_f64(&self) -> Option<f64>;

    fn into_decimal(self) -> Option<BigDecimal>;
}

impl<P: PropUnwrap> PropUnwrap for Option<P> {
    fn into_u8(self) -> Option<u8> {
        self.and_then(|p| p.into_u8())
    }

    fn into_u16(self) -> Option<u16> {
        self.and_then(|p| p.into_u16())
    }

    fn into_str(self) -> Option<ArcStr> {
        self.and_then(|p| p.into_str())
    }

    fn into_i32(self) -> Option<i32> {
        self.and_then(|p| p.into_i32())
    }

    fn into_i64(self) -> Option<i64> {
        self.and_then(|p| p.into_i64())
    }

    fn into_u32(self) -> Option<u32> {
        self.and_then(|p| p.into_u32())
    }

    fn into_u64(self) -> Option<u64> {
        self.and_then(|p| p.into_u64())
    }

    fn into_f32(self) -> Option<f32> {
        self.and_then(|p| p.into_f32())
    }

    fn into_f64(self) -> Option<f64> {
        self.and_then(|p| p.into_f64())
    }

    fn into_bool(self) -> Option<bool> {
        self.and_then(|p| p.into_bool())
    }

    fn into_list(self) -> Option<Arc<Vec<Prop>>> {
        self.and_then(|p| p.into_list())
    }

    fn into_map(self) -> Option<Arc<FxHashMap<ArcStr, Prop>>> {
        self.and_then(|p| p.into_map())
    }

    fn into_ndtime(self) -> Option<NaiveDateTime> {
        self.and_then(|p| p.into_ndtime())
    }

    fn into_array(self) -> Option<ArrayRef> {
        self.and_then(|p| p.into_array())
    }

    fn as_f64(&self) -> Option<f64> {
        self.as_ref().and_then(|p| p.as_f64())
    }

    fn into_decimal(self) -> Option<BigDecimal> {
        self.and_then(|p| p.into_decimal())
    }
}

impl PropUnwrap for Prop {
    fn into_u8(self) -> Option<u8> {
        if let Prop::U8(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn into_u16(self) -> Option<u16> {
        if let Prop::U16(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn into_str(self) -> Option<ArcStr> {
        if let Prop::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn into_i32(self) -> Option<i32> {
        if let Prop::I32(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_i64(self) -> Option<i64> {
        if let Prop::I64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_u32(self) -> Option<u32> {
        if let Prop::U32(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_u64(self) -> Option<u64> {
        if let Prop::U64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_f32(self) -> Option<f32> {
        if let Prop::F32(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_f64(self) -> Option<f64> {
        if let Prop::F64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_bool(self) -> Option<bool> {
        if let Prop::Bool(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_list(self) -> Option<Arc<Vec<Prop>>> {
        if let Prop::List(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_map(self) -> Option<Arc<FxHashMap<ArcStr, Prop>>> {
        if let Prop::Map(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_ndtime(self) -> Option<NaiveDateTime> {
        if let Prop::NDTime(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn into_array(self) -> Option<ArrayRef> {
        if let Prop::Array(v) = self {
            v.into_array_ref()
        } else {
            None
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Prop::U8(v) => Some(*v as f64),
            Prop::U16(v) => Some(*v as f64),
            Prop::I32(v) => Some(*v as f64),
            Prop::I64(v) => Some(*v as f64),
            Prop::U32(v) => Some(*v as f64),
            Prop::U64(v) => Some(*v as f64),
            Prop::F32(v) => Some(*v as f64),
            Prop::F64(v) => Some(*v),
            _ => None,
        }
    }

    fn into_decimal(self) -> Option<BigDecimal> {
        if let Prop::Decimal(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

impl Display for Prop {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Prop::Str(value) => write!(f, "{}", value.0.to_string()),
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

impl From<Prop> for Value {
    fn from(prop: Prop) -> Self {
        match prop {
            Prop::Str(value) => Value::String(value.to_string()),
            Prop::U8(value) => Value::Number(value.into()),
            Prop::U16(value) => Value::Number(value.into()),
            Prop::I32(value) => Value::Number(value.into()),
            Prop::I64(value) => Value::Number(value.into()),
            Prop::U32(value) => Value::Number(value.into()),
            Prop::U64(value) => Value::Number(value.into()),
            Prop::F32(value) => serde_json::Number::from_f64(value as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Prop::F64(value) => serde_json::Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Prop::Bool(value) => Value::Bool(value),
            Prop::List(values) => Value::Array(values.iter().cloned().map(Value::from).collect()),
            Prop::Map(map) => {
                let json_map: serde_json::Map<String, Value> = map
                    .iter()
                    .map(|(k, v)| (k.to_string(), Value::from(v.clone())))
                    .collect();
                Value::Object(json_map)
            }
            Prop::NDTime(value) => Value::String(value.to_string()),
            Prop::DTime(value) => Value::String(value.to_string()),
            _ => Value::Null,
        }
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

#[cfg(feature = "io")]
mod serde_value_into_prop {
    use std::collections::HashMap;

    use super::{IntoPropMap, Prop};
    use serde_json::Value;

    impl TryFrom<Value> for Prop {
        type Error = String;

        fn try_from(value: Value) -> Result<Self, Self::Error> {
            match value {
                Value::Null => Err("Null property not valid".to_string()),
                Value::Bool(value) => Ok(value.into()),
                Value::Number(value) => value
                    .as_i64()
                    .map(|num| num.into())
                    .or_else(|| value.as_f64().map(|num| num.into()))
                    .ok_or(format!("Number conversion error for: {}", value)),
                Value::String(value) => Ok(value.into()),
                Value::Array(value) => value
                    .into_iter()
                    .map(|item| item.try_into())
                    .collect::<Result<Vec<Prop>, Self::Error>>()
                    .map(|item| item.into()),
                Value::Object(value) => value
                    .into_iter()
                    .map(|(key, value)| {
                        let prop = value.try_into()?;
                        Ok((key, prop))
                    })
                    .collect::<Result<HashMap<String, Prop>, Self::Error>>()
                    .map(|item| item.into_prop_map()),
            }
        }
    }
}
