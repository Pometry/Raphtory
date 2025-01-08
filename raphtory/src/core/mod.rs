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
//! **Note** they must have Rust 1.82 or later.
//!
//!    * `Linux`
//!    * `Windows`
//!    * `macOS`
//!

use arrow_array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray, RecordBatch};
use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{json, Value};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};
use utils::errors::GraphError;

use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use base64::{prelude::BASE64_STANDARD, Engine};

#[cfg(test)]
extern crate core;

pub mod entities;
pub mod state;
pub mod storage;
pub mod utils;

pub use raphtory_api::core::*;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Hash, Default)]
pub enum Lifespan {
    Interval {
        start: i64,
        end: i64,
    },
    Event {
        time: i64,
    },
    #[default]
    Inherited,
}

/// struct containing all the necessary information to allow Raphtory creating a document and
/// storing it
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash, Default)]
pub struct DocumentInput {
    pub content: String,
    pub life: Lifespan,
}

impl Display for DocumentInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.content)
    }
}

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
    Map(Arc<HashMap<ArcStr, Prop>>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Array(PropArray),
    Document(DocumentInput),
}

#[derive(Default, Debug, Clone)]
pub enum PropArray {
    #[default]
    Empty,
    Array(ArrayRef),
}

impl Hash for PropArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // FIXME check this is sane
        if let PropArray::Array(array) = self {
            let data = array.to_data();
            let dtype = array.data_type();
            dtype.hash(state);
            data.offset().hash(state);
            data.len().hash(state);
            for buffer in data.buffers() {
                buffer.hash(state);
            }
        } else {
            PropArray::Empty.hash(state);
        }
    }
}

impl PropArray {
    pub fn to_vec_u8(&self) -> Vec<u8> {
        // assuming we can allocate this can't fail
        let mut bytes = vec![];
        if let PropArray::Array(value) = self {
            let schema = Schema::new(vec![Field::new("data", value.data_type().clone(), true)]);
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            let rb = RecordBatch::try_new(schema.into(), vec![value.clone()]).unwrap();
            writer.write(&rb).unwrap();
            writer.finish().unwrap();
        }
        bytes
    }

    pub fn from_vec_u8(bytes: &[u8]) -> Result<Self, GraphError> {
        if bytes.is_empty() {
            return Ok(PropArray::Empty);
        }
        let mut reader = StreamReader::try_new(bytes, None)?;
        let rb = reader.next().ok_or(GraphError::DeserialisationError(
            "failed to deserialize ArrayRef".to_string(),
        ))??;
        Ok(PropArray::Array(rb.column(0).clone()))
    }

    pub fn into_array_ref(self) -> Option<ArrayRef> {
        match self {
            PropArray::Array(arr) => Some(arr),
            _ => None,
        }
    }

    // pub fn iter_prop(&self) -> Option<BoxedLIter<Prop>> {
    //     self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::Int32Type>>().map(|arr| {
    //         arr.into_iter().map(|v| Prop::I32(v.unwrap_or_default())).into_dyn_boxed()
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::Float64Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::F64(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::Float32Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::F32(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::UInt64Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::U64(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::UInt32Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::U32(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::Int64Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::I64(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::UInt16Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::U16(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     }).or_else(|| {
    //         self.0.as_any().downcast_ref::<PrimitiveArray<arrow_array::types::UInt8Type>>().map(|arr| {
    //             arr.into_iter().map(|v| Prop::U8(v.unwrap_or_default())).into_dyn_boxed()
    //         })
    //     })
    // }
}

impl Serialize for PropArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_vec_u8();
        serializer.serialize_str(&BASE64_STANDARD.encode(&bytes))
    }
}

impl<'de> Deserialize<'de> for PropArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let base64_str = String::deserialize(deserializer)?;
        let bytes = BASE64_STANDARD
            .decode(&base64_str)
            .map_err(serde::de::Error::custom)?;
        PropArray::from_vec_u8(&bytes).map_err(serde::de::Error::custom)
    }
}

impl PartialEq for PropArray {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PropArray::Empty, PropArray::Empty) => true,
            (PropArray::Array(a), PropArray::Array(b)) => a.eq(b),
            _ => false,
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
            Prop::Document(d) => d.hash(state),
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
            _ => None,
        }
    }
}

impl Prop {
    pub fn from_arr<T: ArrowNativeType, TT: ArrowPrimitiveType<Native = T>>(vals: Vec<T>) -> Self {
        let buf: ScalarBuffer<T> = vals.into();
        let arr: PrimitiveArray<TT> = PrimitiveArray::new(buf, None);
        let arr: ArrayRef = Arc::new(arr);
        Prop::Array(PropArray::Array(arr))
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
            Prop::List(_) => PropType::List,
            Prop::Map(_) => PropType::Map,
            Prop::NDTime(_) => PropType::NDTime,
            Prop::Array(arr) => PropType::Array(Box::new(PropType::U8)), // TODO
            Prop::Document(_) => PropType::Document,
            Prop::DTime(_) => PropType::DTime,
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
            (Prop::F32(a), Prop::F32(b)) if b != 0.0 => Some(Prop::F32(a / b)),
            (Prop::F64(a), Prop::F64(b)) if b != 0.0 => Some(Prop::F64(a / b)),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
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

    fn into_map(self) -> Option<Arc<HashMap<ArcStr, Prop>>>;
    fn unwrap_map(self) -> Arc<HashMap<ArcStr, Prop>> {
        self.into_map().unwrap()
    }

    fn into_ndtime(self) -> Option<NaiveDateTime>;
    fn unwrap_ndtime(self) -> NaiveDateTime {
        self.into_ndtime().unwrap()
    }

    fn into_document(self) -> Option<DocumentInput>;
    fn unwrap_document(self) -> DocumentInput {
        self.into_document().unwrap()
    }

    fn into_blob(self) -> Option<ArrayRef>;
    fn unwrap_blob(self) -> ArrayRef {
        self.into_blob().unwrap()
    }
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

    fn into_map(self) -> Option<Arc<HashMap<ArcStr, Prop>>> {
        self.and_then(|p| p.into_map())
    }

    fn into_ndtime(self) -> Option<NaiveDateTime> {
        self.and_then(|p| p.into_ndtime())
    }

    fn into_document(self) -> Option<DocumentInput> {
        self.and_then(|p| p.into_document())
    }

    fn into_blob(self) -> Option<ArrayRef> {
        self.and_then(|p| p.into_blob())
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

    fn into_map(self) -> Option<Arc<HashMap<ArcStr, Prop>>> {
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

    fn into_document(self) -> Option<DocumentInput> {
        if let Prop::Document(d) = self {
            Some(d)
        } else {
            None
        }
    }

    fn into_blob(self) -> Option<ArrayRef> {
        if let Prop::Array(v) = self {
            v.into_array_ref()
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
            Prop::Document(value) => write!(f, "{}", value),
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
            Prop::Document(doc) => json!({
                "content": doc.content,
                "life": Value::from(doc.life),
            }),
            _ => Value::Null,
        }
    }
}

impl From<Lifespan> for Value {
    fn from(lifespan: Lifespan) -> Self {
        match lifespan {
            Lifespan::Interval { start, end } => json!({ "start": start, "end": end }),
            Lifespan::Event { time } => json!({ "time": time }),
            Lifespan::Inherited => Value::String("inherited".to_string()),
        }
    }
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
