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

use crate::{
    db::graph::{graph::Graph, views::deletion_graph::PersistentGraph},
    prelude::GraphViewOps,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

#[cfg(test)]
extern crate core;

pub mod entities;
pub mod state;
pub mod storage;
pub mod utils;

pub use raphtory_api::core::*;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub enum Lifespan {
    Interval { start: i64, end: i64 },
    Event { time: i64 },
    Inherited,
}

/// struct containing all the necessary information to allow Raphtory creating a document and
/// storing it
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
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
    Graph(Graph),
    PersistentGraph(PersistentGraph),
    Document(DocumentInput),
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
                for (key, prop) in m.iter() {
                    key.hash(state);
                    prop.hash(state);
                }
            }
            Prop::Graph(g) => {
                for node in g.nodes() {
                    node.node.hash(state);
                }
                for edge in g.edges() {
                    edge.edge.pid().hash(state);
                }
            }
            Prop::PersistentGraph(pg) => {
                for node in pg.nodes() {
                    node.node.hash(state);
                }
                for edge in pg.edges() {
                    edge.edge.pid().hash(state);
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
            _ => None,
        }
    }
}

impl Prop {
    pub fn to_json(&self) -> Value {
        match self {
            Prop::Str(value) => Value::String(value.to_string()),
            Prop::U8(value) => Value::Number((*value).into()),
            Prop::U16(value) => Value::Number((*value).into()),
            Prop::I32(value) => Value::Number((*value).into()),
            Prop::I64(value) => Value::Number((*value).into()),
            Prop::U32(value) => Value::Number((*value).into()),
            Prop::U64(value) => Value::Number((*value).into()),
            Prop::F32(value) => Value::Number(serde_json::Number::from_f64(*value as f64).unwrap()),
            Prop::F64(value) => Value::Number(serde_json::Number::from_f64(*value).unwrap()),
            Prop::Bool(value) => Value::Bool(*value),
            Prop::List(value) => {
                let vec: Vec<Value> = value.iter().map(|v| v.to_json()).collect();
                Value::Array(vec)
            }
            Prop::Map(value) => {
                let map: serde_json::Map<String, Value> = value
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_json()))
                    .collect();
                Value::Object(map)
            }
            Prop::DTime(value) => Value::String(value.to_string()),
            Prop::NDTime(value) => Value::String(value.to_string()),
            Prop::Graph(_) => Value::String("Graph cannot be converted to JSON".to_string()),
            Prop::PersistentGraph(_) => {
                Value::String("Persistent Graph cannot be converted to JSON".to_string())
            }
            Prop::Document(DocumentInput { content, .. }) => Value::String(content.to_owned()), // TODO: return Value::Object ??
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
            Prop::List(_) => PropType::List,
            Prop::Map(_) => PropType::Map,
            Prop::NDTime(_) => PropType::NDTime,
            Prop::Graph(_) => PropType::Graph,
            Prop::PersistentGraph(_) => PropType::PersistentGraph,
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

    fn into_graph(self) -> Option<Graph>;

    fn into_persistent_graph(self) -> Option<PersistentGraph>;

    fn unwrap_graph(self) -> Graph {
        self.into_graph().unwrap()
    }

    fn into_document(self) -> Option<DocumentInput>;
    fn unwrap_document(self) -> DocumentInput {
        self.into_document().unwrap()
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

    fn into_graph(self) -> Option<Graph> {
        self.and_then(|p| p.into_graph())
    }

    fn into_persistent_graph(self) -> Option<PersistentGraph> {
        self.and_then(|p| p.into_persistent_graph())
    }

    fn into_document(self) -> Option<DocumentInput> {
        self.and_then(|p| p.into_document())
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

    fn into_graph(self) -> Option<Graph> {
        if let Prop::Graph(g) = self {
            Some(g)
        } else {
            None
        }
    }

    fn into_persistent_graph(self) -> Option<PersistentGraph> {
        if let Prop::PersistentGraph(g) = self {
            Some(g)
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
            Prop::Graph(value) => write!(
                f,
                "Graph(num_nodes={}, num_edges={})",
                value.count_nodes(),
                value.count_edges()
            ),
            Prop::PersistentGraph(value) => write!(
                f,
                "Graph(num_nodes={}, num_edges={})",
                value.count_nodes(),
                value.count_edges()
            ),
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

// From impl for Prop

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
