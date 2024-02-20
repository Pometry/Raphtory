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
//! **Note** they must have Rust 1.53 or later.
//!
//!    * `Linux`
//!    * `Windows`
//!    * `macOS`
//!

use crate::{db::graph::graph::Graph, prelude::GraphViewOps};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
};

#[cfg(test)]
extern crate core;

pub mod entities;
pub mod state;
pub(crate) mod storage;
pub mod utils;

// this is here because Arc<str> annoyingly doesn't implement all the expected comparisons
#[derive(Clone, Debug, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct ArcStr(pub(crate) Arc<str>);

impl Display for ArcStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<T: Into<Arc<str>>> From<T> for ArcStr {
    fn from(value: T) -> Self {
        ArcStr(value.into())
    }
}

impl From<ArcStr> for String {
    fn from(value: ArcStr) -> Self {
        value.to_string()
    }
}
impl Deref for ArcStr {
    type Target = Arc<str>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for ArcStr {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl<T> AsRef<T> for ArcStr
where
    T: ?Sized,
    <ArcStr as Deref>::Target: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.deref().as_ref()
    }
}

impl<T: Borrow<str> + ?Sized> PartialEq<T> for ArcStr {
    fn eq(&self, other: &T) -> bool {
        <ArcStr as Borrow<str>>::borrow(self).eq(other.borrow())
    }
}

impl<T: Borrow<str>> PartialOrd<T> for ArcStr {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        <ArcStr as Borrow<str>>::borrow(self).partial_cmp(other.borrow())
    }
}

pub trait OptionAsStr<'a> {
    fn as_str(self) -> Option<&'a str>;
}

impl<'a, O: AsRef<str> + 'a> OptionAsStr<'a> for &'a Option<O> {
    fn as_str(self) -> Option<&'a str> {
        self.as_ref().map(|s| s.as_ref())
    }
}

impl<'a, O: AsRef<str> + 'a> OptionAsStr<'a> for Option<&'a O> {
    fn as_str(self) -> Option<&'a str> {
        self.map(|s| s.as_ref())
    }
}

/// Denotes the direction of an edge. Can be incoming, outgoing or both.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum Lifespan {
    Interval { start: i64, end: i64 },
    Event { time: i64 },
    Inherited,
}

/// struct containing all the necessary information to allow Raphtory creating a document and
/// storing it
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DocumentInput {
    pub content: String,
    pub life: Lifespan,
}

impl Display for DocumentInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.content)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub enum PropType {
    #[default]
    Empty,
    Str,
    U8,
    U16,
    I32,
    I64,
    U32,
    U64,
    F32,
    F64,
    Bool,
    List,
    Map,
    DTime,
    Graph,
    Document,
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
    DTime(NaiveDateTime),
    Graph(Graph),
    Document(DocumentInput),
}

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
            Prop::Graph(_) => Value::String("Graph cannot be converted to JSON".to_string()),
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
            Prop::DTime(_) => PropType::DTime,
            Prop::Graph(_) => PropType::Graph,
            Prop::Document(_) => PropType::Document,
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
            (Prop::Str(a), Prop::Str(b)) => Some(Prop::Str((a.to_string() + &b).into())),
            _ => None,
        }
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

    fn into_dtime(self) -> Option<NaiveDateTime>;
    fn unwrap_dtime(self) -> NaiveDateTime {
        self.into_dtime().unwrap()
    }

    fn into_graph(self) -> Option<Graph>;
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

    fn into_dtime(self) -> Option<NaiveDateTime> {
        self.and_then(|p| p.into_dtime())
    }

    fn into_graph(self) -> Option<Graph> {
        self.and_then(|p| p.into_graph())
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

    fn into_dtime(self) -> Option<NaiveDateTime> {
        if let Prop::DTime(v) = self {
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

    fn into_document(self) -> Option<DocumentInput> {
        if let Prop::Document(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

impl fmt::Display for Prop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
            Prop::Graph(value) => write!(
                f,
                "Graph(num_nodes={}, num_edges={})",
                value.count_nodes(),
                value.count_edges()
            ),
            Prop::List(value) => {
                write!(f, "{:?}", value)
            }
            Prop::Map(value) => {
                write!(f, "{:?}", value)
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

#[cfg(test)]
mod test_arc_str {
    use crate::core::{ArcStr, OptionAsStr};
    use std::sync::Arc;

    #[test]
    fn can_compare_with_str() {
        let test: ArcStr = "test".into();
        assert_eq!(test, "test");
        assert_eq!(test, "test".to_string());
        assert_eq!(test, Arc::from("test"));
        assert_eq!(&test, &"test".to_string())
    }

    #[test]
    fn test_option_conv() {
        let test: Option<ArcStr> = Some("test".into());

        let test_ref = test.as_ref();

        let opt_str = test.as_str();
        let opt_str3 = test_ref.as_str();

        let test2 = Some("test".to_string());
        let opt_str_2 = test2.as_str();

        assert_eq!(opt_str, Some("test"));
        assert_eq!(opt_str_2, Some("test"));
        assert_eq!(opt_str3, Some("test"));
    }
}
