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
//! **Note** they must have Rust 1.80 or later.
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
pub mod utils;

pub use raphtory_api::core::*;
pub use raphtory_memstorage::core::{entities::*, storage::*};

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

#[cfg(feature = "io")]
mod serde_value_into_prop {
    use std::collections::HashMap;

    use super::Prop;
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
