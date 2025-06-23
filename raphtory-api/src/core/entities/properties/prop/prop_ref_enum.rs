use std::sync::Arc;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use rustc_hash::FxHashMap;

#[cfg(feature = "arrow")]
use crate::core::entities::properties::prop::PropArray;
use crate::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};

#[derive(Debug, PartialEq, Clone)]
// TODO: this needs more refinement, as it's not generic enough for all the storage types
pub enum PropRef<'a> {
    Str(&'a str),
    U8(u8),
    U16(u16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
    List(&'a Arc<Vec<Prop>>),
    Map(&'a Arc<FxHashMap<ArcStr, Prop>>),
    NDTime(&'a NaiveDateTime),
    DTime(&'a DateTime<Utc>),
    #[cfg(feature = "arrow")]
    Array(&'a PropArray),
    Decimal(&'a BigDecimal),
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
