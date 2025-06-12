use crate::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use rustc_hash::FxHashMap;
use std::sync::Arc;

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
