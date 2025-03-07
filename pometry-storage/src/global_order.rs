use std::fmt::Debug;

use crate::arrow2::{
    array::{Array, PrimitiveArray, Utf8Array},
    datatypes::ArrowDataType as DataType,
};
use ahash::AHashMap;
use itertools::Itertools;
use polars_arrow::array::Utf8ViewArray;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use super::{GidRef, RAError, GID};

pub trait GlobalOrder: Debug + Send + Sync {
    fn len(&self) -> usize;

    fn id_type(&self) -> GidType;

    fn find<'a, Ref: Into<GidRef<'a>>>(&'a self, gid: Ref) -> Option<usize>;

    fn contains<'a, Ref: Into<GidRef<'a>>>(&'a self, gid: Ref) -> bool;
}

impl<ID: Into<GID>> FromIterator<ID> for GlobalMap {
    fn from_iter<I: IntoIterator<Item = ID>>(iter: I) -> Self {
        let mut global: GlobalMap = Default::default();
        for gid in iter {
            global.push_id(gid);
        }
        global
    }
}

impl<ID: Into<GID>, I: IntoIterator<Item = ID>> From<I> for GlobalMap {
    fn from(iter: I) -> Self {
        iter.into_iter().collect()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GlobalMap {
    gids_table: AHashMap<GID, usize>,
}

impl GlobalMap {
    fn push_id<ID: Into<GID>>(&mut self, id: ID) {
        let id = id.into();
        let pos = self.gids_table.len();
        self.gids_table.insert(id, pos);
    }

    pub fn get_or_insert<ID: Into<GID>>(&mut self, id: ID) -> usize {
        let id = id.into();
        let next_id = self.gids_table.len();
        *self.gids_table.entry(id).or_insert(next_id)
    }

    pub fn insert<ID: Into<GID>>(&mut self, id: ID, index: usize) {
        self.gids_table.insert(id.into(), index);
    }

    pub fn sorted_gids(&self) -> Box<dyn Iterator<Item = GID> + '_> {
        Box::new(
            self.gids_table
                .iter()
                .sorted_by(|(_, i1), (_, i2)| i1.cmp(i2))
                .map(|(id, _)| id.clone()),
        )
    }
}

impl GlobalOrder for GlobalMap {
    fn len(&self) -> usize {
        self.gids_table.len()
    }

    fn id_type(&self) -> GidType {
        match self.gids_table.keys().next() {
            None => GidType::U64,
            Some(v) => match v {
                GID::U64(_) => GidType::U64,
                GID::Str(_) => GidType::Str,
            },
        }
    }

    fn find<'a, Ref: Into<GidRef<'a>>>(&'a self, gid: Ref) -> Option<usize> {
        let gid = gid.into().to_owned();
        self.gids_table.get(&gid).cloned()
    }

    fn contains<'a, Ref: Into<GidRef<'a>>>(&self, gid: Ref) -> bool {
        self.gids_table.contains_key(&gid.into().to_owned())
    }
}

#[derive(Debug, PartialEq)]
pub enum GIDArray {
    I64(PrimitiveArray<i64>),
    U64(PrimitiveArray<u64>),
    Str32(Utf8Array<i32>),
    Str64(Utf8Array<i64>),
    StrView(Utf8ViewArray),
}

impl GIDArray {
    pub fn id_type(&self) -> GidType {
        match self {
            GIDArray::I64(_) => GidType::I64,
            GIDArray::U64(_) => GidType::U64,
            GIDArray::Str32(_) | GIDArray::StrView(_) | GIDArray::Str64(_) => GidType::Str,
        }
    }

    pub fn to_boxed(&self) -> Box<dyn Array> {
        match self {
            GIDArray::I64(a) => a.to_boxed(),
            GIDArray::U64(a) => a.to_boxed(),
            GIDArray::Str32(a) => a.to_boxed(),
            GIDArray::Str64(a) => a.to_boxed(),
            GIDArray::StrView(a) => a.to_boxed(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            GIDArray::I64(a) => a.len(),
            GIDArray::U64(a) => a.len(),
            GIDArray::Str32(a) => a.len(),
            GIDArray::Str64(a) => a.len(),
            GIDArray::StrView(a) => a.len(),
        }
    }

    pub fn value(&self, index: usize) -> GidRef {
        match self {
            GIDArray::I64(a) => GidRef::U64(a.value(index) as u64),
            GIDArray::U64(a) => GidRef::U64(a.value(index)),
            GIDArray::Str32(a) => GidRef::Str(a.value(index)),
            GIDArray::Str64(a) => GidRef::Str(a.value(index)),
            GIDArray::StrView(a) => GidRef::Str(a.value(index)),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = GidRef> {
        (0..self.len()).map(|i| self.value(i))
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = GidRef> {
        (0..self.len()).into_par_iter().map(|i| self.value(i))
    }
}

impl TryFrom<Box<dyn Array>> for GIDArray {
    type Error = RAError;

    fn try_from(array: Box<dyn Array>) -> Result<Self, Self::Error> {
        match array.data_type() {
            DataType::UInt64 => {
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap();
                Ok(GIDArray::U64(array.clone()))
            }
            DataType::Int64 => {
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                Ok(GIDArray::I64(array.clone()))
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                Ok(GIDArray::Str32(array.clone()))
            }
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
                Ok(GIDArray::Str64(array.clone()))
            }
            DataType::Utf8View => {
                let array = array.as_any().downcast_ref::<Utf8ViewArray>().unwrap();
                Ok(GIDArray::StrView(array.clone()))
            }
            d_type => Err(RAError::InvalidTypeColumn(format!("{:?}", d_type))),
        }
    }
}

#[derive(Debug)]
pub enum GidType {
    Str,
    U64,
    I64,
}
