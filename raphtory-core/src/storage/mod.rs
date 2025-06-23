use crate::{
    entities::{
        nodes::node_store::NodeStore,
        properties::{props::TPropError, tprop::IllegalPropType},
    },
    storage::lazy_vec::IllegalSet,
};
use bigdecimal::BigDecimal;
use itertools::Itertools;
use lazy_vec::LazyVec;
use lock_api;
use node_entry::NodePtr;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "arrow")]
use raphtory_api::core::entities::properties::prop::PropArray;
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropRef, PropType},
        GidRef, VID,
    },
    storage::arc_str::ArcStr,
};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::{Deref, DerefMut, Index, IndexMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use thiserror::Error;

pub mod lazy_vec;
pub mod locked_view;
pub mod node_entry;
pub mod raw_edges;
pub mod timeindex;

type ArcRwLockReadGuard<T> = lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, T>;
#[must_use]
pub struct UninitialisedEntry<'a, T, TS> {
    offset: usize,
    guard: RwLockWriteGuard<'a, TS>,
    value: T,
}

impl<'a, T: Default, TS: DerefMut<Target = Vec<T>>> UninitialisedEntry<'a, T, TS> {
    pub fn init(mut self) {
        if self.offset >= self.guard.len() {
            self.guard.resize_with(self.offset + 1, Default::default);
        }
        self.guard[self.offset] = self.value;
    }
    pub fn value(&self) -> &T {
        &self.value
    }
}

#[inline]
fn resolve(index: usize, num_buckets: usize) -> (usize, usize) {
    let bucket = index % num_buckets;
    let offset = index / num_buckets;
    (bucket, offset)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeVec {
    data: Arc<RwLock<NodeSlot>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct NodeSlot {
    nodes: Vec<NodeStore>,
    t_props_log: TColumns, // not the same size as nodes
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct TColumns {
    t_props_log: Vec<PropColumn>,
    num_rows: usize,
}

impl TColumns {
    pub fn push(
        &mut self,
        row: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<Option<usize>, TPropError> {
        let id = self.num_rows;
        let mut has_props = false;

        for (prop_id, prop) in row {
            match self.t_props_log.get_mut(prop_id) {
                Some(col) => col.push(prop)?,
                None => {
                    let col: PropColumn = PropColumn::new(self.num_rows, prop);
                    self.t_props_log
                        .resize_with(prop_id + 1, || PropColumn::Empty(id));
                    self.t_props_log[prop_id] = col;
                }
            }
            has_props = true;
        }

        if has_props {
            self.num_rows += 1;
            for col in self.t_props_log.iter_mut() {
                col.grow(self.num_rows);
            }
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub fn push_null(&mut self) -> usize {
        let id = self.num_rows;
        for col in self.t_props_log.iter_mut() {
            col.push_null();
        }
        self.num_rows += 1;
        id
    }

    pub fn get(&self, prop_id: usize) -> Option<&PropColumn> {
        self.t_props_log.get(prop_id)
    }

    pub fn getx(&self, prop_id: usize) -> Option<&PropColumn> {
        self.t_props_log.get(prop_id)
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &PropColumn> {
        self.t_props_log.iter()
    }

    pub fn num_columns(&self) -> usize {
        self.t_props_log.len()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum PropColumn {
    Empty(usize),
    Bool(LazyVec<bool>),
    U8(LazyVec<u8>),
    U16(LazyVec<u16>),
    U32(LazyVec<u32>),
    U64(LazyVec<u64>),
    I32(LazyVec<i32>),
    I64(LazyVec<i64>),
    F32(LazyVec<f32>),
    F64(LazyVec<f64>),
    Str(LazyVec<ArcStr>),
    #[cfg(feature = "arrow")]
    Array(LazyVec<PropArray>),
    List(LazyVec<Arc<Vec<Prop>>>),
    Map(LazyVec<Arc<FxHashMap<ArcStr, Prop>>>),
    NDTime(LazyVec<chrono::NaiveDateTime>),
    DTime(LazyVec<chrono::DateTime<chrono::Utc>>),
    Decimal(LazyVec<BigDecimal>),
}

#[derive(Error, Debug)]
pub enum TPropColumnError {
    #[error(transparent)]
    IllegalSetBool(#[from] IllegalSet<bool>),
    #[error(transparent)]
    IllegalSetU8(#[from] IllegalSet<u8>),
    #[error(transparent)]
    IllegalSetU16(#[from] IllegalSet<u16>),
    #[error(transparent)]
    IllegalSetU32(#[from] IllegalSet<u32>),
    #[error(transparent)]
    IllegalSetU64(#[from] IllegalSet<u64>),
    #[error(transparent)]
    IllegalSetI32(#[from] IllegalSet<i32>),
    #[error(transparent)]
    IllegalSetI64(#[from] IllegalSet<i64>),
    #[error(transparent)]
    IllegalSetF32(#[from] IllegalSet<f32>),
    #[error(transparent)]
    IllegalSetF64(#[from] IllegalSet<f64>),
    #[error(transparent)]
    IllegalSetStr(#[from] IllegalSet<ArcStr>),
    #[cfg(feature = "arrow")]
    #[error(transparent)]
    IllegalSetArray(#[from] IllegalSet<PropArray>),
    #[error(transparent)]
    IllegalSetList(#[from] IllegalSet<Arc<Vec<Prop>>>),
    #[error(transparent)]
    IllegalSetMap(#[from] IllegalSet<Arc<FxHashMap<ArcStr, Prop>>>),
    #[error(transparent)]
    IllegalSetNDTime(#[from] IllegalSet<chrono::NaiveDateTime>),
    #[error(transparent)]
    IllegalSetDTime(#[from] IllegalSet<chrono::DateTime<chrono::Utc>>),
    #[error(transparent)]
    Decimal(#[from] IllegalSet<BigDecimal>),
    #[error(transparent)]
    IllegalPropType(#[from] IllegalPropType),
}

impl Default for PropColumn {
    fn default() -> Self {
        PropColumn::Empty(0)
    }
}

impl PropColumn {
    pub(crate) fn new(idx: usize, prop: Prop) -> Self {
        let mut col = PropColumn::default();
        col.set(idx, prop).unwrap();
        col
    }

    pub(crate) fn dtype(&self) -> PropType {
        match self {
            PropColumn::Empty(_) => PropType::Empty,
            PropColumn::Bool(_) => PropType::Bool,
            PropColumn::U8(_) => PropType::U8,
            PropColumn::U16(_) => PropType::U16,
            PropColumn::U32(_) => PropType::U32,
            PropColumn::U64(_) => PropType::U64,
            PropColumn::I32(_) => PropType::I32,
            PropColumn::I64(_) => PropType::I64,
            PropColumn::F32(_) => PropType::F32,
            PropColumn::F64(_) => PropType::F64,
            PropColumn::Str(_) => PropType::Str,
            #[cfg(feature = "arrow")]
            PropColumn::Array(_) => PropType::Array(Box::new(PropType::Empty)),
            PropColumn::List(_) => PropType::List(Box::new(PropType::Empty)),
            PropColumn::Map(_) => PropType::Map(HashMap::new().into()),
            PropColumn::NDTime(_) => PropType::NDTime,
            PropColumn::DTime(_) => PropType::DTime,
            PropColumn::Decimal(_) => PropType::Decimal { scale: 0 },
        }
    }

    pub(crate) fn grow(&mut self, new_len: usize) {
        while self.len() < new_len {
            self.push_null();
        }
    }

    pub fn set(&mut self, index: usize, prop: Prop) -> Result<(), TPropColumnError> {
        self.init_empty_col(&prop);
        match (self, prop) {
            (PropColumn::Bool(col), Prop::Bool(v)) => col.set(index, v)?,
            (PropColumn::I64(col), Prop::I64(v)) => col.set(index, v)?,
            (PropColumn::U32(col), Prop::U32(v)) => col.set(index, v)?,
            (PropColumn::U64(col), Prop::U64(v)) => col.set(index, v)?,
            (PropColumn::F32(col), Prop::F32(v)) => col.set(index, v)?,
            (PropColumn::F64(col), Prop::F64(v)) => col.set(index, v)?,
            (PropColumn::Str(col), Prop::Str(v)) => col.set(index, v)?,
            #[cfg(feature = "arrow")]
            (PropColumn::Array(col), Prop::Array(v)) => col.set(index, v)?,
            (PropColumn::U8(col), Prop::U8(v)) => col.set(index, v)?,
            (PropColumn::U16(col), Prop::U16(v)) => col.set(index, v)?,
            (PropColumn::I32(col), Prop::I32(v)) => col.set(index, v)?,
            (PropColumn::List(col), Prop::List(v)) => col.set(index, v)?,
            (PropColumn::Map(col), Prop::Map(v)) => col.set(index, v)?,
            (PropColumn::NDTime(col), Prop::NDTime(v)) => col.set(index, v)?,
            (PropColumn::DTime(col), Prop::DTime(v)) => col.set(index, v)?,
            (PropColumn::Decimal(col), Prop::Decimal(v)) => col.set(index, v)?,
            (col, prop) => {
                Err(IllegalPropType {
                    expected: col.dtype(),
                    actual: prop.dtype(),
                })?;
            }
        }
        Ok(())
    }

    pub(crate) fn push(&mut self, prop: Prop) -> Result<(), IllegalPropType> {
        self.init_empty_col(&prop);
        match (self, prop) {
            (PropColumn::Bool(col), Prop::Bool(v)) => col.push(Some(v)),
            (PropColumn::U8(col), Prop::U8(v)) => col.push(Some(v)),
            (PropColumn::I64(col), Prop::I64(v)) => col.push(Some(v)),
            (PropColumn::U32(col), Prop::U32(v)) => col.push(Some(v)),
            (PropColumn::U64(col), Prop::U64(v)) => col.push(Some(v)),
            (PropColumn::F32(col), Prop::F32(v)) => col.push(Some(v)),
            (PropColumn::F64(col), Prop::F64(v)) => col.push(Some(v)),
            (PropColumn::Str(col), Prop::Str(v)) => col.push(Some(v)),
            #[cfg(feature = "arrow")]
            (PropColumn::Array(col), Prop::Array(v)) => col.push(Some(v)),
            (PropColumn::U16(col), Prop::U16(v)) => col.push(Some(v)),
            (PropColumn::I32(col), Prop::I32(v)) => col.push(Some(v)),
            (PropColumn::List(col), Prop::List(v)) => col.push(Some(v)),
            (PropColumn::Map(col), Prop::Map(v)) => col.push(Some(v)),
            (PropColumn::NDTime(col), Prop::NDTime(v)) => col.push(Some(v)),
            (PropColumn::DTime(col), Prop::DTime(v)) => col.push(Some(v)),
            (PropColumn::Decimal(col), Prop::Decimal(v)) => col.push(Some(v)),
            (col, prop) => {
                return Err(IllegalPropType {
                    expected: col.dtype(),
                    actual: prop.dtype(),
                })
            }
        }
        Ok(())
    }

    fn init_empty_col(&mut self, prop: &Prop) {
        if let PropColumn::Empty(len) = self {
            match prop {
                Prop::Bool(_) => *self = PropColumn::Bool(LazyVec::with_len(*len)),
                Prop::I64(_) => *self = PropColumn::I64(LazyVec::with_len(*len)),
                Prop::U32(_) => *self = PropColumn::U32(LazyVec::with_len(*len)),
                Prop::U64(_) => *self = PropColumn::U64(LazyVec::with_len(*len)),
                Prop::F32(_) => *self = PropColumn::F32(LazyVec::with_len(*len)),
                Prop::F64(_) => *self = PropColumn::F64(LazyVec::with_len(*len)),
                Prop::Str(_) => *self = PropColumn::Str(LazyVec::with_len(*len)),
                #[cfg(feature = "arrow")]
                Prop::Array(_) => *self = PropColumn::Array(LazyVec::with_len(*len)),
                Prop::U8(_) => *self = PropColumn::U8(LazyVec::with_len(*len)),
                Prop::U16(_) => *self = PropColumn::U16(LazyVec::with_len(*len)),
                Prop::I32(_) => *self = PropColumn::I32(LazyVec::with_len(*len)),
                Prop::List(_) => *self = PropColumn::List(LazyVec::with_len(*len)),
                Prop::Map(_) => *self = PropColumn::Map(LazyVec::with_len(*len)),
                Prop::NDTime(_) => *self = PropColumn::NDTime(LazyVec::with_len(*len)),
                Prop::DTime(_) => *self = PropColumn::DTime(LazyVec::with_len(*len)),
                Prop::Decimal(_) => *self = PropColumn::Decimal(LazyVec::with_len(*len)),
            }
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, PropColumn::Empty(_))
    }

    pub(crate) fn push_null(&mut self) {
        match self {
            PropColumn::Bool(col) => col.push(None),
            PropColumn::I64(col) => col.push(None),
            PropColumn::U32(col) => col.push(None),
            PropColumn::U64(col) => col.push(None),
            PropColumn::F32(col) => col.push(None),
            PropColumn::F64(col) => col.push(None),
            PropColumn::Str(col) => col.push(None),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.push(None),
            PropColumn::U8(col) => col.push(None),
            PropColumn::U16(col) => col.push(None),
            PropColumn::I32(col) => col.push(None),
            PropColumn::List(col) => col.push(None),
            PropColumn::Map(col) => col.push(None),
            PropColumn::NDTime(col) => col.push(None),
            PropColumn::DTime(col) => col.push(None),
            PropColumn::Decimal(col) => col.push(None),
            PropColumn::Empty(count) => {
                *count += 1;
            }
        }
    }

    pub fn get(&self, index: usize) -> Option<Prop> {
        match self {
            PropColumn::Bool(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::I64(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::U32(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::U64(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::F32(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::F64(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::Str(col) => col.get_opt(index).map(|prop| prop.into()),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.get_opt(index).map(|prop| Prop::Array(prop.clone())),
            PropColumn::U8(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::U16(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::I32(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::List(col) => col.get_opt(index).map(|prop| Prop::List(prop.clone())),
            PropColumn::Map(col) => col.get_opt(index).map(|prop| Prop::Map(prop.clone())),
            PropColumn::NDTime(col) => col.get_opt(index).map(|prop| Prop::NDTime(*prop)),
            PropColumn::DTime(col) => col.get_opt(index).map(|prop| Prop::DTime(*prop)),
            PropColumn::Decimal(col) => col.get_opt(index).map(|prop| Prop::Decimal(prop.clone())),
            PropColumn::Empty(_) => None,
        }
    }

    pub fn get_ref(&self, index: usize) -> Option<PropRef> {
        match self {
            PropColumn::Bool(col) => col.get_opt(index).map(|prop| PropRef::Bool(*prop)),
            PropColumn::I64(col) => col.get_opt(index).map(|prop| PropRef::I64(*prop)),
            PropColumn::U32(col) => col.get_opt(index).map(|prop| PropRef::U32(*prop)),
            PropColumn::U64(col) => col.get_opt(index).map(|prop| PropRef::U64(*prop)),
            PropColumn::F32(col) => col.get_opt(index).map(|prop| PropRef::F32(*prop)),
            PropColumn::F64(col) => col.get_opt(index).map(|prop| PropRef::F64(*prop)),
            PropColumn::Str(col) => col.get_opt(index).map(|prop| PropRef::Str(prop.as_ref())),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.get_opt(index).map(PropRef::Array),
            PropColumn::U8(col) => col.get_opt(index).map(|prop| PropRef::U8(*prop)),
            PropColumn::U16(col) => col.get_opt(index).map(|prop| PropRef::U16(*prop)),
            PropColumn::I32(col) => col.get_opt(index).map(|prop| PropRef::I32(*prop)),
            PropColumn::List(col) => col.get_opt(index).map(PropRef::List),
            PropColumn::Map(col) => col.get_opt(index).map(PropRef::Map),
            PropColumn::NDTime(col) => col.get_opt(index).map(PropRef::NDTime),
            PropColumn::DTime(col) => col.get_opt(index).map(PropRef::DTime),
            PropColumn::Decimal(col) => col.get_opt(index).map(PropRef::Decimal),
            PropColumn::Empty(_) => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            PropColumn::Bool(col) => col.len(),
            PropColumn::I64(col) => col.len(),
            PropColumn::U32(col) => col.len(),
            PropColumn::U64(col) => col.len(),
            PropColumn::F32(col) => col.len(),
            PropColumn::F64(col) => col.len(),
            PropColumn::Str(col) => col.len(),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.len(),
            PropColumn::U8(col) => col.len(),
            PropColumn::U16(col) => col.len(),
            PropColumn::I32(col) => col.len(),
            PropColumn::List(col) => col.len(),
            PropColumn::Map(col) => col.len(),
            PropColumn::NDTime(col) => col.len(),
            PropColumn::DTime(col) => col.len(),
            PropColumn::Decimal(col) => col.len(),
            PropColumn::Empty(count) => *count,
        }
    }
}

impl NodeSlot {
    pub fn t_props_log(&self) -> &TColumns {
        &self.t_props_log
    }

    pub fn t_props_log_mut(&mut self) -> &mut TColumns {
        &mut self.t_props_log
    }

    pub fn iter(&self) -> impl Iterator<Item = NodePtr> {
        self.nodes
            .iter()
            .map(|ns| NodePtr::new(ns, &self.t_props_log))
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodePtr> {
        self.nodes
            .par_iter()
            .map(|ns| NodePtr::new(ns, &self.t_props_log))
    }
}

impl Index<usize> for NodeSlot {
    type Output = NodeStore;

    fn index(&self, index: usize) -> &Self::Output {
        &self.nodes[index]
    }
}

impl IndexMut<usize> for NodeSlot {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.nodes[index]
    }
}

impl Deref for NodeSlot {
    type Target = Vec<NodeStore>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl DerefMut for NodeSlot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}

impl PartialEq for NodeVec {
    fn eq(&self, other: &Self) -> bool {
        let a = self.data.read();
        let b = other.data.read();
        a.deref() == b.deref()
    }
}

impl Default for NodeVec {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeVec {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Default::default())),
        }
    }

    #[inline]
    pub fn read_arc_lock(&self) -> ArcRwLockReadGuard<NodeSlot> {
        RwLock::read_arc(&self.data)
    }

    #[inline]
    pub fn write(&self) -> impl DerefMut<Target = NodeSlot> + '_ {
        self.data.write()
    }

    #[inline]
    pub fn read(&self) -> impl Deref<Target = NodeSlot> + '_ {
        self.data.read()
    }
}

#[derive(Serialize, Deserialize)]
pub struct NodeStorage {
    pub(crate) data: Box<[NodeVec]>,
    len: AtomicUsize,
}

impl Debug for NodeStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStorage")
            .field("len", &self.len())
            .field("data", &self.read_lock().iter().collect_vec())
            .finish()
    }
}

impl PartialEq for NodeStorage {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

#[derive(Debug)]
pub struct ReadLockedStorage {
    pub(crate) locks: Vec<Arc<ArcRwLockReadGuard<NodeSlot>>>,
    len: usize,
}

impl ReadLockedStorage {
    fn resolve(&self, index: VID) -> (usize, usize) {
        let index: usize = index.into();
        let n = self.locks.len();
        let bucket = index % n;
        let offset = index / n;
        (bucket, offset)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(test)]
    pub fn get(&self, index: VID) -> &NodeStore {
        let (bucket, offset) = self.resolve(index);
        let bucket = &self.locks[bucket];
        &bucket[offset]
    }

    #[inline]
    pub fn get_entry(&self, index: VID) -> NodePtr {
        let (bucket, offset) = self.resolve(index);
        let bucket = &self.locks[bucket];
        NodePtr::new(&bucket[offset], &bucket.t_props_log)
    }

    pub fn iter(&self) -> impl Iterator<Item = NodePtr> + '_ {
        self.locks.iter().flat_map(|v| v.iter())
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodePtr> + '_ {
        self.locks.par_iter().flat_map(|v| v.par_iter())
    }
}

impl NodeStorage {
    pub fn count_with_filter<F: Fn(NodePtr<'_>) -> bool + Send + Sync>(&self, f: F) -> usize {
        self.read_lock().par_iter().filter(|x| f(*x)).count()
    }
}

impl NodeStorage {
    #[inline]
    fn resolve(&self, index: usize) -> (usize, usize) {
        resolve(index, self.data.len())
    }

    #[inline]
    pub fn read_lock(&self) -> ReadLockedStorage {
        let guards = self
            .data
            .iter()
            .map(|v| Arc::new(v.read_arc_lock()))
            .collect();
        ReadLockedStorage {
            locks: guards,
            len: self.len(),
        }
    }

    pub fn write_lock(&self) -> WriteLockedNodes {
        WriteLockedNodes {
            guards: self.data.iter().map(|lock| lock.data.write()).collect(),
            global_len: &self.len,
        }
    }

    pub fn new(n_locks: usize) -> Self {
        let data: Box<[NodeVec]> = (0..n_locks)
            .map(|_| NodeVec::new())
            .collect::<Vec<_>>()
            .into();

        Self {
            data,
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, mut value: NodeStore) -> UninitialisedEntry<NodeStore, NodeSlot> {
        let index = self.len.fetch_add(1, Ordering::Relaxed);
        value.vid = VID(index);
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        UninitialisedEntry {
            offset,
            guard,
            value,
        }
    }

    pub fn set(&self, value: NodeStore) {
        let VID(index) = value.vid;
        self.len.fetch_max(index + 1, Ordering::Relaxed);
        let (bucket, offset) = self.resolve(index);
        let mut guard = self.data[bucket].data.write();
        if guard.len() <= offset {
            guard.resize_with(offset + 1, NodeStore::default)
        }
        guard[offset] = value
    }

    #[inline]
    pub fn entry(&self, index: VID) -> NodeEntry<'_> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.read_recursive();
        NodeEntry { offset, guard }
    }

    pub fn entry_mut(&self, index: VID) -> EntryMut<'_, RwLockWriteGuard<'_, NodeSlot>> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        EntryMut {
            i: offset,
            guard,
            _pd: PhantomData,
        }
    }

    pub fn prop_entry_mut(&self, index: VID) -> impl DerefMut<Target = TColumns> + '_ {
        let index = index.into();
        let (bucket, _) = self.resolve(index);
        let lock = self.data[bucket].data.write();
        RwLockWriteGuard::map(lock, |data| &mut data.t_props_log)
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: VID, j: VID) -> PairEntryMut<'_> {
        let i = i.into();
        let j = j.into();
        let (bucket_i, offset_i) = self.resolve(i);
        let (bucket_j, offset_j) = self.resolve(j);
        // always acquire lock for smaller bucket first to avoid deadlock between two updates for the same pair of buckets
        if bucket_i < bucket_j {
            let guard_i = self.data[bucket_i].data.write();
            let guard_j = self.data[bucket_j].data.write();
            PairEntryMut::Different {
                i: offset_i,
                j: offset_j,
                guard1: guard_i,
                guard2: guard_j,
            }
        } else if bucket_i > bucket_j {
            let guard_j = self.data[bucket_j].data.write();
            let guard_i = self.data[bucket_i].data.write();
            PairEntryMut::Different {
                i: offset_i,
                j: offset_j,
                guard1: guard_i,
                guard2: guard_j,
            }
        } else {
            PairEntryMut::Same {
                i: offset_i,
                j: offset_j,
                guard: self.data[bucket_i].data.write(),
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn next_id(&self) -> VID {
        VID(self.len.fetch_add(1, Ordering::Relaxed))
    }
}

pub struct WriteLockedNodes<'a> {
    guards: Vec<RwLockWriteGuard<'a, NodeSlot>>,
    global_len: &'a AtomicUsize,
}

pub struct NodeShardWriter<'a, S> {
    shard: S,
    shard_id: usize,
    num_shards: usize,
    global_len: &'a AtomicUsize,
}

impl<'a, S> NodeShardWriter<'a, S>
where
    S: DerefMut<Target = NodeSlot>,
{
    #[inline]
    fn resolve(&self, index: VID) -> Option<usize> {
        let (shard_id, offset) = resolve(index.into(), self.num_shards);
        (shard_id == self.shard_id).then_some(offset)
    }

    #[inline]
    pub fn get_mut(&mut self, index: VID) -> Option<&mut NodeStore> {
        self.resolve(index).map(|offset| &mut self.shard[offset])
    }

    #[inline]
    pub fn get_mut_entry(&mut self, index: VID) -> Option<EntryMut<'_, &mut S>> {
        self.resolve(index).map(|offset| EntryMut {
            i: offset,
            guard: &mut self.shard,
            _pd: PhantomData,
        })
    }

    #[inline]
    pub fn get(&self, index: VID) -> Option<&NodeStore> {
        self.resolve(index).map(|offset| &self.shard[offset])
    }

    #[inline]
    pub fn t_prop_log_mut(&mut self) -> &mut TColumns {
        &mut self.shard.t_props_log
    }

    pub fn set(&mut self, vid: VID, gid: GidRef) -> Option<EntryMut<'_, &mut S>> {
        self.resolve(vid).map(|offset| {
            if offset >= self.shard.len() {
                self.shard.resize_with(offset + 1, NodeStore::default);
                self.global_len
                    .fetch_max(vid.index() + 1, Ordering::Relaxed);
            }
            self.shard[offset] = NodeStore::resolved(gid.to_owned(), vid);

            EntryMut {
                i: offset,
                guard: &mut self.shard,
                _pd: PhantomData,
            }
        })
    }

    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn resize(&mut self, new_global_len: usize) {
        let mut new_len = new_global_len / self.num_shards;
        if self.shard_id < new_global_len % self.num_shards {
            new_len += 1;
        }
        if new_len > self.shard.len() {
            self.shard.resize_with(new_len, Default::default);
            self.global_len.fetch_max(new_global_len, Ordering::Relaxed);
        }
    }
}

impl<'a> WriteLockedNodes<'a> {
    pub fn par_iter_mut(
        &mut self,
    ) -> impl IndexedParallelIterator<Item = NodeShardWriter<&mut NodeSlot>> + '_ {
        let num_shards = self.guards.len();
        let global_len = self.global_len;
        let shards: Vec<&mut NodeSlot> = self
            .guards
            .iter_mut()
            .map(|guard| guard.deref_mut())
            .collect();
        shards
            .into_par_iter()
            .enumerate()
            .map(move |(shard_id, shard)| NodeShardWriter {
                shard,
                shard_id,
                num_shards,
                global_len,
            })
    }

    pub fn into_par_iter_mut(
        self,
    ) -> impl IndexedParallelIterator<Item = NodeShardWriter<'a, RwLockWriteGuard<'a, NodeSlot>>> + 'a
    {
        let num_shards = self.guards.len();
        let global_len = self.global_len;
        self.guards
            .into_par_iter()
            .enumerate()
            .map(move |(shard_id, shard)| NodeShardWriter {
                shard,
                shard_id,
                num_shards,
                global_len,
            })
    }

    pub fn resize(&mut self, new_len: usize) {
        self.par_iter_mut()
            .for_each(|mut shard| shard.resize(new_len))
    }

    pub fn num_shards(&self) -> usize {
        self.guards.len()
    }
}

#[derive(Debug)]
pub struct NodeEntry<'a> {
    offset: usize,
    guard: RwLockReadGuard<'a, NodeSlot>,
}

impl NodeEntry<'_> {
    #[inline]
    pub fn as_ref(&self) -> NodePtr<'_> {
        NodePtr::new(&self.guard[self.offset], &self.guard.t_props_log)
    }
}

pub enum PairEntryMut<'a> {
    Same {
        i: usize,
        j: usize,
        guard: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
    },
    Different {
        i: usize,
        j: usize,
        guard1: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
        guard2: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
    },
}

impl<'a> PairEntryMut<'a> {
    pub(crate) fn get_i(&self) -> &NodeStore {
        match self {
            PairEntryMut::Same { i, guard, .. } => &guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &guard1[*i],
        }
    }
    pub(crate) fn get_mut_i(&mut self) -> &mut NodeStore {
        match self {
            PairEntryMut::Same { i, guard, .. } => &mut guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &mut guard1[*i],
        }
    }

    pub(crate) fn get_j(&self) -> &NodeStore {
        match self {
            PairEntryMut::Same { j, guard, .. } => &guard[*j],
            PairEntryMut::Different { j, guard2, .. } => &guard2[*j],
        }
    }

    pub(crate) fn get_mut_j(&mut self) -> &mut NodeStore {
        match self {
            PairEntryMut::Same { j, guard, .. } => &mut guard[*j],
            PairEntryMut::Different { j, guard2, .. } => &mut guard2[*j],
        }
    }
}

pub struct EntryMut<'a, NS: 'a> {
    i: usize,
    guard: NS,
    _pd: PhantomData<&'a ()>,
}

impl<'a, NS> EntryMut<'a, NS> {
    pub fn to_mut(&mut self) -> EntryMut<'a, &mut NS> {
        EntryMut {
            i: self.i,
            guard: &mut self.guard,
            _pd: self._pd,
        }
    }
}

impl<'a, NS: DerefMut<Target = NodeSlot>> AsMut<NodeStore> for EntryMut<'a, NS> {
    fn as_mut(&mut self) -> &mut NodeStore {
        let slots = self.guard.deref_mut();
        &mut slots[self.i]
    }
}

impl<'a, NS: DerefMut<Target = NodeSlot> + 'a> EntryMut<'a, &'a mut NS> {
    pub fn node_store_mut(&mut self) -> &mut NodeStore {
        &mut self.guard[self.i]
    }

    pub fn t_props_log_mut(&mut self) -> &mut TColumns {
        &mut self.guard.t_props_log
    }
}

#[cfg(test)]
mod test {
    use super::{NodeStorage, TColumns};
    use crate::entities::nodes::node_store::NodeStore;
    use proptest::{arbitrary::any, prop_assert_eq, proptest};
    use raphtory_api::core::entities::{properties::prop::Prop, GID, VID};
    use rayon::prelude::*;
    use std::borrow::Cow;

    #[test]
    fn tcolumns_append_1() {
        let mut t_cols = TColumns::default();

        t_cols.push([(1, Prop::U64(1))]).unwrap();

        let col0 = t_cols.get(0).unwrap();
        let col1 = t_cols.get(1).unwrap();

        assert_eq!(col0.len(), 1);
        assert_eq!(col1.len(), 1);
    }

    #[test]
    fn tcolumns_append_3_rows() {
        let mut t_cols = TColumns::default();

        t_cols
            .push([(1, Prop::U64(1)), (0, Prop::Str("a".into()))])
            .unwrap();
        t_cols
            .push([(0, Prop::Str("c".into())), (2, Prop::I64(9))])
            .unwrap();
        t_cols
            .push([(1, Prop::U64(1)), (3, Prop::Str("c".into()))])
            .unwrap();

        assert_eq!(t_cols.len(), 3);

        for col_id in 0..4 {
            let col = t_cols.get(col_id).unwrap();
            assert_eq!(col.len(), 3);
        }

        let col0 = (0..3)
            .map(|row| t_cols.get(0).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            col0,
            vec![
                Some(Prop::Str("a".into())),
                Some(Prop::Str("c".into())),
                None
            ]
        );

        let col1 = (0..3)
            .map(|row| t_cols.get(1).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(col1, vec![Some(Prop::U64(1)), None, Some(Prop::U64(1))]);

        let col2 = (0..3)
            .map(|row| t_cols.get(2).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(col2, vec![None, Some(Prop::I64(9)), None]);

        let col3 = (0..3)
            .map(|row| t_cols.get(3).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(col3, vec![None, None, Some(Prop::Str("c".into()))]);
    }

    #[test]
    fn tcolumns_append_2_columns_12_items() {
        let mut t_cols = TColumns::default();

        for value in 0..12 {
            if value % 2 == 0 {
                t_cols
                    .push([
                        (1, Prop::U64(value)),
                        (0, Prop::Str(value.to_string().into())),
                    ])
                    .unwrap();
            } else {
                t_cols.push([(1, Prop::U64(value))]).unwrap();
            }
        }

        assert_eq!(t_cols.len(), 12);

        let col0 = (0..12)
            .map(|row| t_cols.get(0).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            col0,
            vec![
                Some(Prop::Str("0".into())),
                None,
                Some(Prop::Str("2".into())),
                None,
                Some(Prop::Str("4".into())),
                None,
                Some(Prop::Str("6".into())),
                None,
                Some(Prop::Str("8".into())),
                None,
                Some(Prop::Str("10".into())),
                None
            ]
        );

        let col1 = (0..12)
            .map(|row| t_cols.get(1).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            col1,
            vec![
                Some(Prop::U64(0)),
                Some(Prop::U64(1)),
                Some(Prop::U64(2)),
                Some(Prop::U64(3)),
                Some(Prop::U64(4)),
                Some(Prop::U64(5)),
                Some(Prop::U64(6)),
                Some(Prop::U64(7)),
                Some(Prop::U64(8)),
                Some(Prop::U64(9)),
                Some(Prop::U64(10)),
                Some(Prop::U64(11))
            ]
        );
    }

    #[test]
    fn add_5_values_to_storage() {
        let storage = NodeStorage::new(2);

        for i in 0..5 {
            storage.push(NodeStore::empty(i.into())).init();
        }

        assert_eq!(storage.len(), 5);

        for i in 0..5 {
            let entry = storage.entry(VID(i));
            assert_eq!(entry.as_ref().node().vid, VID(i));
        }

        let items = storage.read_lock();

        let actual = items
            .iter()
            .map(|s| s.node().vid.index())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![0, 2, 4, 1, 3]);
    }

    #[test]
    fn test_index_correctness() {
        let storage = NodeStorage::new(2);

        for i in 0..5 {
            storage.push(NodeStore::empty(i.into())).init();
        }
        let locked = storage.read_lock();
        let actual: Vec<_> = (0..5)
            .map(|i| (i, locked.get(VID(i)).global_id.to_str()))
            .collect();

        assert_eq!(
            actual,
            vec![
                (0usize, Cow::Borrowed("0")),
                (1, "1".into()),
                (2, "2".into()),
                (3, "3".into()),
                (4, "4".into())
            ]
        );
    }

    #[test]
    fn test_entry() {
        let storage = NodeStorage::new(2);

        for i in 0..5 {
            storage.push(NodeStore::empty(i.into())).init();
        }

        for i in 0..5 {
            let entry = storage.entry(VID(i));
            assert_eq!(*entry.as_ref().node().global_id.to_str(), i.to_string());
        }
    }

    #[test]
    fn concurrent_push() {
        proptest!(|(v in any::<Vec<u64>>())| {
            let storage = NodeStorage::new(16);
            let mut expected = v
                .into_par_iter()
                .map(|v| {
                    storage.push(NodeStore::empty(GID::U64(v))).init();
                    v
                })
                .collect::<Vec<_>>();

            let locked = storage.read_lock();
            let mut actual: Vec<_> = locked
                .iter()
                .map(|n| n.node().global_id.as_u64().unwrap())
                .collect();

            actual.sort();
            expected.sort();
            prop_assert_eq!(actual, expected)
        })
    }
}
