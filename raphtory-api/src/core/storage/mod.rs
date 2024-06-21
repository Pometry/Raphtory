use dashmap::DashMap;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;

pub mod arc_str;
pub mod dict_mapper;
pub mod locked_vec;
pub mod timeindex;

pub type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

pub type ArcRwLockReadGuard<T> = lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, T>;
