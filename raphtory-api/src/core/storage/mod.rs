use dashmap::DashMap;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;

pub mod arc_str;
pub mod dict_mapper;
pub mod graph_folder;
pub mod locked_vec;
pub mod sorted_vec_map;
pub mod timeindex;

pub type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;
