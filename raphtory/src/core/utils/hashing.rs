#![allow(dead_code)]

//! Utility functions used throughout the modules.

use std::hash::{Hash, Hasher};

use twox_hash::XxHash64;

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = XxHash64::default();
    t.hash(&mut s);
    s.finish()
}

pub fn get_shard_id_from_global_vid(v_id: u64, n_shards: usize) -> usize {
    (v_id % n_shards as u64) as usize
}
