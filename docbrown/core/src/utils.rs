//! Utility functions used throughout the modules.

use std::hash::{Hash, Hasher};

use twox_hash::XxHash64;

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = XxHash64::default();
    t.hash(&mut s);
    s.finish()
}

pub fn get_shard_id_from_global_vid<T: Hash>(v_id: T, n_shards: usize) -> usize {
    let v_hash: u64 = calculate_hash(&v_id);
    let v: usize = v_hash.try_into().unwrap();
    v % n_shards
}
