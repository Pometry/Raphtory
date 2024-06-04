#![allow(dead_code)]

//! Utility functions used throughout the modules.

use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

#[cfg(feature = "storage")]
use pometry_storage::GID;

pub fn calculate_hash<T: Hash + ?Sized>(t: &T) -> u64 {
    let mut s = XxHash64::default();
    t.hash(&mut s);
    s.finish()
}

#[cfg(feature = "storage")]
pub fn calculate_hash_spark(gid: &GID) -> i64 {
    let mut s = XxHash64::with_seed(42);
    match gid {
        GID::U64(x) => s.write_u64(*x),
        GID::I64(x) => s.write_i64(*x),
        GID::Str(t) => {
            t.chars().for_each(|c| s.write_u8(c as u8));
        }
    }
    s.finish() as i64
}

pub fn get_shard_id_from_global_vid(v_id: u64, n_shards: usize) -> usize {
    (v_id % n_shards as u64) as usize
}
