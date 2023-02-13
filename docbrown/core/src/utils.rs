use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn get_shard_id_from_global_vid(v_id: u64, n_shards: usize) -> usize {
    let v: usize = v_id.try_into().unwrap();
    v % n_shards
}
