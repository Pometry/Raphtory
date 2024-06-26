//! Utility functions used throughout the modules.

use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

pub fn calculate_hash<T: Hash + ?Sized>(t: &T) -> u64 {
    let mut s = XxHash64::default();
    t.hash(&mut s);
    s.finish()
}
