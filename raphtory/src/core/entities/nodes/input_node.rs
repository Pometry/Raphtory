//! A data structure for representing input nodes in a temporal graph.
//!
//! Input nodes are used when first creating or accessing verticies by the user.
//! This trait allows you to use a variety of types as input nodes, including
//! `u64`, `&str`, and `String`.

use crate::core::utils::hashing;

pub trait InputNode: Clone {
    fn id(&self) -> u64;
    fn id_str(&self) -> Option<&str>;
}

impl InputNode for u64 {
    fn id(&self) -> u64 {
        *self
    }

    fn id_str(&self) -> Option<&str> {
        None
    }
}

impl<'a> InputNode for &'a str {
    fn id(&self) -> u64 {
        if &self.chars().next().unwrap_or('0') != &'0' {
            self.parse().unwrap_or(hashing::calculate_hash(self))
        } else {
            hashing::calculate_hash(self)
        }
    }

    fn id_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl InputNode for String {
    fn id(&self) -> u64 {
        let s: &str = self;
        s.id()
    }

    fn id_str(&self) -> Option<&str> {
        Some(self)
    }
}
