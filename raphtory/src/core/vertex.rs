//! A data structure for representing input vertices in a temporal graph.
//!
//! Input vertices are used when first creating or accessing verticies by the user.
//! This trait allows you to use a variety of types as input vertices, including
//! `u64`, `&str`, and `String`.

use crate::core::{utils, Prop};

pub trait InputVertex: Clone {
    fn id(&self) -> u64;
    fn id_str(&self) -> Option<&str>;
}

impl InputVertex for u64 {
    fn id(&self) -> u64 {
        *self
    }

    fn id_str(&self) -> Option<&str> {
        None
    }
}

impl<'a> InputVertex for &'a str {
    fn id(&self) -> u64 {
        self.parse().unwrap_or(utils::calculate_hash(self))
    }

    fn id_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl InputVertex for String {
    fn id(&self) -> u64 {
        let s: &str = self;
        s.id()
    }

    fn id_str(&self) -> Option<&str> {
        Some(self)
    }
}
