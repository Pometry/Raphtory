//! A data structure for representing input vertices in a temporal graph.
//!
//! Input vertices are used when first creating or accessing verticies by the user.
//! This trait allows you to use a variety of types as input vertices, including
//! `u64`, `&str`, and `String`.

use crate::{utils, Prop};

pub trait InputVertex {
    fn id(&self) -> u64;
    fn name_prop(&self) -> Option<Prop>;
}

impl InputVertex for u64 {
    fn id(&self) -> u64 {
        *self
    }

    fn name_prop(&self) -> Option<Prop> {
        None
    }
}

impl<'a> InputVertex for &'a str {
    fn id(&self) -> u64 {
        utils::calculate_hash(self)
    }

    fn name_prop(&self) -> Option<Prop> {
        Some(Prop::Str(self.to_string()))
    }
}

impl InputVertex for String {
    fn id(&self) -> u64 {
        utils::calculate_hash(self)
    }

    fn name_prop(&self) -> Option<Prop> {
        Some(Prop::Str(self.to_string()))
    }
}
