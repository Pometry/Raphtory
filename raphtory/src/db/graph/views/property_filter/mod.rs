use crate::core::Prop;
use std::cmp::Ordering;

pub mod edge_property_filter;

#[derive(Debug, Clone)]
pub struct PropFilter {
    value: Prop,
    cmp: Ordering,
}

impl PropFilter {
    pub fn new(value: Prop, cmp: Ordering) -> Self {
        Self { value, cmp }
    }

    fn filter(&self, other: &Prop) -> bool {
        match other.partial_cmp(&self.value) {
            None => false,
            Some(ordering) => ordering == self.cmp,
        }
    }
}
