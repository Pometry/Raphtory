use crate::core::Prop;
use std::cmp::Ordering;

pub mod edge_property_filter;

#[derive(Debug, Clone)]
pub struct PropFilter {
    value: Prop,
    filter: fn(&Prop, &Prop) -> bool,
}

impl PropFilter {
    /// Construct a property filter
    ///
    /// `value` is the first argument passed to `filter`, the second argument is the property value
    /// from the node or edge
    pub fn new(value: Prop, filter: fn(&Prop, &Prop) -> bool) -> Self {
        Self { value, filter }
    }

    fn filter(&self, other: &Prop) -> bool {
        (self.filter)(&self.value, &other)
    }
}
