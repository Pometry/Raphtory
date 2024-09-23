use crate::core::Prop;
use std::collections::HashSet;

pub mod edge_property_filter;
mod exploded_edge_property_filter;

#[derive(Debug, Clone)]
pub(crate) struct PropValueFilter {
    value: Prop,
    filter: fn(&Prop, &Prop) -> bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PropFilter {
    ByValue(PropValueFilter),
    Has,
    HasNot,
    In(HashSet<Prop>),
    NotIn(HashSet<Prop>),
}

impl PropFilter {
    fn filter(&self, value: Option<&Prop>) -> bool {
        match self {
            PropFilter::ByValue(filter) => value.filter(|&v| filter.filter(v)).is_some(),
            PropFilter::Has => value.is_some(),
            PropFilter::HasNot => value.is_none(),
            PropFilter::In(set) => value.filter(|&v| set.contains(v)).is_some(),
            PropFilter::NotIn(set) => match value {
                Some(value) => !set.contains(value),
                None => true,
            },
        }
    }
}

impl From<PropValueFilter> for PropFilter {
    fn from(value: PropValueFilter) -> Self {
        Self::ByValue(value)
    }
}

impl PropValueFilter {
    /// Construct a property filter
    ///
    /// the first argument passed to `filter` is the property value from the node or edge, the second argument is `value`
    /// from the node or edge
    pub(crate) fn new(value: Prop, filter: fn(&Prop, &Prop) -> bool) -> Self {
        Self { value, filter }
    }

    fn filter(&self, other: &Prop) -> bool {
        (self.filter)(&other, &self.value)
    }
}
