use crate::core::{Prop, PropType};
use std::{collections::HashSet, sync::Arc};

pub mod edge_property_filter;
pub mod exploded_edge_property_filter;

#[derive(Debug, Clone)]
pub struct PropValueFilter {
    value: Prop,
    filter: fn(&Prop, &Prop) -> bool,
}

#[derive(Debug, Clone)]
pub enum PropertyFilter {
    ByValue(PropValueFilter),
    Has,
    HasNot,
    In(Arc<HashSet<Prop>>),
    NotIn(Arc<HashSet<Prop>>),
}

impl PropertyFilter {
    pub fn eq(value: impl Into<Prop>) -> Self {
        Self::ByValue(PropValueFilter::new(value.into(), |left, right| {
            left == right
        }))
    }

    pub fn ne(value: impl Into<Prop>) -> Self {
        Self::ByValue(PropValueFilter::new(value.into(), |left, right| {
            left != right
        }))
    }

    pub fn le(value: impl Into<Prop>) -> Self {
        Self::ByValue(PropValueFilter::new(value.into(), |left, right| {
            left <= right
        }))
    }

    pub fn ge(value: impl Into<Prop>) -> Self {
        Self::ByValue(PropValueFilter::new(value.into(), |left, right| {
            left >= right
        }))
    }

    pub fn lt(value: impl Into<Prop>) -> Self {
        Self::ByValue(PropValueFilter::new(value.into(), |left, right| {
            left < right
        }))
    }

    pub fn gt(value: impl Into<Prop>) -> Self {
        Self::ByValue(PropValueFilter::new(value.into(), |left, right| {
            left > right
        }))
    }

    pub fn any(values: impl IntoIterator<Item = impl Into<Prop>>) -> Self {
        Self::In(Arc::new(values.into_iter().map(|v| v.into()).collect()))
    }

    pub fn not_any(values: impl IntoIterator<Item = impl Into<Prop>>) -> Self {
        Self::NotIn(Arc::new(values.into_iter().map(|v| v.into()).collect()))
    }

    pub fn is_none() -> Self {
        Self::HasNot
    }

    pub fn is_some() -> Self {
        Self::Has
    }

    fn filter(&self, value: Option<&Prop>) -> bool {
        match self {
            PropertyFilter::ByValue(filter) => value.filter(|&v| filter.filter(v)).is_some(),
            PropertyFilter::Has => value.is_some(),
            PropertyFilter::HasNot => value.is_none(),
            PropertyFilter::In(set) => value.filter(|&v| set.contains(v)).is_some(),
            PropertyFilter::NotIn(set) => match value {
                Some(value) => !set.contains(value),
                None => true,
            },
        }
    }
}

impl From<PropValueFilter> for PropertyFilter {
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

    pub(crate) fn dtype(&self) -> PropType {
        self.value.dtype()
    }

    fn filter(&self, other: &Prop) -> bool {
        (self.filter)(&other, &self.value)
    }
}
