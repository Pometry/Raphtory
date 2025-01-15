use crate::core::{entities::properties::props::Meta, utils::errors::GraphError, Prop, PropType};
use std::{collections::HashSet, sync::Arc};

pub mod edge_property_filter;
pub mod exploded_edge_property_filter;
pub(crate) mod internal;
pub mod node_property_filter;

#[derive(Debug, Clone)]
pub(crate) struct PropValueCmp {
    value: Prop,
    filter: fn(&Prop, &Prop) -> bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PropertyValueFilter {
    ByValue(PropValueCmp),
    Has,
    HasNot,
    In(Arc<HashSet<Prop>>),
    NotIn(Arc<HashSet<Prop>>),
}

#[derive(Debug, Clone)]
pub struct PropertyFilter {
    name: String,
    filter: PropertyValueFilter,
}

impl PropertyFilter {
    fn new(name: impl Into<String>, filter: PropertyValueFilter) -> Self {
        Self {
            name: name.into(),
            filter,
        }
    }
    pub fn eq(name: impl Into<String>, value: impl Into<Prop>) -> Self {
        Self::new(
            name,
            PropertyValueFilter::ByValue(PropValueCmp::new(value.into(), |left, right| {
                left == right
            })),
        )
    }

    pub fn ne(name: impl Into<String>, value: impl Into<Prop>) -> Self {
        Self::new(
            name,
            PropertyValueFilter::ByValue(PropValueCmp::new(value.into(), |left, right| {
                left != right
            })),
        )
    }

    pub fn le(name: impl Into<String>, value: impl Into<Prop>) -> Self {
        let filter =
            PropertyValueFilter::ByValue(PropValueCmp::new(value.into(), |left, right| {
                left <= right
            }));
        Self::new(name, filter)
    }

    pub fn ge(name: impl Into<String>, value: impl Into<Prop>) -> Self {
        let filter =
            PropertyValueFilter::ByValue(PropValueCmp::new(value.into(), |left, right| {
                left >= right
            }));
        Self::new(name, filter)
    }

    pub fn lt(name: impl Into<String>, value: impl Into<Prop>) -> Self {
        let filter =
            PropertyValueFilter::ByValue(PropValueCmp::new(value.into(), |left, right| {
                left < right
            }));
        Self::new(name, filter)
    }

    pub fn gt(name: impl Into<String>, value: impl Into<Prop>) -> Self {
        let filter =
            PropertyValueFilter::ByValue(PropValueCmp::new(value.into(), |left, right| {
                left > right
            }));
        Self::new(name, filter)
    }

    pub fn any(name: impl Into<String>, values: impl IntoIterator<Item = impl Into<Prop>>) -> Self {
        let filter =
            PropertyValueFilter::In(Arc::new(values.into_iter().map(|v| v.into()).collect()));
        Self::new(name, filter)
    }

    pub fn not_any(
        name: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<Prop>>,
    ) -> Self {
        let filter =
            PropertyValueFilter::NotIn(Arc::new(values.into_iter().map(|v| v.into()).collect()));
        Self::new(name, filter)
    }

    pub fn is_none(name: impl Into<String>) -> Self {
        let filter = PropertyValueFilter::HasNot;
        Self::new(name, filter)
    }

    pub fn is_some(name: impl Into<String>) -> Self {
        let filter = PropertyValueFilter::Has;
        Self::new(name, filter)
    }
}

impl PropertyValueFilter {
    fn filter(&self, value: Option<&Prop>) -> bool {
        match self {
            PropertyValueFilter::ByValue(filter) => value.filter(|&v| filter.filter(v)).is_some(),
            PropertyValueFilter::Has => value.is_some(),
            PropertyValueFilter::HasNot => value.is_none(),
            PropertyValueFilter::In(set) => value.filter(|&v| set.contains(v)).is_some(),
            PropertyValueFilter::NotIn(set) => match value {
                Some(value) => !set.contains(value),
                None => true,
            },
        }
    }
}

impl From<PropValueCmp> for PropertyValueFilter {
    fn from(value: PropValueCmp) -> Self {
        Self::ByValue(value)
    }
}

impl PropValueCmp {
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

fn get_ids_and_check_type(
    meta: &Meta,
    property: &str,
    dtype: PropType,
) -> Result<(Option<usize>, Option<usize>), GraphError> {
    let t_prop_id = meta
        .temporal_prop_meta()
        .get_and_validate(property, dtype.clone())?;
    let c_prop_id = meta.const_prop_meta().get_and_validate(property, dtype)?;
    Ok((t_prop_id, c_prop_id))
}

fn get_ids(meta: &Meta, property: &str) -> (Option<usize>, Option<usize>) {
    let t_prop_id = meta.temporal_prop_meta().get_id(property);
    let c_prop_id = meta.const_prop_meta().get_id(property);
    (t_prop_id, c_prop_id)
}
