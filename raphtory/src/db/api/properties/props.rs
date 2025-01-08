use crate::{
    core::Prop,
    db::api::properties::{
        constant_props::ConstantProperties, internal::*, temporal_props::TemporalProperties,
    },
};
use raphtory_api::core::storage::arc_str::ArcStr;
use std::collections::HashMap;

/// View of the properties of an entity (graph|node|edge)
#[derive(Clone, Debug)]
pub struct Properties<P: PropertiesOps + Clone> {
    pub(crate) props: P,
}

impl<P: PropertiesOps + Clone> Properties<P> {
    pub fn new(props: P) -> Properties<P> {
        Self { props }
    }

    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to static properties.
    pub fn get(&self, key: &str) -> Option<Prop> {
        self.props
            .get_temporal_prop_id(key)
            .and_then(|k| self.props.temporal_value(k))
            .or_else(|| {
                self.props
                    .get_const_prop_id(key)
                    .and_then(|id| self.props.get_const_prop(id))
            })
    }

    pub fn get_by_id(&self, id: usize) -> Option<Prop> {
        self.props
            .temporal_value(id)
            .or_else(|| self.props.get_const_prop(id))
    }

    /// Check if property `key` exists.
    pub fn contains(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Iterate over all property keys
    pub fn keys(&self) -> impl Iterator<Item = ArcStr> + '_ {
        self.props.temporal_prop_keys().chain(
            self.props
                .const_prop_keys()
                .filter(|k| self.props.get_temporal_prop_id(k).is_none()),
        )
    }

    /// Iterate over all property values
    pub fn values(&self) -> impl Iterator<Item = Option<Prop>> + '_ {
        self.keys().map(move |k| self.get(&k))
    }

    /// Iterate over all property key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (ArcStr, Option<Prop>)> + '_ {
        self.keys().zip(self.values())
    }

    /// Get a view of the temporal properties only.
    pub fn temporal(&self) -> TemporalProperties<P> {
        TemporalProperties::new(self.props.clone())
    }

    /// Get a view of the constant properties (meta-data) only.
    pub fn constant<'a>(&self) -> ConstantProperties<'a, P> {
        ConstantProperties::new(self.props.clone())
    }

    /// Collect properties into vector
    pub fn as_vec(&self) -> Vec<(ArcStr, Prop)> {
        self.iter()
            .filter_map(|(k, v)| v.map(move |v| (k, v)))
            .collect()
    }

    /// Collect properties into map
    pub fn as_map(&self) -> HashMap<ArcStr, Prop> {
        self.iter()
            .filter_map(|(k, v)| v.map(move |v| (k, v)))
            .collect()
    }

    /// Number of properties
    pub fn len(&self) -> usize {
        self.keys().count()
    }

    /// Check if there are no properties
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<P: PropertiesOps + Clone> IntoIterator for Properties<P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys: Vec<_> = self.keys().collect();
        let vals: Vec<_> = self.values().collect();
        Box::new(keys.into_iter().zip(vals))
    }
}

impl<'a, P: PropertiesOps + Clone + 'a> IntoIterator for &'a Properties<P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}
