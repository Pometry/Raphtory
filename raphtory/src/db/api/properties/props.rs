use crate::{
    core::{storage::locked_view::LockedView, Prop},
    db::api::properties::{
        internal::*, static_props::StaticProperties, temporal_props::TemporalProperties,
    },
};
use std::collections::HashMap;

/// View of the properties of an entity (graph|vertex|edge)
#[derive(Clone)]
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
    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<Prop> {
        self.props
            .get_temporal_property(key.as_ref())
            .and_then(|k| self.props.temporal_value(&k))
            .or_else(|| self.props.get_static_property(key.as_ref()))
    }

    /// Check if property `key` exists.
    pub fn contains<Q: AsRef<str>>(&self, key: Q) -> bool {
        self.get(key).is_some()
    }

    pub fn keys<'a>(&'a self) -> impl Iterator<Item = LockedView<'a, String>> + 'a {
        self.props.temporal_property_keys().chain(
            self.props
                .static_property_keys()
                .filter(|k| self.props.get_temporal_property(k).is_none()),
        )
    }

    pub fn values(&self) -> impl Iterator<Item = Prop> + '_ {
        self.keys().map(|k| self.get(&k).unwrap())
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (LockedView<'a, String>, Prop)> + 'a {
        self.keys().zip(self.values())
    }

    /// Get a view of the temporal properties only.
    pub fn temporal(&self) -> TemporalProperties<P> {
        TemporalProperties::new(self.props.clone())
    }

    /// Get a view of the static properties (meta-data) only.
    pub fn meta(&self) -> StaticProperties<P> {
        StaticProperties::new(self.props.clone())
    }

    pub fn as_vec(&self) -> Vec<(String, Prop)> {
        self.iter().map(|(k, v)| (k.clone(), v)).collect()
    }

    pub fn as_map(&self) -> HashMap<String, Prop> {
        self.iter().map(|(k, v)| (k.clone(), v)).collect()
    }
}

impl<P: PropertiesOps + Clone> IntoIterator for Properties<P> {
    type Item = (String, Prop);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys: Vec<_> = self.keys().map(|k| k.clone()).collect();
        let vals: Vec<_> = self.values().collect();
        Box::new(keys.into_iter().zip(vals))
    }
}
