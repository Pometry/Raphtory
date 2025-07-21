use crate::db::api::properties::{
    constant_props::ConstantProperties, internal::*, temporal_props::TemporalProperties,
};
use raphtory_api::{
    core::{entities::properties::prop::Prop, storage::arc_str::ArcStr},
    iter::IntoDynBoxed,
};
use raphtory_core::utils::iter::GenLockedIter;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
};
use uuid::ClockSequence;

/// View of the properties of an entity (graph|node|edge)
#[derive(Clone)]
pub struct Properties<P: InternalPropertiesOps + Clone> {
    pub(crate) props: P,
}

impl<P: InternalPropertiesOps + Clone> Debug for Properties<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Properties({:?})", self.iter().collect::<Vec<_>>())
    }
}

pub trait PropertiesOps: Send + Sync {
    fn get(&self, key: &str) -> Option<Prop> {
        self.get_by_id(self.get_id(key)?)
    }

    fn get_by_id(&self, id: usize) -> Option<Prop>;

    fn get_id(&self, key: &str) -> Option<usize>;

    /// Check if property `key` exists.
    fn contains(&self, key: &str) -> bool {
        self.get_id(key).is_some()
    }

    /// Iterate over all property keys
    fn keys(&self) -> impl Iterator<Item = ArcStr> + Send + Sync + '_;

    fn ids(&self) -> impl Iterator<Item = usize> + Send + Sync + '_;

    /// Iterate over all property values
    fn values(&self) -> impl Iterator<Item = Option<Prop>> + Send + Sync + '_ {
        self.ids().map(|id| self.get_by_id(id))
    }

    /// Iterate over all property key-value pairs
    fn iter(&self) -> impl Iterator<Item = (ArcStr, Option<Prop>)> + Send + Sync + '_ {
        self.keys().zip(self.values())
    }

    /// Iterate over all valid property key-value pairs
    fn iter_filtered(&self) -> impl Iterator<Item = (ArcStr, Prop)> + Send + Sync + '_ {
        self.iter().flat_map(|(key, value)| Some((key, value?)))
    }

    /// Collect properties into vector
    fn as_vec(&self) -> Vec<(ArcStr, Prop)> {
        self.iter()
            .filter_map(|(k, v)| v.map(move |v| (k, v)))
            .collect()
    }

    /// Collect properties into map
    fn as_map(&self) -> HashMap<ArcStr, Prop> {
        self.iter()
            .filter_map(|(k, v)| v.map(move |v| (k, v)))
            .collect()
    }

    /// Number of properties
    fn len(&self) -> usize {
        self.ids().count()
    }

    /// Check if there are no properties
    fn is_empty(&self) -> bool {
        self.ids().next().is_some()
    }
}

impl<P: InternalPropertiesOps + Clone> Properties<P> {
    pub fn new(props: P) -> Properties<P> {
        Self { props }
    }

    /// Get a view of the temporal properties only.
    pub fn temporal(&self) -> TemporalProperties<P> {
        TemporalProperties::new(self.props.clone())
    }
}

impl<P: InternalPropertiesOps + Clone> PropertiesOps for Properties<P> {
    fn get_by_id(&self, id: usize) -> Option<Prop> {
        self.props.temporal_value(id)
    }

    fn get_id(&self, key: &str) -> Option<usize> {
        self.props.get_temporal_prop_id(key)
    }

    fn ids(&self) -> impl Iterator<Item = usize> + Send + Sync + '_ {
        self.props.temporal_prop_ids()
    }

    /// Iterate over all property keys
    fn keys(&self) -> impl Iterator<Item = ArcStr> + Send + Sync + '_ {
        self.props.temporal_prop_keys()
    }
}

impl<P: InternalPropertiesOps + Clone + 'static> IntoIterator for Properties<P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = GenLockedIter<'static, Properties<P>, (ArcStr, Option<Prop>)>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self, |props| {
            props.keys().zip(props.values()).into_dyn_boxed()
        })
    }
}

impl<'a, P: InternalPropertiesOps + Clone + 'a> IntoIterator for &'a Properties<P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}
