use crate::core::storage::locked_view::LockedView;
use crate::core::Prop;
use crate::db::api::properties::internal::*;
use crate::db::api::properties::static_props::StaticProperties;
use crate::db::api::properties::temporal_props::TemporalProperties;
use std::borrow::Borrow;

/// View of the properties of an entity (graph|vertex|edge)
pub struct Properties<P: PropertiesOps + Clone> {
    props: P,
}

impl<P: PropertiesOps + Clone> Properties<P> {
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
        self.props.temporal_property_keys()
    }

    /// Get a view of the temporal properties only.
    pub fn temporal(&self) -> TemporalProperties<P> {
        TemporalProperties::new(self.props.clone())
    }

    /// Get a view of the static properties (meta-data) only.
    pub fn meta(&self) -> StaticProperties<P> {
        StaticProperties::new(self.props.clone())
    }
}
