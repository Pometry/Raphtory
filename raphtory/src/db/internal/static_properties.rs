use crate::core::entities::graph::tgraph::InnerTemporalGraph;
use crate::core::Prop;
use crate::db::api::properties::internal::StaticPropertiesOps;
use crate::db::api::view::internal::CoreGraphOps;

impl<const N: usize> StaticPropertiesOps for InnerTemporalGraph<N> {
    fn static_property_keys(&self) -> Vec<String> {
        self.static_prop_names()
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.static_prop(key)
    }
}
