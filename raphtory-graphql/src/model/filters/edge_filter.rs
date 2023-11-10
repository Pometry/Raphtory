use crate::model::{
    filters::{
        primitive_filter::{StringFilter, StringVecFilter},
        property_filter::PropertyHasFilter,
    },
    graph::edge::Edge,
};
use dynamic_graphql::InputObject;
use raphtory::db::api::view::{EdgeViewOps, VertexViewOps};

#[derive(InputObject, Clone)]
pub struct EdgeFilter {
    node_ids: Option<StringVecFilter>,
    node_names: Option<StringVecFilter>,
    src: Option<StringFilter>,
    dst: Option<StringFilter>,
    property_has: Option<PropertyHasFilter>,
    pub(crate) layer_names: Option<StringVecFilter>,
}

impl EdgeFilter {
    pub(crate) fn matches(&self, edge: &Edge) -> bool {
        // Filters edges where BOTH the src and dst id match one of the ids in the filter
        if let Some(ids_filter) = &self.node_ids {
            let src = edge.ee.src().id();
            let dst = edge.ee.dst().id();
            if !ids_filter.contains(&src.to_string()) || !ids_filter.contains(&dst.to_string()) {
                return false;
            }
        }

        // Filters edges where BOTH the src and dst name match one of the names in the filter
        if let Some(names_filter) = &self.node_names {
            let src = edge.ee.src().name();
            let dst = edge.ee.dst().name();
            if !names_filter.contains(&src) || !names_filter.contains(&dst) {
                return false;
            }
        }

        if let Some(name_filter) = &self.src {
            if !name_filter.matches(&edge.ee.src().name()) {
                return false;
            }
        }

        if let Some(name_filter) = &self.dst {
            if !name_filter.matches(&edge.ee.dst().name()) {
                return false;
            }
        }

        if let Some(name_filter) = &self.layer_names {
            return edge
                .ee
                .layer_names()
                .any(|name| name_filter.contains(&name));
        }

        if let Some(property_has_filter) = &self.property_has {
            if !property_has_filter.matches_edge_properties(&edge) {
                return false;
            }
        }

        true
    }
}
