use crate::model::{
    filters::{
        primitives::{NumberFilter, StringFilter, StringVecFilter},
        property::PropertyHasFilter,
    },
    graph::node::Node,
};
use dynamic_graphql::InputObject;
use raphtory::{core::Prop, db::api::view::VertexViewOps};

#[derive(InputObject)]
pub struct NodeFilter {
    names: Option<StringVecFilter>,
    name: Option<StringFilter>,
    node_type: Option<StringFilter>,
    in_degree: Option<NumberFilter>,
    out_degree: Option<NumberFilter>,
    property_has: Option<PropertyHasFilter>,
}

impl NodeFilter {
    pub(crate) fn matches(&self, node: &Node) -> bool {
        if let Some(names_filter) = &self.names {
            if !names_filter.contains(&node.vv.name()) {
                return false;
            }
        }

        if let Some(name_filter) = &self.name {
            if !name_filter.matches(&node.vv.name()) {
                return false;
            }
        }

        if let Some(type_filter) = &self.node_type {
            let node_type = node
                .vv
                .property("type".to_string(), true)
                .unwrap_or(Prop::Str("NONE".to_string()))
                .to_string();
            if !type_filter.matches(&node_type) {
                return false;
            }
        }

        if let Some(in_degree_filter) = &self.in_degree {
            if !in_degree_filter.matches(node.vv.in_degree()) {
                return false;
            }
        }

        if let Some(out_degree_filter) = &self.out_degree {
            if !out_degree_filter.matches(node.vv.out_degree()) {
                return false;
            }
        }

        if let Some(property_has_filter) = &self.property_has {
            if !property_has_filter.matches(&node) {
                return false;
            }
        }

        true
    }
}
