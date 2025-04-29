use crate::{
    db::{
        api::storage::graph::nodes::node_ref::NodeStorageRef,
        graph::views::filter::model::{property_filter::PropertyFilter, Filter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;
use std::{collections::HashMap, fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone)]
pub struct NodeNameFilter(pub Filter);

impl Display for NodeNameFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeNameFilter {
    fn from(filter: Filter) -> Self {
        NodeNameFilter(filter)
    }
}

#[derive(Debug, Clone)]
pub struct NodeTypeFilter(pub Filter);

impl Display for NodeTypeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeTypeFilter {
    fn from(filter: Filter) -> Self {
        NodeTypeFilter(filter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter),
    And(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Or(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
        }
    }
}

impl CompositeNodeFilter {
    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_ids: &HashMap<String, usize>,
        c_prop_ids: &HashMap<String, usize>,
        node_types_filter: &Arc<[bool]>,
        layer_ids: &LayerIds,
        node: NodeStorageRef,
    ) -> bool {
        match self {
            CompositeNodeFilter::Node(node_filter) => {
                node_filter.matches_node::<G>(graph, node_types_filter, layer_ids, node)
            }
            CompositeNodeFilter::Property(property_filter) => {
                let prop_name = property_filter.prop_ref.name();
                let t_prop_id = t_prop_ids.get(prop_name).copied();
                let c_prop_id = c_prop_ids.get(prop_name).copied();
                property_filter.matches_node(graph, t_prop_id, c_prop_id, node)
            }
            CompositeNodeFilter::And(left, right) => {
                left.matches_node(
                    graph,
                    t_prop_ids,
                    c_prop_ids,
                    node_types_filter,
                    layer_ids,
                    node,
                ) && right.matches_node(
                    graph,
                    t_prop_ids,
                    c_prop_ids,
                    node_types_filter,
                    layer_ids,
                    node,
                )
            }
            CompositeNodeFilter::Or(left, right) => {
                left.matches_node(
                    graph,
                    t_prop_ids,
                    c_prop_ids,
                    node_types_filter,
                    layer_ids,
                    node,
                ) || right.matches_node(
                    graph,
                    t_prop_ids,
                    c_prop_ids,
                    node_types_filter,
                    layer_ids,
                    node,
                )
            }
        }
    }
}
