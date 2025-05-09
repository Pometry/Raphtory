use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{storage::graph::nodes::node_ref::NodeStorageRef, view::BoxableGraphView},
        graph::views::filter::{
            internal::{InternalEdgeFilterOps, InternalNodeFilterOps},
            model::{property_filter::PropertyFilter, AndFilter, Filter, NotFilter, OrFilter},
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;
use std::{collections::HashMap, fmt, fmt::Display, ops::Deref, sync::Arc};

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
    Not(Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeNodeFilter::Not(filter) => write!(f, "NOT({})", filter),
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
            CompositeNodeFilter::Not(filter) => !filter.matches_node(
                graph,
                t_prop_ids,
                c_prop_ids,
                node_types_filter,
                layer_ids,
                node,
            ),
        }
    }
}

impl InternalNodeFilterOps for CompositeNodeFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        match self {
            CompositeNodeFilter::Node(i) => match i.field_name.as_str() {
                "node_name" => Ok(Arc::new(NodeNameFilter(i).create_node_filter(graph)?)),
                "node_type" => Ok(Arc::new(NodeTypeFilter(i).create_node_filter(graph)?)),
                _ => {
                    unreachable!()
                }
            },
            CompositeNodeFilter::Property(i) => Ok(Arc::new(i.create_node_filter(graph)?)),
            CompositeNodeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_node_filter(graph)?,
            )),
            CompositeNodeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_node_filter(graph)?,
            )),
            CompositeNodeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_node_filter(graph)?))
            }
        }
    }
}
