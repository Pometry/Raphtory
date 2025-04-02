use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            view::{
                internal::{
                    Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                    InheritStorageOps, InheritTimeSemantics, NodeFilterOps, Static,
                },
                node::NodeViewOps,
                Base,
            },
        },
        graph::{
            node::NodeView,
            views::filter::{internal::InternalNodeFilterOps, CompositeNodeFilter},
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::LayerIds, storage::arc_str::OptionAsStr};

#[derive(Debug, Clone)]
pub struct NodeFilteredGraph<G> {
    graph: G,
    filter: CompositeNodeFilter,
}

impl<'graph, G> NodeFilteredGraph<G> {
    pub(crate) fn new(graph: G, filter: CompositeNodeFilter) -> Self {
        Self { graph, filter }
    }
}

impl InternalNodeFilterOps for CompositeNodeFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        Ok(NodeFilteredGraph::new(graph, self))
    }
}

impl<'graph, G> Base for NodeFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodeFilteredGraph<G> {}
impl<G> Immutable for NodeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeFilteredGraph<G> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.filter_node(node, layer_ids) {
            match &self.filter {
                CompositeNodeFilter::Node(filter) => filter.matches_node(&self.graph, node),
                CompositeNodeFilter::Property(filter) => {
                    let t_prop_id = filter
                        .resolve_temporal_prop_ids(self.graph.node_meta())
                        .unwrap_or(None);
                    let c_prop_id = filter
                        .resolve_constant_prop_ids(self.graph.node_meta())
                        .unwrap_or(None);
                    filter.matches_node(&self.graph, t_prop_id, c_prop_id, node)
                }
                CompositeNodeFilter::And(left, right) => {
                    let left_filter = NodeFilteredGraph {
                        graph: self.graph.clone(),
                        filter: (**left).clone(),
                    };
                    let right_filter = NodeFilteredGraph {
                        graph: self.graph.clone(),
                        filter: (**right).clone(),
                    };
                    left_filter.filter_node(node.clone(), layer_ids)
                        && right_filter.filter_node(node, layer_ids)
                }
                CompositeNodeFilter::Or(left, right) => {
                    let left_filter = NodeFilteredGraph {
                        graph: self.graph.clone(),
                        filter: (**left).clone(),
                    };
                    let right_filter = NodeFilteredGraph {
                        graph: self.graph.clone(),
                        filter: (**right).clone(),
                    };
                    left_filter.filter_node(node.clone(), layer_ids)
                        || right_filter.filter_node(node, layer_ids)
                }
            }
        } else {
            false
        }
    }
}
