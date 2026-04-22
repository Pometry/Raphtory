use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::api::{
        properties::internal::InheritPropertiesOps,
        state::Index,
        view::internal::{
            EdgeList, GraphTimeSemanticsOps, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
            InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
            InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
            InternalNodeFilterOps, ListOps, NodeList, NodeTimeSemanticsOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerId, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{
        edges::{edge_ref::EdgeEntryRef, edge_storage_ops::EdgeStorageOps},
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct NodeSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) nodes: Index<VID>,
}

impl<G> Static for NodeSubgraph<G> {}

impl<G: Debug> Debug for NodeSubgraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSubgraph")
            .field("graph", &self.graph as &dyn Debug)
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for NodeSubgraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeSubgraph<G> {
    pub fn new(graph: G, nodes: impl IntoIterator<Item = impl AsNodeRef>) -> Self {
        let nodes: Index<_> = nodes
            .into_iter()
            .filter_map(|v| (&&graph).node(v).map(|n| n.node))
            .collect();
        let filter = NodeSubgraph {
            graph: &graph,
            nodes: nodes.clone(),
        };
        let time_semantics = filter.node_time_semantics();
        let nodes: Index<_> = nodes
            .into_iter()
            .filter(|vid| time_semantics.node_valid(filter.core_node(*vid).as_ref(), &filter)) // need to bypass higher-level optimisation as it assumes the node list is already filtered
            .collect();
        Self { graph, nodes }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalExplodedEdgeFilterOps for NodeSubgraph<G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_exploded_edge_filter(&self) -> bool {
        self.graph.edge_filter_includes_exploded_edge_filter()
    }

    fn edge_layer_filter_includes_exploded_edge_filter(&self) -> bool {
        self.graph.edge_layer_filter_includes_exploded_edge_filter()
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for NodeSubgraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_edge_layer_filter(&self) -> bool {
        self.graph.edge_filter_includes_edge_layer_filter()
    }

    fn exploded_edge_filter_includes_edge_layer_filter(&self) -> bool {
        self.graph.exploded_edge_filter_includes_edge_layer_filter()
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for NodeSubgraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.nodes.contains(&edge.src())
            && self.nodes.contains(&edge.dst())
            && self.graph.internal_filter_edge(edge, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeSubgraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        true
    }
    fn internal_node_list_trusted(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_layer_filter_includes_node_filter()
    }

    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.graph.exploded_edge_filter_includes_node_filter()
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }
    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids) && self.nodes.contains(&node.vid())
    }
}

impl<'graph, G: GraphViewOps<'graph>> ListOps for NodeSubgraph<G> {
    fn node_list(&self) -> NodeList {
        NodeList::List {
            elems: self.nodes.clone(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }
}
