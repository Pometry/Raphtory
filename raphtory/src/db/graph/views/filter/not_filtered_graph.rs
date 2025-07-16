use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                FilterOps, GraphView, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                InternalExplodedEdgeFilterOps, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateFilter, model::NotFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerIds, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{
        edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};

#[derive(Debug, Clone)]
pub struct NotFilteredGraph<G, T> {
    graph: G,
    filter: T,
}

impl<T: CreateFilter> CreateFilter for NotFilter<T> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = NotFilteredGraph<G, T::EntityFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.0.create_filter(graph.clone())?;
        Ok(NotFilteredGraph { graph, filter })
    }
}

impl<G, T> Base for NotFilteredGraph<G, T> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, T> Static for NotFilteredGraph<G, T> {}
impl<G, T> Immutable for NotFilteredGraph<G, T> {}

impl<'graph, G: GraphViewOps<'graph>, T> InheritCoreGraphOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritStorageOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritLayerOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritListOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritMaterialize for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritPropertiesOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for NotFilteredGraph<G, T> {}

impl<G: GraphView, T: GraphView> InternalNodeFilterOps for NotFilteredGraph<G, T> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered() || self.filter.internal_nodes_filtered()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids) && {
            !self.filter.internal_nodes_filtered()
                || !self
                    .filter
                    .internal_filter_node(node, self.filter.layer_ids())
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: GraphView> InternalEdgeLayerFilterOps
    for NotFilteredGraph<G, T>
{
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered() || self.filter.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer) && {
            !self.filter.internal_edge_layer_filtered()
                || !self.filter.internal_filter_edge_layer(edge, layer)
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: GraphView> InternalExplodedEdgeFilterOps
    for NotFilteredGraph<G, T>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
            || self.filter.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.graph.filter_exploded_edge(eid, t) && {
            !self.filter.internal_exploded_edge_filtered()
                || !self.filter.filter_exploded_edge(eid, t)
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: GraphView> InternalEdgeFilterOps
    for NotFilteredGraph<G, T>
{
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered() || self.filter.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_edge(edge, layer_ids) && {
            !self.filter.internal_edge_filtered()
                || !self
                    .filter
                    .internal_filter_edge(edge, self.filter.layer_ids())
        }
    }
}
