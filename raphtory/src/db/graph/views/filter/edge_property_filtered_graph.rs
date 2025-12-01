use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            state::ops::NotANodeFilter,
            view::internal::{
                GraphView, Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
                InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, Static,
            },
        },
        graph::views::{
            filter::{
                internal::CreateFilter,
                model::{
                    edge_filter::EdgeFilter, property_filter::PropertyFilter,
                    windowed_filter::Windowed,
                },
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps, TimeOps},
};
use raphtory_api::{core::storage::timeindex::AsTime, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};

#[derive(Debug, Clone)]
pub struct EdgePropertyFilteredGraph<G> {
    graph: G,
    prop_id: usize,
    filter: PropertyFilter<EdgeFilter>,
}

impl<G> EdgePropertyFilteredGraph<G> {
    pub(crate) fn new(graph: G, prop_id: usize, filter: PropertyFilter<EdgeFilter>) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }
}

impl CreateFilter for PropertyFilter<Windowed<EdgeFilter>> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgePropertyFilteredGraph<WindowedGraph<G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.edge_meta(), graph.num_layers() > 1)?;
        let filter = PropertyFilter {
            prop_ref: self.prop_ref,
            prop_value: self.prop_value,
            operator: self.operator,
            ops: self.ops,
            entity: EdgeFilter,
        };
        Ok(EdgePropertyFilteredGraph::new(
            graph.window(self.entity.start.t(), self.entity.end.t()),
            prop_id,
            filter,
        ))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl CreateFilter for PropertyFilter<EdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = EdgePropertyFilteredGraph<G>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.edge_meta(), graph.num_layers() > 1)?;
        Ok(EdgePropertyFilteredGraph::new(graph, prop_id, self))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl<G> Base for EdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgePropertyFilteredGraph<G> {}
impl<G> Immutable for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps
    for EdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for EdgePropertyFilteredGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_edge(edge, layer_ids)
            && self.filter.matches_edge(&self.graph, self.prop_id, edge)
    }
}
