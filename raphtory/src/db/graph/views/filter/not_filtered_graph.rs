use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                FilterOps, GraphView, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                InternalExplodedEdgeFilterOps, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{admits_edge, admits_edge_layer, admits_exploded_edge, admits_node},
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerId, LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};

/// The complement of a filtered view (`filter`) of the same graph, carrying
/// the view (`graph`) the negation resolves to.
#[derive(Debug, Clone)]
pub struct NotFilteredGraph<G, T> {
    pub(crate) graph: G,
    pub(crate) filter: T,
}

impl<G, T> NotFilteredGraph<G, T> {
    pub fn new(graph: G, filter: T) -> Self {
        Self { graph, filter }
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
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodePropertySchemaOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgePropertySchemaOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for NotFilteredGraph<G, T> {}

// An entity of a kind the inner expression restricts is admitted when the
// inner expression does not admit it; a kind it does not restrict passes
// through, so an expression testing only edges leaves nodes alone.
//
// Which kinds those are comes from the inner expression's own per-kind hooks,
// never from a view it carries: negating a view alone resolves to the
// complement view and never reaches this graph, so a view here always sits
// beside a predicate, and counting it as restricting every kind would negate
// node membership too and drop edges whose endpoints are in the view.
// `admits_*` still consults the inner expression's composed filter, so the
// view counts for the kinds it does restrict.
impl<G: GraphView, T: GraphView> InternalNodeFilterOps for NotFilteredGraph<G, T> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered() || self.filter.internal_nodes_filtered()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids) && {
            !self.filter.internal_nodes_filtered() || !admits_node(&self.filter, node)
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

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer) && {
            !self.filter.internal_edge_layer_filtered()
                || !admits_edge_layer(&self.filter, edge, layer)
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
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.graph.filter_exploded_edge(eid, t) && {
            !self.filter.internal_exploded_edge_filtered()
                || !admits_exploded_edge(&self.filter, eid, t)
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
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_edge(edge, layer_ids) && {
            !self.filter.internal_edge_filtered() || !admits_edge(&self.filter, edge)
        }
    }
}
