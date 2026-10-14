use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                GraphView, Immutable, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
                InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, InternalNodeFilterOps,
                Static,
            },
        },
        graph::views::filter::{
            admits_edge, admits_edge_layer, admits_exploded_edge, admits_node, restricts_every_kind,
        },
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

/// The union of two filtered views of the same graph, carrying the view
/// (`graph`) their union resolves to.
#[derive(Debug, Clone)]
pub struct OrFilteredGraph<G, L, R> {
    pub(crate) graph: G,
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<G, L, R> OrFilteredGraph<G, L, R> {
    pub fn new(graph: G, left: L, right: R) -> Self {
        Self { graph, left, right }
    }
}

impl<G, L, R> Base for OrFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for OrFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for OrFilteredGraph<G, L, R> {}

impl<'graph, G: GraphViewOps<'graph>, L, R> InheritCoreGraphOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritLayerOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritListOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodePropertySchemaOps
    for OrFilteredGraph<G, L, R>
{
}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritEdgePropertySchemaOps
    for OrFilteredGraph<G, L, R>
{
}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for OrFilteredGraph<G, L, R> {}

/// Either operand admits the entity, as `admits_*` defines admitting. A kind
/// is restricted only when *both* operands restrict it, since either one
/// admitting an entity is enough — and an operand restricting by a view
/// restricts every kind.
impl<G, L: GraphView, R: GraphView> InternalNodeFilterOps for OrFilteredGraph<G, L, R> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        (self.left.internal_nodes_filtered() || restricts_every_kind(&self.left))
            && (self.right.internal_nodes_filtered() || restricts_every_kind(&self.right))
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        admits_node(&self.left, node) || admits_node(&self.right, node)
    }
}

impl<G, L: GraphView, R: GraphView> InternalEdgeLayerFilterOps for OrFilteredGraph<G, L, R> {
    fn internal_edge_layer_filtered(&self) -> bool {
        (self.left.internal_edge_layer_filtered() || restricts_every_kind(&self.left))
            && (self.right.internal_edge_layer_filtered() || restricts_every_kind(&self.right))
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        admits_edge_layer(&self.left, edge, layer) || admits_edge_layer(&self.right, edge, layer)
    }
}

impl<G, L: GraphView, R: GraphView> InternalExplodedEdgeFilterOps for OrFilteredGraph<G, L, R> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        (self.left.internal_exploded_edge_filtered() || restricts_every_kind(&self.left))
            && (self.right.internal_exploded_edge_filtered() || restricts_every_kind(&self.right))
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
        admits_exploded_edge(&self.left, eid, t) || admits_exploded_edge(&self.right, eid, t)
    }
}

impl<G, L: GraphView, R: GraphView> InternalEdgeFilterOps for OrFilteredGraph<G, L, R> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        (self.left.internal_edge_filtered() || restricts_every_kind(&self.left))
            && (self.right.internal_edge_filtered() || restricts_every_kind(&self.right))
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        admits_edge(&self.left, edge) || admits_edge(&self.right, edge)
    }
}
