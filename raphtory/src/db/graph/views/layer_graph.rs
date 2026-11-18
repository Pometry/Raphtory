use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                EdgeList, GraphView, Immutable, InheritEdgeHistoryFilter,
                InheritExplodedEdgeFilterOps, InheritMaterialize, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InnerFilterOps, InternalEdgeFilterOps,
                InternalEdgeLayerFilterOps, InternalLayerOps, InternalNodeFilterOps, ListOps,
                NodeList, Static,
            },
        },
        graph::views::window_graph::{ViewMasks, WindowMasks},
    },
};
use raphtory_api::{
    core::entities::{LayerId, EID},
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};
use std::fmt::{Debug, Formatter};
use storage::EdgeEntryRef;

#[derive(Clone)]
pub struct LayeredGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layers: LayerIds,
    /// Time-index masks for these layers of a windowed `graph`.
    masks: ViewMasks,
}

impl<G: GraphView> Immutable for LayeredGraph<G> {}

impl<G> Static for LayeredGraph<G> {}

impl<G: GraphView + Debug> Debug for LayeredGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredGraph")
            .field("graph", &self.graph as &dyn Debug)
            .field("layers", &self.layers)
            .finish()
    }
}

impl<G: GraphView> Base for LayeredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphView> InheritTimeSemantics for LayeredGraph<G> {}

impl<G: GraphView> ListOps for LayeredGraph<G> {
    fn node_list(&self) -> NodeList {
        match self.masks() {
            Some(masks) => masks.node_list(),
            None => self.graph.node_list(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }

    fn node_count(&self) -> Option<usize> {
        match self.masks() {
            Some(masks) => Some(masks.node_count()),
            None => self.graph.node_count().filter(|_| self.layers.is_all()),
        }
    }

    fn edge_count(&self) -> Option<usize> {
        match self.masks() {
            Some(masks) => Some(masks.edge_count()),
            None => self.graph.edge_count().filter(|_| self.layers.is_all()),
        }
    }

    fn window_masks(&self, layer_ids: &LayerIds) -> Option<WindowMasks> {
        self.graph.window_masks(layer_ids)
    }
}

impl<G: GraphView> InheritCoreGraphOps for LayeredGraph<G> {}

impl<G: GraphView> InheritMaterialize for LayeredGraph<G> {}

impl<G: GraphView> InheritPropertiesOps for LayeredGraph<G> {}

impl<G: GraphView> InheritNodePropertySchemaOps for LayeredGraph<G> {}

impl<G: GraphView> InheritEdgePropertySchemaOps for LayeredGraph<G> {}

impl<G: GraphView> InheritStorageOps for LayeredGraph<G> {}

impl<G: GraphView> InheritNodeHistoryFilter for LayeredGraph<G> {}

impl<G: GraphView> InheritEdgeHistoryFilter for LayeredGraph<G> {}
impl<G: GraphView> InternalNodeFilterOps for LayeredGraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered()
    }

    fn internal_node_list_trusted(&self) -> bool {
        // after applying a layer, previously filtered lists can no longer be trusted
        self.masks.resolved().is_some()
            || (self.graph.internal_node_list_trusted()
                && (self.layers.is_all() || self.graph.node_list().is_empty()))
    }

    fn node_filter_includes_window_filter(&self) -> bool {
        self.masks.resolved().is_some()
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        !self.graph.internal_nodes_filtered()
            || (self.graph.edge_filter_includes_node_filter() && self.layers.is_all())
    }

    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        !self.graph.internal_nodes_filtered()
            || (self.graph.edge_layer_filter_includes_node_filter() && self.layers.is_all())
    }

    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        !self.graph.internal_nodes_filtered()
            || (self.graph.exploded_edge_filter_includes_node_filter() && self.layers.is_all())
    }

    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.masks
            .resolved()
            .is_none_or(|masks| masks.has_node(node.vid()))
            && self.graph.internal_filter_node(node, layer_ids)
    }
}

impl<G: GraphView> LayeredGraph<G> {
    pub fn new(graph: G, layers: LayerIds) -> Self {
        Self {
            graph,
            layers,
            masks: ViewMasks::default(),
        }
    }

    /// Always scans.
    pub(crate) fn unmasked(graph: G, layers: LayerIds) -> Self {
        Self {
            graph,
            layers,
            masks: ViewMasks::none(),
        }
    }

    fn masks(&self) -> Option<&WindowMasks> {
        self.masks.get_or_resolve(|| {
            // masks exist only for an otherwise unfiltered window
            if self.layers.is_all() || self.graph.filtered_inner() || !self.graph.window_filtered()
            {
                None
            } else {
                self.graph.window_masks(&self.layers)
            }
        })
    }
}

impl<G: GraphView> InternalLayerOps for LayeredGraph<G> {
    fn layer_ids(&self) -> &LayerIds {
        &self.layers
    }
}

impl<G: GraphView> InternalEdgeLayerFilterOps for LayeredGraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        !self.graph.internal_edge_layer_filtered()
            || (self.graph.internal_layer_filter_edge_list_trusted() && self.layers.is_all())
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        // can no longer trust this as additional nodes might need to be removed
        !self.graph.internal_edge_layer_filtered()
            || (self.graph.node_filter_includes_edge_filter() && self.layers.is_all())
    }

    fn edge_filter_includes_edge_layer_filter(&self) -> bool {
        // can no longer trust this as additional edges might need to be removed
        !self.graph.internal_edge_layer_filtered()
            || (self.graph.edge_filter_includes_edge_layer_filter() && self.layers.is_all())
    }

    fn exploded_edge_filter_includes_edge_layer_filter(&self) -> bool {
        // exploded edges are always within a layer
        self.graph.exploded_edge_filter_includes_edge_layer_filter()
    }
}

impl<G: GraphView> InternalEdgeFilterOps for LayeredGraph<G> {
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered()
    }

    fn internal_edge_list_trusted(&self) -> bool {
        self.graph.internal_edge_list_trusted()
    }

    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.masks
            .resolved()
            .is_none_or(|masks| masks.has_edge(edge.eid()))
            && self.graph.internal_filter_edge(edge, layer_ids)
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        self.graph.node_filter_includes_edge_filter()
    }

    fn exploded_edge_filter_includes_edge_filter(&self) -> bool {
        self.graph.exploded_edge_filter_includes_edge_filter()
    }

    fn edge_layer_filter_includes_edge_filter(&self) -> bool {
        self.graph.edge_layer_filter_includes_edge_filter()
    }

    // the window's own masks cover more layers
    fn edge_filter_includes_window_filter(&self) -> bool {
        self.masks.resolved().is_some()
    }

    fn internal_filter_edge_id(&self, eid: EID, layer_ids: &LayerIds) -> Option<bool> {
        match self.masks.resolved() {
            Some(masks) if layer_ids == &self.layers => Some(masks.has_edge(eid)),
            _ => self.graph.internal_filter_edge_id(eid, layer_ids),
        }
    }
}

impl<G: GraphView> InheritExplodedEdgeFilterOps for LayeredGraph<G> {}
