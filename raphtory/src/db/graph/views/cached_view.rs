use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::{
            InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
        },
        state::Index,
        view::internal::{
            EdgeList, EdgeTimeSemanticsOps, FilterOps, GraphView, Immutable,
            InheritEdgeHistoryFilter, InheritLayerOps, InheritMaterialize,
            InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
            InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
            InternalLayerOps, InternalNodeFilterOps, ListOps, NodeList, Static,
        },
    },
    prelude::{GraphViewOps, Layer, LayerOps},
    storage::core_ops::InheritCoreGraphOps,
};
use raphtory_api::{
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, ELID},
        storage::timeindex::{AsTime, EventTime},
    },
    inherit::Base,
};
use raphtory_storage::graph::{
    edges::edge_storage_ops::EdgeStorageOps,
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use rayon::prelude::*;
use roaring::RoaringTreemap;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};
use storage::EdgeEntryRef;

#[derive(Clone)]
pub struct CachedView<G> {
    pub(crate) graph: G,
    pub(crate) global_nodes_mask: Arc<RoaringTreemap>,
    pub(crate) layered_mask: Arc<[(RoaringTreemap, RoaringTreemap, Option<RoaringTreemap>)]>,
}

impl<G> Static for CachedView<G> {}

impl<G: Debug> Debug for CachedView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedView")
            .field("graph", &self.graph)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for CachedView<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for CachedView<G> {}
impl<G: GraphView> InheritTimeSemantics for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> CachedView<G> {
    pub fn new(graph: G) -> Self {
        // Slot 0 is the static layer, which `unique_layers` does not return.
        let mut layered_graphs = vec![graph.layers(Layer::None).ok()];
        for name in graph.unique_layers() {
            let id = graph.get_layer_id(&name).unwrap().0;
            if layered_graphs.len() <= id {
                layered_graphs.resize_with(id + 1, || None);
            }
            layered_graphs[id] = Some(graph.layers(name).unwrap());
        }
        let empty = || vec![RoaringTreemap::new(); layered_graphs.len()];

        let storage = graph.core_graph().lock();
        let (global_nodes_mask, layered_node_masks) = graph
            .node_list()
            .nodes_par_iter(&storage)
            .fold(
                || (RoaringTreemap::new(), empty()),
                |(mut global, mut per_layer), vid| {
                    if let Some(node) = storage.try_core_node(vid) {
                        let (node, id) = (node.as_ref(), vid.as_u64());
                        if graph.filter_node(node) {
                            push(&mut global, id);
                        }
                        for (nodes, layer) in per_layer.iter_mut().zip(&layered_graphs) {
                            if layer.as_ref().is_some_and(|layer| layer.filter_node(node)) {
                                push(nodes, id);
                            }
                        }
                    }
                    (global, per_layer)
                },
            )
            .reduce(
                || (RoaringTreemap::new(), empty()),
                |(global, per_layer), (other_global, other_per_layer)| {
                    (global | other_global, union(per_layer, other_per_layer))
                },
            );

        // Endpoints are checked against the node masks rather than the node filter for efficiency.
        let edges = graph.core_edges();
        let exploded = graph.internal_exploded_edge_filtered();
        let (layered_edge_masks, exploded_ids) = edges
            .par_iter(&LayerIds::All)
            .fold(
                || (empty(), vec![Vec::new(); layered_graphs.len()]),
                |(mut masks, mut exploded_ids), edge| {
                    let (src, dst) = (edge.src().as_u64(), edge.dst().as_u64());
                    for id in edge.layer_ids_iter(&LayerIds::All).map(|id| id.0) {
                        let Some(Some(layer_g)) = layered_graphs.get(id) else {
                            // this layer is filtered out in this graph view
                            continue;
                        };
                        let nodes = &layered_node_masks[id];
                        if nodes.contains(src)
                            && nodes.contains(dst)
                            && layer_g.filter_edge_except_nodes(edge)
                        {
                            push(&mut masks[id], edge.eid().as_u64());
                            if exploded {
                                exploded_ids[id].extend(
                                    graph
                                        .edge_time_semantics()
                                        .edge_exploded(edge, layer_g, layer_g.layer_ids())
                                        .map(|(t, _)| t.i() as u64),
                                );
                            }
                        }
                    }
                    (masks, exploded_ids)
                },
            )
            .reduce(
                || (empty(), vec![Vec::new(); layered_graphs.len()]),
                |(masks, mut exploded_ids), (other_masks, other_exploded_ids)| {
                    for (ids, other) in exploded_ids.iter_mut().zip(other_exploded_ids) {
                        ids.extend(other);
                    }
                    (union(masks, other_masks), exploded_ids)
                },
            );

        // layer views reuse `node_list` (the global mask), so it must hold every layer's nodes
        debug_assert!(
            layered_node_masks
                .iter()
                .all(|nodes| nodes.is_subset(&global_nodes_mask)),
            "a layer shows a node the whole graph does not"
        );

        let layered_mask = layered_node_masks
            .into_iter()
            .zip(layered_edge_masks)
            .zip(exploded_ids)
            .enumerate()
            .map(|(id, ((nodes, edges), exploded_ids))| {
                // the static layer and missing layers get an empty exploded filter
                let named = id != STATIC_GRAPH_LAYER_ID.0 && layered_graphs[id].is_some();
                let exploded_filter = (!named || exploded).then(|| sorted_id_mask(exploded_ids));
                (nodes, edges, exploded_filter)
            })
            .collect();

        Self {
            graph,
            global_nodes_mask: Arc::new(global_nodes_mask),
            layered_mask,
        }
    }
}

/// Ids arriving in ascending storage order will almost always append.
fn push(mask: &mut RoaringTreemap, id: u64) {
    if mask.try_push(id).is_err() {
        mask.insert(id);
    }
}

fn union(mut masks: Vec<RoaringTreemap>, others: Vec<RoaringTreemap>) -> Vec<RoaringTreemap> {
    for (mask, other) in masks.iter_mut().zip(others) {
        *mask |= other;
    }
    masks
}

/// Useful for event ids, which aren't necessarily in storage order, so they are sorted first.
fn sorted_id_mask(mut ids: Vec<u64>) -> RoaringTreemap {
    ids.par_sort_unstable();
    ids.dedup();
    RoaringTreemap::from_sorted_iter(ids).expect("sorted and deduplicated")
}

impl<'graph, G: GraphViewOps<'graph>> ListOps for CachedView<G> {
    /// The global mask, shared rather than copied.
    fn node_list(&self) -> NodeList {
        NodeList::List {
            elems: Index::from_bitmap(self.global_nodes_mask.clone(), true),
        }
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalExplodedEdgeFilterOps for CachedView<G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.graph.internal_exploded_filter_edge_list_trusted()
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.layered_mask
            .get(eid.layer().0)
            .is_some_and(|(_, _, exploded_filter)| {
                exploded_filter
                    .as_ref()
                    .is_none_or(|filter| filter.contains(t.i() as u64))
            })
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for CachedView<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.graph.internal_layer_filter_edge_list_trusted()
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.layered_mask
            .get(layer.0)
            .is_some_and(|(_, edge_filter, _)| edge_filter.contains(edge.eid().as_u64()))
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }

    fn exploded_edge_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }
}
impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for CachedView<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.graph.internal_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        let filter_fn =
            |(_, edges, _): &(RoaringTreemap, RoaringTreemap, Option<RoaringTreemap>)| {
                edges.contains(edge.eid().as_u64())
            };
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.layered_mask.iter().any(filter_fn),
            LayerIds::One(id) => self.layered_mask.get(id.0).is_some_and(filter_fn),
            LayerIds::Multiple(multiple) => multiple
                .iter()
                .any(|id| self.layered_mask.get(id.0).is_some_and(filter_fn)),
        }
    }
    fn edge_filter_includes_window_filter(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_edge_filter(&self) -> bool {
        true
    }

    fn exploded_edge_filter_includes_edge_filter(&self) -> bool {
        true
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        true
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for CachedView<G> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered()
    }
    fn internal_node_list_trusted(&self) -> bool {
        true
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            // The unlayered nodes should still be returned when no layer is selected
            LayerIds::None => self
                .layered_mask
                .get(STATIC_GRAPH_LAYER_ID.0)
                .is_some_and(|(nodes, _, _)| nodes.contains(node.vid().as_u64())),
            LayerIds::All => self.global_nodes_mask.contains(node.vid().as_u64()),
            LayerIds::One(id) => self
                .layered_mask
                .get(id.0)
                .map(|(nodes, _, _)| nodes.contains(node.vid().as_u64()))
                .unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple.iter().any(|id| {
                self.layered_mask
                    .get(id.0)
                    .map(|(nodes, _, _)| nodes.contains(node.vid().as_u64()))
                    .unwrap_or(false)
            }),
        }
    }

    fn node_filter_includes_window_filter(&self) -> bool {
        true
    }
}
