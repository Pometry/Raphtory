use crate::{
    core::entities::LayerIds,
    db::{
        api::{
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
        graph::views::layer_graph::LayeredGraph,
    },
    prelude::GraphViewOps,
    storage::core_ops::InheritCoreGraphOps,
};
use either::Either;
use raphtory_api::{
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, ELID},
        storage::timeindex::{AsTime, EventTime},
        Direction,
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
        Self::new_with_cancel(graph, || false).expect("never cancelled")
    }

    /// [`Self::new`], giving up with `None` once `cancel` returns true. `cancel` is checked for
    /// every node and edge scanned, so it should be cheap (e.g. an atomic load).
    pub fn new_with_cancel(graph: G, cancel: impl Fn() -> bool + Sync) -> Option<Self> {
        if cancel() {
            return None;
        }
        // One view per layer of `graph`, indexed by layer id. Slot 0 is the private static layer,
        // which is viewed with no layers.
        let num_slots = graph.edge_meta().layer_meta().num_all_fields();
        let mut layered_graphs = vec![Some(LayeredGraph::new(graph.clone(), LayerIds::None))];
        for id in graph.layer_ids().iter(num_slots) {
            if id == STATIC_GRAPH_LAYER_ID {
                continue;
            }
            if layered_graphs.len() <= id.0 {
                layered_graphs.resize_with(id.0 + 1, || None);
            }
            layered_graphs[id.0] = Some(LayeredGraph::new(graph.clone(), LayerIds::One(id)));
        }
        let empty = || vec![RoaringTreemap::new(); layered_graphs.len()];

        // One snapshot for both passes, so the edges are those of the nodes just scanned.
        let storage = graph.core_graph().lock();
        let (global_nodes_mask, layered_node_masks) = graph
            .node_list()
            .nodes_par_iter(&storage)
            .try_fold(
                || (RoaringTreemap::new(), empty()),
                |(mut global, mut per_layer), vid| {
                    if cancel() {
                        return None;
                    }
                    if let Some(node) = storage.try_core_node(vid) {
                        let (node, id) = (node.as_ref(), vid.as_u64());
                        // a layer only shows nodes the whole graph shows
                        if graph.filter_node(node) {
                            push(&mut global, id);
                            for (nodes, layer) in per_layer.iter_mut().zip(&layered_graphs) {
                                if layer.as_ref().is_some_and(|layer| layer.filter_node(node)) {
                                    push(nodes, id);
                                }
                            }
                        }
                    }
                    Some((global, per_layer))
                },
            )
            .try_reduce(
                || (RoaringTreemap::new(), empty()),
                |(global, per_layer), (other_global, other_per_layer)| {
                    Some((global | other_global, union(per_layer, other_per_layer)))
                },
            )?;
        let global_nodes_mask = Arc::new(optimized(global_nodes_mask));

        // Every surviving edge leaves a surviving node in one of the view's layers.
        let edges = storage.owned_edges();
        let nodes = storage.core_nodes();
        let (edges, nodes, layer_ids) = (&edges, &nodes, graph.layer_ids());
        let candidates = Index::from_bitmap(global_nodes_mask.clone(), true)
            .into_par_iter()
            .flat_map_iter(move |vid| {
                nodes
                    .node_entry(vid)
                    .edges_iter(layer_ids, Direction::OUT)
                    .map(move |edge| edges.edge(Either::Right(edge)))
            });

        // Endpoints are checked against the node masks rather than the node filter for efficiency.
        let exploded = graph.internal_exploded_edge_filtered();
        let (layered_edge_masks, exploded_masks) = candidates
            .try_fold(
                || (empty(), empty()),
                |(mut masks, mut exploded_masks), edge| {
                    if cancel() {
                        return None;
                    }
                    let (src, dst) = (edge.src().as_u64(), edge.dst().as_u64());
                    for id in edge.layer_ids_iter(layer_ids).map(|id| id.0) {
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
                                // event ids are not in storage order
                                exploded_masks[id].extend(
                                    graph
                                        .edge_time_semantics()
                                        .edge_exploded(edge, layer_g, layer_g.layer_ids())
                                        .map(|(t, _)| t.i() as u64),
                                );
                            }
                        }
                    }
                    Some((masks, exploded_masks))
                },
            )
            .try_reduce(
                || (empty(), empty()),
                |(masks, exploded_masks), (other_masks, other_exploded_masks)| {
                    Some((
                        union(masks, other_masks),
                        union(exploded_masks, other_exploded_masks),
                    ))
                },
            )?;
        if cancel() {
            return None;
        }

        // layer views reuse `node_list` (the global mask), so it must hold every layer's nodes
        debug_assert!(
            layered_node_masks
                .iter()
                .all(|nodes| nodes.is_subset(&global_nodes_mask)),
            "a layer shows a node the whole graph does not"
        );

        let layered_mask = layered_node_masks
            .into_par_iter()
            .zip(layered_edge_masks)
            .zip(exploded_masks)
            .enumerate()
            .map(|(id, ((nodes, edges), exploded_mask))| {
                // the static layer and missing layers get an empty exploded filter
                let named = id != STATIC_GRAPH_LAYER_ID.0 && layered_graphs[id].is_some();
                let exploded_filter = (!named || exploded).then(|| optimized(exploded_mask));
                (optimized(nodes), optimized(edges), exploded_filter)
            })
            .collect::<Vec<_>>()
            .into();

        Some(Self {
            graph,
            global_nodes_mask,
            layered_mask,
        })
    }
}

/// Run-length encode where that is smaller: masks of mostly visible ids shrink a lot.
fn optimized(mut mask: RoaringTreemap) -> RoaringTreemap {
    mask.optimize();
    mask
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
