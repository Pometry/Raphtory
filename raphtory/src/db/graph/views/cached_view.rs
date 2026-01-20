use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            EdgeTimeSemanticsOps, FilterOps, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
            InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
            InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
            InternalExplodedEdgeFilterOps, InternalLayerOps, InternalNodeFilterOps, Static,
        },
    },
    prelude::{GraphViewOps, LayerOps},
    storage::core_ops::InheritCoreGraphOps,
};
use raphtory_api::{
    core::{
        entities::ELID,
        storage::timeindex::{AsTime, EventTime},
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
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

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> CachedView<G> {
    pub fn new(graph: G) -> Self {
        let mut layered_masks = vec![];
        let global_nodes_mask = Arc::new(
            graph
                .nodes()
                .iter()
                .map(|node| node.node.as_u64())
                .collect(),
        );
        for l_name in graph.unique_layers() {
            let l_id = graph.get_layer_id(&l_name).unwrap();
            let layer_g = graph.layers(l_name).unwrap();

            let nodes = layer_g
                .nodes()
                .par_iter()
                .map(|node| node.node.as_u64())
                .collect::<Vec<_>>();

            let nodes: RoaringTreemap = nodes.into_iter().collect();

            let edges = layer_g.core_edges();

            let edges_chunks = edges
                .as_ref()
                .par_iter(&LayerIds::All)
                .filter(|edge| {
                    layer_g.filter_edge(edge.as_ref())
                        && nodes.contains(edge.src().as_u64())
                        && nodes.contains(edge.dst().as_u64())
                })
                .map(|edge| edge.eid().as_u64())
                .collect_vec_list();
            let edges_filter: RoaringTreemap = edges_chunks.into_iter().flatten().collect();

            let exploded_filter = if graph.internal_exploded_edge_filtered() {
                Some(
                    edges
                        .par_iter(&LayerIds::All)
                        .flat_map_iter(|e| {
                            edges_filter
                                .contains(e.eid().as_u64())
                                .then_some(e)
                                .into_iter()
                                .flat_map(|e| {
                                    let timesemantics = graph.edge_time_semantics();
                                    timesemantics
                                        .edge_exploded(e, &layer_g, layer_g.layer_ids())
                                        .map(|(t, _)| t.i() as u64)
                                })
                        })
                        .collect_vec_list()
                        .into_iter()
                        .flatten()
                        .collect(),
                )
            } else {
                None
            };

            if layered_masks.len() < l_id + 1 {
                layered_masks.resize(
                    l_id + 1,
                    (
                        RoaringTreemap::new(),
                        RoaringTreemap::new(),
                        Some(RoaringTreemap::new()),
                    ),
                );
            }

            layered_masks[l_id] = (nodes, edges_filter, exploded_filter);
        }

        Self {
            graph,
            global_nodes_mask,
            layered_mask: layered_masks.into(),
        }
    }
}

// FIXME: this should use the list version ideally
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for CachedView<G> {}

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
            .get(eid.layer())
            .is_some_and(|(_, _, exploded_filter)| {
                exploded_filter
                    .as_ref()
                    .is_none_or(|filter| filter.contains(t.i() as u64))
            })
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
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

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: usize) -> bool {
        self.layered_mask
            .get(layer)
            .is_some_and(|(_, edge_filter, _)| edge_filter.contains(edge.eid().as_u64()))
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
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
            LayerIds::One(id) => self.layered_mask.get(*id).is_some_and(filter_fn),
            LayerIds::Multiple(multiple) => multiple
                .iter()
                .any(|id| self.layered_mask.get(id).is_some_and(filter_fn)),
        }
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
        self.graph.internal_node_list_trusted()
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
            LayerIds::None => false,
            LayerIds::All => self.global_nodes_mask.contains(node.vid().as_u64()),
            LayerIds::One(id) => self
                .layered_mask
                .get(*id)
                .map(|(nodes, _, _)| nodes.contains(node.vid().as_u64()))
                .unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple.iter().any(|id| {
                self.layered_mask
                    .get(id)
                    .map(|(nodes, _, _)| nodes.contains(node.vid().as_u64()))
                    .unwrap_or(false)
            }),
        }
    }
}
