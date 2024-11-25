use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        },
        view::internal::{
            Base, EdgeFilterOps, Immutable, InheritCoreOps, InheritLayerOps, InheritListOps,
            InheritMaterialize, InheritTimeSemantics, NodeFilterOps, Static,
        },
    },
    prelude::{GraphViewOps, LayerOps},
};
use roaring::RoaringTreemap;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct MaskedGraph<G> {
    pub(crate) graph: G,
    pub(crate) layered_mask: Arc<[(RoaringTreemap, RoaringTreemap)]>,
}

impl<G> Static for MaskedGraph<G> {}

impl<'graph, G: Debug + 'graph> Debug for MaskedGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskedGraph")
            .field("graph", &self.graph as &dyn Debug)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for MaskedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for MaskedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for MaskedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for MaskedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for MaskedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for MaskedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for MaskedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> MaskedGraph<G> {
    pub fn new(graph: G) -> Self {
        let mut layered_masks = vec![];
        for l_name in graph.unique_layers() {
            let l_id = graph.get_layer_id(&l_name).unwrap();
            let layer_g = graph.layers(l_name).unwrap();
            let nodes = layer_g.nodes();
            let edges = layer_g.edges();

            let nodes: RoaringTreemap = nodes.into_iter().map(|id| id.node.as_u64()).collect();
            let edges: RoaringTreemap =
                edges.into_iter().map(|id| id.edge.pid().as_u64()).collect();

            if layered_masks.len() < l_id + 1 {
                layered_masks.resize(l_id + 1, (RoaringTreemap::new(), RoaringTreemap::new()));
            }

            layered_masks[l_id] = (nodes, edges);
        }

        Self {
            graph,
            layered_mask: layered_masks.into(),
        }
    }
}

// FIXME: this should use the list version ideally
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for MaskedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for MaskedGraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        let filter_fn = |(nodes, edges): &(RoaringTreemap, RoaringTreemap)| {
            edges.contains(edge.eid().as_u64())
                && nodes.contains(edge.src().as_u64())
                && nodes.contains(edge.dst().as_u64())
        };
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.layered_mask.iter().any(filter_fn),
            LayerIds::One(id) => self.layered_mask.get(*id).map(filter_fn).unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple
                .iter()
                .any(|id| self.layered_mask.get(id).map(filter_fn).unwrap_or(false)),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for MaskedGraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }
    // FIXME: should use list version and make this true
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .layered_mask
                .iter()
                .any(|(nodes, _)| nodes.contains(node.vid().as_u64())),
            LayerIds::One(id) => self
                .layered_mask
                .get(*id)
                .map(|(nodes, _)| nodes.contains(node.vid().as_u64()))
                .unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple.iter().any(|id| {
                self.layered_mask
                    .get(id)
                    .map(|(nodes, _)| nodes.contains(node.vid().as_u64()))
                    .unwrap_or(false)
            }),
        }
    }
}

#[cfg(test)]
mod masked_graph_tests {
    use crate::{
        algorithms::motifs::triangle_count::triangle_count, db::graph::graph::assert_graph_equal,
        prelude::*, test_storage,
    };
    use itertools::Itertools;

    #[test]
    fn test_materialize_no_edges() {
        let graph = Graph::new();

        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let sg = graph.masked();

            let actual = sg.materialize().unwrap().into_events().unwrap();
            assert_graph_equal(&actual, &sg);
        });
    }

    #[test]
    fn test_mask_the_window_50pc() {
        let graph = Graph::new();
        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];
        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let window = graph.window(12, 24);
            let mask = window.masked();
            let ts = triangle_count(&mask, None);
            let tg = triangle_count(&window, None);
            assert_eq!(ts, tg)
        });
    }
}
