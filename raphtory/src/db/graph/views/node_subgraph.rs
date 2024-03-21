use crate::{
    core::entities::{edges::edge_store::EdgeStore, nodes::node_store::NodeStore, LayerIds, VID},
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            Base, EdgeFilterOps, Immutable, InheritCoreOps, InheritLayerOps, InheritListOps,
            InheritMaterialize, InheritTimeSemantics, NodeFilterOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use rustc_hash::FxHashSet;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct NodeSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) nodes: Arc<FxHashSet<VID>>,
}

impl<G> Static for NodeSubgraph<G> {}

impl<'graph, G: Debug + 'graph> Debug for NodeSubgraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSubgraph")
            .field("graph", &self.graph)
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

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeSubgraph<G> {
    pub fn new(graph: G, nodes: FxHashSet<VID>) -> Self {
        let nodes = Arc::new(nodes);
        Self { graph, nodes }
    }
}

// FIXME: this should use the list version ideally
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for NodeSubgraph<G> {
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
    fn filter_edge(&self, edge: &EdgeStore, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self.nodes.contains(&edge.src)
            && self.nodes.contains(&edge.dst)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeSubgraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }
    // FIXME: should use list version and make this true
    fn node_list_trusted(&self) -> bool {
        false
    }

    fn filter_node(&self, node: &NodeStore, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids) && self.nodes.contains(&node.vid)
    }
}

#[cfg(test)]
mod subgraph_tests {
    use crate::{algorithms::motifs::triangle_count::triangle_count, prelude::*};
    use itertools::Itertools;

    #[test]
    fn test_materialize_no_edges() {
        let g = Graph::new();

        g.add_node(1, 1, NO_PROPS, None).unwrap();
        g.add_node(2, 2, NO_PROPS, None).unwrap();
        let sg = g.subgraph([1, 2]);

        let actual = sg.materialize().unwrap().into_events().unwrap();
        assert_eq!(actual, sg);
    }

    #[test]
    fn test_remove_degree1_triangle_count() {
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
        let subgraph = graph.subgraph(graph.nodes().into_iter().filter(|v| v.degree() > 1));
        let ts = triangle_count(&subgraph, None);
        let tg = triangle_count(&graph, None);
        assert_eq!(ts, tg)
    }

    #[test]
    fn layer_materialize() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        g.add_edge(0, 3, 4, NO_PROPS, Some("2")).unwrap();

        let sg = g.subgraph([1, 2]);
        let sgm = sg.materialize().unwrap();
        assert_eq!(
            sg.unique_layers().collect_vec(),
            sgm.unique_layers().collect_vec()
        );
    }
}
