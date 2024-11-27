use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::api::{
        properties::internal::InheritPropertiesOps,
        state::Index,
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        },
        view::internal::{
            Base, EdgeFilterOps, EdgeList, Immutable, InheritCoreOps, InheritLayerOps,
            InheritMaterialize, InheritTimeSemantics, ListOps, NodeFilterOps, NodeList, Static,
        },
    },
    prelude::GraphViewOps,
};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct NodeSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) nodes: Index<VID>,
}

impl<G> Static for NodeSubgraph<G> {}

impl<'graph, G: Debug + 'graph> Debug for NodeSubgraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSubgraph")
            .field("graph", &self.graph as &dyn Debug)
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
    pub fn new(graph: G, nodes: impl IntoIterator<Item = impl AsNodeRef>) -> Self {
        let nodes = nodes
            .into_iter()
            .flat_map(|v| graph.internalise_node(v.as_node_ref()));
        let mut nodes: Vec<_> = if graph.nodes_filtered() {
            nodes.filter(|n| graph.has_node(*n)).collect()
        } else {
            nodes.collect()
        };
        nodes.sort();
        let nodes = Index::new(nodes, graph.unfiltered_num_nodes());
        Self { graph, nodes }
    }
}

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
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self.nodes.contains(&edge.src())
            && self.nodes.contains(&edge.dst())
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeSubgraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }
    // FIXME: should use list version and make this true
    fn node_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids) && self.nodes.contains(&node.vid())
    }
}

impl<'graph, G: GraphViewOps<'graph>> ListOps for NodeSubgraph<G> {
    fn node_list(&self) -> NodeList {
        NodeList::List {
            nodes: self.nodes.clone(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }
}

#[cfg(test)]
mod subgraph_tests {
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
            let sg = graph.subgraph([1, 2]);

            let actual = sg.materialize().unwrap().into_events().unwrap();
            assert_graph_equal(&actual, &sg);
        });
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
        test_storage!(&graph, |graph| {
            let subgraph = graph.subgraph(graph.nodes().into_iter().filter(|v| v.degree() > 1));
            let ts = triangle_count(&subgraph, None);
            let tg = triangle_count(graph, None);
            assert_eq!(ts, tg)
        });
    }

    #[test]
    fn layer_materialize() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        graph.add_edge(0, 3, 4, NO_PROPS, Some("2")).unwrap();

        test_storage!(&graph, |graph| {
            let sg = graph.subgraph([1, 2]);
            let sgm = sg.materialize().unwrap();
            assert_eq!(
                sg.unique_layers().collect_vec(),
                sgm.unique_layers().collect_vec()
            );
        });
    }
}
