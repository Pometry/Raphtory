#[cfg(all(test, feature = "search"))]
mod graph_index_test {
    use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, IndexMutationOps};

    use raphtory::db::graph::views::filter::model::{
        edge_filter::EdgeFilter, node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
        PropertyFilterFactory,
    };
    use raphtory_test_utils::assertions::{search_edges, search_nodes};

    fn init_nodes_graph(graph: Graph) -> Graph {
        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"), None)
            .unwrap();
        graph
    }

    fn init_edges_graph(graph: Graph) -> Graph {
        graph
            .add_edge(1, 1, 2, [("p1", 1), ("p2", 2)], None)
            .unwrap();
        graph.add_edge(2, 1, 2, [("p6", 6)], None).unwrap();
        graph.add_edge(2, 2, 3, [("p4", 5)], None).unwrap();
        graph
            .add_edge(3, 3, 4, [("p2", 4), ("p3", 3)], None)
            .unwrap();
        graph
    }

    #[test]
    fn test_if_bulk_load_create_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);

        graph.create_index_in_ram().unwrap();
    }

    #[test]
    fn test_if_adding_nodes_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        graph.create_index_in_ram().unwrap();

        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);
    }

    #[test]
    fn test_if_adding_edges_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        // Creates graph index
        graph.create_index_in_ram().unwrap();

        let graph = init_edges_graph(graph);

        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    fn test_node_metadata_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph.node(1).unwrap().add_metadata([("x", 1u64)]).unwrap();

        let filter = NodeFilter.metadata("x").eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);

        graph
            .node(1)
            .unwrap()
            .update_metadata([("x", 2u64)])
            .unwrap();
        let filter = NodeFilter.metadata("x").eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .node(1)
            .unwrap()
            .update_metadata([("x", 2u64)])
            .unwrap();
        let filter = NodeFilter.metadata("x").eq(2u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);
    }

    #[test]
    fn test_edge_metadata_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_edges_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph
            .edge(1, 2)
            .unwrap()
            .add_metadata([("x", 1u64)], None)
            .unwrap();

        let filter = EdgeFilter.metadata("x").eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);

        graph
            .edge(1, 2)
            .unwrap()
            .update_metadata([("x", 2u64)], None)
            .unwrap();
        let filter = EdgeFilter.metadata("x").eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .edge(1, 2)
            .unwrap()
            .update_metadata([("x", 2u64)], None)
            .unwrap();
        let filter = EdgeFilter.metadata("x").eq(2u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);
    }
}
