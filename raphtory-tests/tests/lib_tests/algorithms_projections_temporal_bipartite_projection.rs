#[cfg(test)]
mod bipartite_graph_tests {
    use raphtory::{
        algorithms::projections::temporal_bipartite_projection::temporal_bipartite_projection,
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::{assertions::assert_graph_equal, graph::Graph},
        },
        errors::GraphError,
        prelude::NO_PROPS,
    };
    use raphtory_api::core::storage::timeindex::AsTime;

    #[test]
    fn small_delta_test() {
        let g = Graph::new();
        let vs = vec![
            (1, "A", "1"),
            (3, "A", "2"),
            (3, "B", "2"),
            (4, "C", "3"),
            (6, "B", "3"),
            (8, "A", "3"),
            (10, "C", "4"),
            (11, "B", "4"),
        ];
        for (t, src, dst) in &vs {
            g.add_node(*t, *src, NO_PROPS, Some("Left"), None).unwrap();
            g.add_node(*t, *dst, NO_PROPS, Some("Right"), None).unwrap();
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        let new_graph = temporal_bipartite_projection(&g, 1, "Right").unwrap();
        assert!(new_graph.has_edge("A", "B"));
        assert_eq!(
            new_graph
                .edge("A", "B")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(3)
        );
        assert!(new_graph.has_edge("C", "B"));
        assert_eq!(
            new_graph
                .edge("C", "B")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(10)
        );
        assert!(!new_graph.has_edge("A", "C"));
    }

    #[test]
    fn larger_delta_test() {
        let g = Graph::new();
        let vs = vec![
            (1, "A", "1"),
            (3, "A", "2"),
            (3, "B", "2"),
            (4, "C", "3"),
            (6, "B", "3"),
            (8, "A", "3"),
            (10, "C", "4"),
            (11, "B", "4"),
        ];
        for (t, src, dst) in &vs {
            g.add_node(*t, *src, NO_PROPS, Some("Left"), None).unwrap();
            g.add_node(*t, *dst, NO_PROPS, Some("Right"), None).unwrap();
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        let new_graph = temporal_bipartite_projection(&g, 3, "Right").unwrap();

        assert!(new_graph.has_edge("A", "B"));
        assert_eq!(
            new_graph
                .edge("A", "B")
                .unwrap()
                .earliest_time()
                .map(|t| t.t()),
            Some(3)
        );
        assert_eq!(
            new_graph
                .edge("B", "A")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(7)
        );
        assert!(new_graph.has_edge("C", "B"));
        assert_eq!(
            new_graph
                .edge("C", "B")
                .unwrap()
                .earliest_time()
                .map(|t| t.t()),
            Some(5)
        );
        assert_eq!(
            new_graph
                .edge("C", "B")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(10)
        );
        assert!(!new_graph.has_edge("A", "C"));
    }

    #[test]
    fn unknown_node_type_is_an_error() {
        // add_edge creates its endpoints without a node type
        let g = Graph::new();
        g.add_edge(1, "alice", "laptop", NO_PROPS, None).unwrap();
        g.add_edge(2, "bob", "laptop", NO_PROPS, None).unwrap();

        let err = temporal_bipartite_projection(&g, 5, "Item").unwrap_err();
        assert!(matches!(err, GraphError::NodeTypeMissingError(expected) if expected == "Item"))
    }

    #[test]
    fn untyped_nodes_are_fine() {
        let g = Graph::new();
        g.add_node(1, "A", NO_PROPS, Some("Left"), None).unwrap();
        g.add_node(1, "1", NO_PROPS, Some("Right"), None).unwrap();
        g.add_edge(1, "A", "1", NO_PROPS, None).unwrap();
        // this endpoint is created by add_edge and never given a type
        g.add_edge(2, "B", "1", NO_PROPS, None).unwrap();

        let expected = Graph::new();
        expected.add_edge(1, "A", "B", NO_PROPS, None).unwrap();

        let res = temporal_bipartite_projection(&g, 5, "Right").unwrap();
        assert_graph_equal(&res, &expected);
    }

    #[test]
    fn valid_pivot_type_nobody_carries_gives_an_empty_graph() {
        let g = Graph::new();
        g.add_node(1, "A", NO_PROPS, Some("Left"), None).unwrap();
        g.add_node(1, "1", NO_PROPS, Some("Right"), None).unwrap();
        g.add_edge(1, "A", "1", NO_PROPS, None).unwrap();
        g.add_node(1, "B", NO_PROPS, Some("Item"), Some("other"))
            .unwrap();

        let new_graph = temporal_bipartite_projection(&g.default_layer(), 5, "Item").unwrap();
        assert_eq!(new_graph.count_nodes(), 0);
        assert_eq!(new_graph.count_edges(), 0);
    }
}
