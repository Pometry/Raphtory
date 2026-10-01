#[cfg(test)]
mod largest_connected_component_test {
    use raphtory::algorithms::components::*;
    use raphtory::{
        db::api::view::GraphViewOps,
        prelude::{AdditionOps, Graph, NodeViewOps, NO_PROPS},
    };

    #[test]
    fn test_empty_graph() {
        let graph = Graph::new();
        let subgraph = graph.largest_connected_component();
        assert!(
            subgraph.is_empty(),
            "The subgraph of an empty graph should be empty"
        );
    }

    #[test]
    fn test_single_connected_component() {
        let graph = Graph::new();
        let edges = vec![(1, 1, 2), (2, 2, 1), (3, 3, 1)];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        let subgraph = graph.largest_connected_component();

        let expected_nodes = vec![1, 2, 3];
        for node in expected_nodes {
            assert!(
                subgraph.has_node(node),
                "Node {} should be in the largest connected component.",
                node
            );
        }
        assert_eq!(subgraph.count_nodes(), 3);
    }

    #[test]
    fn test_multiple_connected_components() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (2, 2, 1),
            (3, 3, 1),
            (1, 10, 11),
            (2, 20, 21),
            (3, 30, 31),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        for _ in 0..1000 {
            let subgraph = graph.largest_connected_component();
            let expected_nodes = vec![1, 2, 3];
            for node in expected_nodes {
                assert!(
                    subgraph.has_node(node),
                    "Node {} should be in the largest connected component, actual nodes: {:?}",
                    node,
                    subgraph.nodes().id().collect_vec()
                );
            }
            assert_eq!(subgraph.count_nodes(), 3);
        }
    }

    #[test]
    fn test_same_size_connected_components() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 2, 1),
            (1, 3, 1),
            (1, 5, 6),
            (1, 11, 12),
            (1, 12, 11),
            (1, 13, 11),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        let _subgraph = graph.largest_connected_component();
    }
}
