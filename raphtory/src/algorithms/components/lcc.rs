use crate::{
    algorithms::components::connected_components::weakly_connected_components,
    db::{
        api::view::{GraphViewOps, StaticGraphViewOps},
        graph::views::node_subgraph::NodeSubgraph,
    },
    prelude::Graph,
};

/// Gives the large connected component of a graph.
/// The large connected component is the largest (i.e., with the highest number of nodes)
/// connected sub-graph of the network.
///
/// # Example Usage:
///
/// g.largest_connected_component()
///
/// # Returns:
///
/// A raphtory graph, which essentially is a sub-graph of the graph `g`
///
pub trait LargestConnectedComponent {
    fn largest_connected_component(&self) -> NodeSubgraph<Self>
    where
        Self: StaticGraphViewOps;
}

impl LargestConnectedComponent for Graph {
    fn largest_connected_component(&self) -> NodeSubgraph<Self>
    where
        Self: StaticGraphViewOps,
    {
        let mut connected_components_map =
            weakly_connected_components(self, usize::MAX, None).group_by();
        let mut lcc_key = 0;
        let mut key_length = 0;
        let mut is_tie = false;

        for (key, value) in &connected_components_map {
            let length = value.len();
            if length > key_length {
                key_length = length;
                lcc_key = *key;
                is_tie = false;
            } else if length == key_length {
                is_tie = true
            }
        }
        if is_tie {
            println!("Warning: The graph has two or more connected components that are both the largest. \
            The returned component has been picked arbitrarily.");
        }
        return match connected_components_map.remove(&lcc_key) {
            Some(nodes) => self.subgraph(nodes),
            None => self.subgraph(self.nodes()),
        };
    }
}

#[cfg(test)]
mod largest_connected_component_test {
    use super::*;
    use crate::{
        db::api::view::GraphViewOps,
        prelude::{AdditionOps, Graph, NO_PROPS},
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
