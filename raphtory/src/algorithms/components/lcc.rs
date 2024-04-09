use crate::{
    algorithms::{components::connected_components::weakly_connected_components},
    db::{
        api::view::{StaticGraphViewOps, GraphViewOps},
        graph::views::node_subgraph::NodeSubgraph,
    },
    prelude::Graph,
};
use std::collections::HashMap;

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
        let connected_components_map = weakly_connected_components(self, usize::MAX, None).get_all_with_names();
        let mut lcc_id: (u64, usize) = (0, 0);
        let mut component_sizes: HashMap<u64, usize> = HashMap::new();
        let mut is_tie: bool = false;

        for &component_id in connected_components_map.values() {
            let count = component_sizes.entry(component_id).or_insert(0);
            *count += 1;
            if *count > lcc_id.1 {
                lcc_id = (component_id, *count);
                is_tie = false;
            } else if *count == lcc_id.1 && component_id != lcc_id.0 {
                is_tie = true;
            }
        }

        if is_tie{
            println!("Warning: The graph has two or more connected components that are both the largest. \
            The returned component has been picked arbitrarily.");
        }

        let lcc_nodes: Vec<String> = connected_components_map
            .iter()
            .filter(|&(_, &cc_id)| cc_id == lcc_id.0)
            .map(|(node, _)| node.clone())
            .collect();

        self.subgraph(lcc_nodes)
    }
}

#[cfg(test)]
mod largest_connected_component_test {
    use super::*;
    use crate::{
        prelude::{AdditionOps, Graph, NO_PROPS},
        db::api::view::{GraphViewOps}
    };

    #[test]
    fn test_empty_graph() {
        let graph = Graph::new();
        let subgraph = graph.largest_connected_component();
        assert!(subgraph.is_empty(), "The subgraph of an empty graph should be empty");
    }

    #[test]
    fn test_single_connected_component() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (2, 2, 1),
            (3, 3, 1),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        let subgraph = graph.largest_connected_component();

        let expected_nodes = vec![1, 2, 3];
        for node in expected_nodes {
            assert_eq!(subgraph.has_node(node), true, "Node {} should be in the largest connected component.", node);
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

            (4, 40, 41)
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        let subgraph = graph.largest_connected_component();
        let expected_nodes = vec![1, 2, 3];
        for node in expected_nodes {
            assert_eq!(subgraph.has_node(node), true, "Node {} should be in the largest connected component.", node);
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


