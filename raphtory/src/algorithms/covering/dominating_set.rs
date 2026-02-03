use crate::{db::api::view::StaticGraphViewOps};
use crate::db::api::view::node::NodeViewOps;
use raphtory_api::core::{
    entities::{
        VID,
    },
};
use std::{
    collections::HashSet,
};
use crate::db::api::view::graph::GraphViewOps;

#[derive(Default, Clone)]
struct LinkedListNode {
    next: Option<usize>,
    prev: Option<usize>,
    uncovered_count: usize,
    vid: VID
}

struct DominatingSetQueue {
    linked_nodes: Vec<LinkedListNode>,
    uncovered_count_map: Vec<Option<usize>>,
    max_uncovered_count: usize,
}

impl DominatingSetQueue {
    pub fn from_graph<G: StaticGraphViewOps>(g: &G)-> Self {
        let n_nodes = g.count_nodes();
        let mut linked_nodes = vec![LinkedListNode::default(); n_nodes];
        let mut uncovered_count_map = vec![None; n_nodes + 1];
        for node in g.nodes() {
            let vid = node.node;
            let index = vid.index();
            let uncovered_count = node.degree() + 1;
            let current_linked_node = &mut linked_nodes[index];
            current_linked_node.uncovered_count = uncovered_count;
            current_linked_node.vid = vid;
            if let Some(existing_index) = uncovered_count_map[uncovered_count] {
                current_linked_node.next = Some(existing_index);
                linked_nodes[existing_index].prev = Some(index);
            } 
            uncovered_count_map[uncovered_count] = Some(index);
        }
        Self {
            linked_nodes,
            uncovered_count_map,
            max_uncovered_count: n_nodes,
        }
    }

    pub fn maximum(&mut self) -> Option<usize> {
        while self.max_uncovered_count > 0 {
            if let Some(index) = self.uncovered_count_map[self.max_uncovered_count] {
                if let Some(next_index) = self.linked_nodes[index].next {
                    let next_linked_node = &mut self.linked_nodes[next_index];
                    next_linked_node.prev = None;
                    self.uncovered_count_map[self.max_uncovered_count] = Some(next_index);  
                } else {
                    self.uncovered_count_map[self.max_uncovered_count] = None;
                }
                self.linked_nodes[index].next = None;
                return Some(index);
            } 
            self.max_uncovered_count -= 1;
        }
        None
    }
    // uncovered count should be less than the max uncovered count
    pub fn insert(&mut self, index: usize, uncovered_count: usize) {
        self.linked_nodes[index].uncovered_count = uncovered_count;
        if let Some(existing_index) = self.uncovered_count_map[uncovered_count] {
            self.linked_nodes[index].next = Some(existing_index);
            self.linked_nodes[existing_index].prev = Some(index);
        }
        self.uncovered_count_map[uncovered_count] = Some(index);
    }

    pub fn node_details(&self, index: usize) -> (VID, usize) {
        let linked_node = &self.linked_nodes[index];
        (linked_node.vid, linked_node.uncovered_count)
    }
}


pub fn lazy_greedy_dominating_set<G: StaticGraphViewOps>(g: &G) -> HashSet<VID> {
    let n_nodes = g.count_nodes();
    let mut dominating_set: HashSet<VID> = HashSet::new();
    let mut covered_count = 0;
    let mut covered_nodes: Vec<bool> = vec![false; n_nodes];
    let mut queue = DominatingSetQueue::from_graph(g);
    while covered_count < n_nodes {
        let index = queue.maximum().unwrap();
        let (vid, stale_uncovered_count) = queue.node_details(index);
        let node = g.node(vid).unwrap();
        let mut actual_uncovered_count = 0; 
        if !covered_nodes[vid.index()] {
            actual_uncovered_count += 1;
        } 
        for neighbor in node.neighbours() {
            if !covered_nodes[neighbor.node.index()] {
                actual_uncovered_count += 1;
            }
        }
        if actual_uncovered_count == stale_uncovered_count {
            dominating_set.insert(vid);
            if !covered_nodes[vid.index()] {
                covered_nodes[vid.index()] = true;
                covered_count += 1;
            }
            for neighbor in node.neighbours() {
                if !covered_nodes[neighbor.node.index()] {
                    covered_nodes[neighbor.node.index()] = true;
                    covered_count += 1;
                }
            }
        } else {
            queue.insert(index, actual_uncovered_count);
        }
    }
    dominating_set
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    /// Helper function to verify if a set of nodes is a valid dominating set
    fn is_dominating_set<G: StaticGraphViewOps>(graph: &G, ds: &HashSet<VID>) -> bool {
        let mut covered = HashSet::new();
        
        // Add all dominating set nodes and their neighbors to covered set
        for &vid in ds {
            covered.insert(vid);
            if let Some(node) = graph.node(vid) {
                for neighbor in node.neighbours() {
                    covered.insert(neighbor.node);
                }
            }
        }
        
        // Check that all nodes in the graph are covered
        for node in graph.nodes() {
            if !covered.contains(&node.node) {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_empty_graph() {
        let graph = Graph::new();
        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(ds.is_empty(), "Empty graph should have an empty dominating set");
    }

    #[test]
    fn test_single_node_graph() {
        let graph = Graph::new();
        graph.add_node(0, 1, NO_PROPS, None).unwrap();

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 1, "Single node should dominate itself");
        assert!(ds.contains(&VID(0)), "The single node should be in the dominating set");
    }

    #[test]
    fn test_two_connected_nodes() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 1, "One node should be sufficient to dominate both nodes in an edge");
    }

    #[test]
    fn test_star_graph() {
        let graph = Graph::new();
        // Star with center 0 and leaves 1-5
        for i in 1..=5 {
            graph.add_edge(0, 0, i, NO_PROPS, None).unwrap();
        }

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 1, "Star graph center should be the only node in the dominating set");
        assert!(ds.contains(&VID(0)), "Center node should be in the dominating set");
    }

    #[test]
    fn test_path_graph() {
        let graph = Graph::new();
        // Path: 1-2-3-4-5
        for i in 1..5 {
            graph.add_edge(0, i, i + 1, NO_PROPS, None).unwrap();
        }

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        // For a path of 5 nodes, we need at most 2 nodes
        assert!(ds.len() <= 2, "Path of 5 nodes should need at most 2 nodes in dominating set");
    }

    #[test]
    fn test_triangle_graph() {
        let graph = Graph::new();
        // Triangle: 1-2-3-1
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 3, 1, NO_PROPS, None).unwrap();

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 1, "Triangle should need only 1 node in dominating set");
    }

    #[test]
    fn test_complete_graph() {
        let graph = Graph::new();
        // Complete graph K4
        for i in 1..=4 {
            for j in (i+1)..=4 {
                graph.add_edge(0, i, j, NO_PROPS, None).unwrap();
            }
        }

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 1, "Complete graph should need only 1 node in dominating set");
    }

    #[test]
    fn test_disconnected_graph() {
        let graph = Graph::new();
        // Two separate edges: 1-2 and 3-4
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 3, 4, NO_PROPS, None).unwrap();

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 2, "Two disconnected components need at least 2 nodes");
    }

    #[test]
    fn test_cycle_graph() {
        let graph = Graph::new();
        // Cycle: 1-2-3-4-5-1
        for i in 1..=5 {
            let next = if i == 5 { 1 } else { i + 1 };
            graph.add_edge(0, i, next, NO_PROPS, None).unwrap();
        }

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        // For a cycle of 5 nodes, we need at most 2 nodes
        assert!(ds.len() <= 2, "Cycle of 5 nodes should need at most 2 nodes in dominating set");
    }

    #[test]
    fn test_bipartite_graph() {
        let graph = Graph::new();
        // Complete bipartite graph K_{2,3}
        for i in 1..=2 {
            for j in 3..=5 {
                graph.add_edge(0, i, j, NO_PROPS, None).unwrap();
            }
        }

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        // Can be dominated by either all nodes from one partition (2 or 3 nodes)
        assert!(ds.len() <= 3);
    }

    #[test]
    fn test_isolated_nodes() {
        let graph = Graph::new();
        // Add isolated nodes without edges
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(0, 2, NO_PROPS, None).unwrap();
        graph.add_node(0, 3, NO_PROPS, None).unwrap();

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert_eq!(ds.len(), 3, "All isolated nodes must be in the dominating set");
    }

    #[test]
    fn test_mixed_graph() {
        let graph = Graph::new();
        // Mix of connected and isolated nodes
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_node(0, 10, NO_PROPS, None).unwrap(); // Isolated node

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        assert!(ds.contains(&VID(3)), "Isolated node must be in the dominating set");
        // Should have at least 2 nodes: one for the connected part, one for the isolated node
        assert!(ds.len() >= 2, "Should have at least 2 nodes in dominating set");
    }

    #[test]
    fn test_larger_graph() {
        let graph = Graph::new();
        // Create a more complex graph structure
        // Central hub connected to multiple smaller clusters
        for i in 1..=3 {
            graph.add_edge(0, 0, i, NO_PROPS, None).unwrap();
            for j in 1..=2 {
                let node_id = i * 10 + j;
                graph.add_edge(0, i, node_id, NO_PROPS, None).unwrap();
            }
        }

        let ds = lazy_greedy_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds), "Result should be a valid dominating set");
        println!("Larger graph dominating set size: {}", ds.len());
    }
}