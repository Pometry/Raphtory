use crate::db::api::state::ops::node;
use crate::db::graph::assertions::FilterNeighbours;
/// Dijkstra's algorithm
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::StaticGraphViewOps};
use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::state::{ops::filter::NO_FILTER, Index, NodeState},
        graph::nodes::Nodes,
    },
    errors::GraphError,
    prelude::*,
};
use indexmap::IndexSet;
use raphtory_api::core::{
    entities::{
        properties::prop::{PropType, PropUnwrap},
        VID,
    },
    Direction,
};
use std::hash::Hash;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};
use rayon::prelude::*;

#[derive(Default, Clone)]
struct NodeCoveringState {
    vid: VID, 
    is_covered: bool,
    is_active: bool,
    is_candidate: bool,
    has_no_coverage: bool,
    support: usize,
    candidates: usize, 
    weight_rounded: usize,
    weight: usize,
    add_to_dominating_set: bool
}


pub fn fast_distributed_dominating_set<G: StaticGraphViewOps>(g: &G) -> HashSet<VID> {
    let mut dominating_set = HashSet::new();
    let mut covered_count = 0; 
    let n_nodes = g.count_nodes();
    let mut adj_list: Vec<Vec<usize>> = vec![vec![]; n_nodes];
    let mut current_node_configs = vec![NodeCoveringState::default(); n_nodes]; 
    let mut next_node_configs = vec![NodeCoveringState::default(); n_nodes]; 
    for node in g.nodes() {
        let vid = node.node;
        current_node_configs[vid.index()].vid = vid;
        next_node_configs[vid.index()].vid = vid;
        adj_list[vid.index()] = node.neighbours().iter().map(|n| n.node.index()).collect();    
    }
    while covered_count < n_nodes {
        next_node_configs.par_iter_mut().for_each(|next_node_config| {
            let node_index = next_node_config.vid.index();
            let current_node_config = &current_node_configs[node_index];
            next_node_config.is_covered = current_node_config.is_covered;
            if current_node_config.has_no_coverage {
                return;
            }
            let mut node_weight = 0 as u64;     
            if !current_node_config.is_covered {
                node_weight += 1;
            } 
            for neighbor_index in &adj_list[node_index] {
                let neighbor_config = &current_node_configs[*neighbor_index];
                if !neighbor_config.is_covered {
                    node_weight += 1;
                }
            }
            if node_weight == 0 {
                next_node_config.has_no_coverage = true; 
                next_node_config.weight = 0;
                next_node_config.weight_rounded = 0;
            } else {
                let node_weight_rounded = (2 as u64).pow(node_weight.ilog2()) as usize; 
                next_node_config.weight = node_weight as usize;
                next_node_config.weight_rounded = node_weight_rounded;
            }
        });
        std::mem::swap(&mut current_node_configs, &mut next_node_configs);
        next_node_configs.par_iter_mut().for_each(|next_node_config| {
            let node_index = next_node_config.vid.index();
            let current_node_config = &current_node_configs[node_index];
            next_node_config.has_no_coverage = current_node_config.has_no_coverage;
            next_node_config.weight = current_node_config.weight; 
            next_node_config.weight_rounded = current_node_config.weight_rounded;
            if current_node_config.has_no_coverage {
                next_node_config.is_active = false;
                return;
            }
            let mut max_weight_rounded = current_node_config.weight_rounded; 
            for neighbor_index in &adj_list[node_index] {
                let neighbor_config = &current_node_configs[*neighbor_index];
                if neighbor_config.weight_rounded > max_weight_rounded {
                    max_weight_rounded = neighbor_config.weight_rounded;
                }
                for second_neighbor_index in &adj_list[*neighbor_index] {
                    let second_neighbor_config = &current_node_configs[*second_neighbor_index];
                    if second_neighbor_config.weight_rounded > max_weight_rounded {
                        max_weight_rounded = second_neighbor_config.weight_rounded;
                    }
                }
            }
            if current_node_config.weight_rounded == max_weight_rounded {
                next_node_config.is_active = true;
            } else {
                next_node_config.is_active = false;
            }
        });               
        std::mem::swap(&mut current_node_configs, &mut next_node_configs);
        next_node_configs.par_iter_mut().for_each(|next_node_config| {
            let node_index = next_node_config.vid.index();
            let current_node_config = &current_node_configs[node_index];
            next_node_config.is_active = current_node_config.is_active;
            if current_node_config.has_no_coverage {
                next_node_config.support = 0;
                return;
            }
            let mut support = 0;
            if current_node_config.is_active {
                support += 1;
            }
            for neighbor_index in &adj_list[node_index] {
                let neighbor_config = &current_node_configs[*neighbor_index];
                if neighbor_config.is_active {
                    support += 1;
                }
            }
            next_node_config.support = support;
        });
        std::mem::swap(&mut current_node_configs, &mut next_node_configs);
        next_node_configs.par_iter_mut().for_each(|next_node_config| {
            let node_index = next_node_config.vid.index();
            let current_node_config = &current_node_configs[node_index];
            next_node_config.support = current_node_config.support;
            if !current_node_config.is_active{
                next_node_config.is_candidate = false;
                return;
            }
            let mut max_support = 0;
            if !current_node_config.is_covered {
                max_support = current_node_config.support;
            }  
            for neighbor_index in &adj_list[node_index] {
                let neighbor_config = &current_node_configs[*neighbor_index];
                if !neighbor_config.is_covered && neighbor_config.support > max_support {
                    max_support = neighbor_config.support;
                }
            }
            let p = 1.0/(max_support as f64);
            let r: f64 = rand::random();
            if r < p {
                next_node_config.is_candidate = true;
            } else {
                next_node_config.is_candidate = false;
            }
        });
        std::mem::swap(&mut current_node_configs, &mut next_node_configs);
        next_node_configs.par_iter_mut().for_each(|next_node_config| {
            let node_index = next_node_config.vid.index();
            let current_node_config = &current_node_configs[node_index];
            next_node_config.is_candidate = current_node_config.is_candidate;
            if current_node_config.has_no_coverage {
                next_node_config.candidates = 0;
                return;
            }
            let mut candidates = 0;
            if current_node_config.is_candidate {
                candidates += 1;
            }
            for neighbor_index in &adj_list[node_index] {
                let neighbor_config = &current_node_configs[*neighbor_index];
                if neighbor_config.is_candidate {
                    candidates += 1;
                }
            }
            next_node_config.candidates = candidates;
        });
        std::mem::swap(&mut current_node_configs, &mut next_node_configs);
        next_node_configs.par_iter_mut().for_each(|next_node_config| {
            let node_index = next_node_config.vid.index();
            let current_node_config = &current_node_configs[node_index];
            next_node_config.candidates = current_node_config.candidates;
            if !current_node_config.is_candidate {
                return;
            }
            let mut sum_candidates = 0;
            if !current_node_config.is_covered {
                sum_candidates += current_node_config.candidates;
            }
            for neighbor_index in &adj_list[node_index] {
                let neighbor_config = &current_node_configs[*neighbor_index];
                if !neighbor_config.is_covered {
                    sum_candidates += neighbor_config.candidates;
                }
            }
            if sum_candidates <= 3 * current_node_config.weight_rounded {
                next_node_config.add_to_dominating_set = true;
            }
        });
        std::mem::swap(&mut current_node_configs, &mut next_node_configs);
        for i in 0..n_nodes {
            let add_to_dominating_set = current_node_configs[i].add_to_dominating_set;
            if add_to_dominating_set {
                {
                    let node_config = &mut current_node_configs[i];
                    dominating_set.insert(node_config.vid);
                    node_config.add_to_dominating_set = false;
                    if !node_config.is_covered {
                        node_config.is_covered = true;
                        covered_count += 1;
                    }
                }
                for neighbor_index in &adj_list[i] {
                    let neighbor_config = &mut current_node_configs[*neighbor_index];
                    if !neighbor_config.is_covered {
                        neighbor_config.is_covered = true;
                        covered_count += 1;
                    }
                }
            }
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
    fn test_single_node_graph() {
        let graph = Graph::new();
        graph.add_node(0, 1, NO_PROPS, None).unwrap();

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert_eq!(ds.len(), 1, "Single node should dominate itself");
    }

    #[test]
    fn test_two_connected_nodes() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert_eq!(ds.len(), 1, "One node should dominate an edge");
    }

    #[test]
    fn test_star_graph() {
        let graph = Graph::new();
        // Star with center 0 and leaves 1-5
        for i in 1..=5 {
            graph.add_edge(0, 0, i, NO_PROPS, None).unwrap();
        }

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert_eq!(ds.len(), 1, "Star graph center should be the dominating set");
    }

    #[test]
    fn test_path_graph() {
        let graph = Graph::new();
        // Path: 1-2-3-4-5
        for i in 1..5 {
            graph.add_edge(0, i, i + 1, NO_PROPS, None).unwrap();
        }

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        println!("Path graph dominating set: {:?}", ds);
    }

    #[test]
    fn test_triangle_graph() {
        let graph = Graph::new();
        // Triangle: 1-2-3-1
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 3, 1, NO_PROPS, None).unwrap();

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert!(ds.len() <= 2, "At most 2 nodes needed to dominate a triangle");
    }

    #[test]
    fn test_complete_graph_k4() {
        let graph = Graph::new();
        // Complete graph K4
        for i in 1..=4 {
            for j in (i + 1)..=4 {
                graph.add_edge(0, i, j, NO_PROPS, None).unwrap();
            }
        }

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert!(!ds.is_empty());
    }

    #[test]
    fn test_cycle_graph() {
        let graph = Graph::new();
        // Cycle: 1-2-3-4-5-1
        for i in 1..=5 {
            graph.add_edge(0, i, if i == 5 { 1 } else { i + 1 }, NO_PROPS, None).unwrap();
        }

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        println!("Cycle graph dominating set: {:?}", ds);
    }

    #[test]
    fn test_disconnected_components() {
        let graph = Graph::new();
        // Component 1: 1-2-3
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        // Component 2: 4-5
        graph.add_edge(0, 4, 5, NO_PROPS, None).unwrap();

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert!(ds.len() >= 2, "Should have at least one node per component");
    }

    #[test]
    fn test_grid_graph() {
        let graph = Graph::new();
        // 3x3 grid
        // 1-2-3
        // | | |
        // 4-5-6
        // | | |
        // 7-8-9
        let edges = vec![
            (1, 2), (2, 3), (1, 4), (2, 5), (3, 6),
            (4, 5), (5, 6), (4, 7), (5, 8), (6, 9),
            (7, 8), (8, 9),
        ];
        
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        println!("Grid dominating set size: {}", ds.len());
    }

    #[test]
    fn test_with_isolated_nodes() {
        let graph = Graph::new();
        // Connected: 1-2-3
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        // Isolated nodes
        graph.add_node(0, 4, NO_PROPS, None).unwrap();
        graph.add_node(0, 5, NO_PROPS, None).unwrap();

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        assert!(ds.len() >= 3, "Each isolated node must be in the dominating set");
    }

    #[test]
    fn test_larger_random_structure() {
        let graph = Graph::new();
        // Create a more complex structure
        let edges = vec![
            (1, 2), (1, 3), (1, 4),  // Node 1 hub
            (2, 5), (3, 6), (4, 7),  // Branches
            (5, 8), (6, 8), (7, 8),  // Converge to 8
            (8, 9), (8, 10),         // From 8
            (9, 10),                 // Triangle
        ];
        
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        let ds = fast_distributed_dominating_set(&graph);
        
        assert!(is_dominating_set(&graph, &ds));
        println!("Complex structure dominating set size: {}", ds.len());
    }
}

