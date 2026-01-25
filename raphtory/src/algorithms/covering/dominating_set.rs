use crate::{db::api::{view::StaticGraphViewOps}, prelude::{GraphViewOps, NodeViewOps}};
use raphtory_api::core::{
    entities::{
        VID,
    },
    Direction,
};
use std::{
    collections::HashSet
};


pub fn compute_dominating_set<G: StaticGraphViewOps>(g: &G, direction: Direction) -> HashSet<VID> {
    let mut dominating_set = HashSet::new();
    let mut uncovered_set = HashSet::new();
    for node in g.nodes() {
        uncovered_set.insert(node.node);
    }
    while !uncovered_set.is_empty() {
        let mut max_vid = None;
        let mut max_score = -1; 
        for v in g.nodes() {
            let vid = v.node;
            if dominating_set.contains(&vid) {
                continue;
            }

            let neighbors = match direction { 
                 Direction::IN => v.in_neighbours(),
                 Direction::OUT => v.out_neighbours(), 
                 Direction::BOTH => v.neighbours(),
            };
            
            let mut score = 0;
            if uncovered_set.contains(&vid) {
                score += 1;
            }
            for neighbor in neighbors {
                if uncovered_set.contains(&neighbor.node) {
                    score += 1;
                }
            }
            
            if max_vid.is_none() || score > max_score {
                max_vid = Some(vid);
                max_score = score;
            }
        }
        
        let max_vid = max_vid.unwrap();
        let max_node = g.node(max_vid).unwrap();
        let max_node_neighbors = match direction { 
             Direction::IN => max_node.in_neighbours(),
             Direction::OUT => max_node.out_neighbours(), 
             Direction::BOTH => max_node.neighbours(),
        };
        
        uncovered_set.remove(&max_vid);
        for neighbor in max_node_neighbors {
            uncovered_set.remove(&neighbor.node);
        }
        dominating_set.insert(max_vid);
    }
    dominating_set
}

pub fn is_dominating_set<G: StaticGraphViewOps>(g: &G, ds: &HashSet<VID>, direction: Direction) -> bool {
    let mut covered = HashSet::new();   
    for &v in ds {
        covered.insert(v);
        let node = g.node(v).unwrap();
        match direction {
            Direction::IN => {
                for n in node.in_neighbours() { covered.insert(n.node); }
            },
            Direction::OUT => {
                for n in node.out_neighbours() { covered.insert(n.node); }
            },
            Direction::BOTH => {
                for n in node.neighbours() { covered.insert(n.node); }
            }
        }
    }
    g.count_nodes() == covered.len()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use raphtory_api::core::Direction;

    #[test]
    fn test_dominating_set_line_graph() {
        let g = Graph::new();
        // 1 -> 2 -> 3
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        
        let ds = compute_dominating_set(&g, Direction::OUT);
        assert!(is_dominating_set(&g, &ds, Direction::OUT));
    }

    #[test]
    fn test_dominating_set_star_graph() {
        let g = Graph::new();
        // 1 -> 2, 1 -> 3, 1 -> 4
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(0, 1, 4, NO_PROPS, None).unwrap();
        
        let ds = compute_dominating_set(&g, Direction::OUT);
        assert!(is_dominating_set(&g, &ds, Direction::OUT));
        let vid_1 = g.node(1).unwrap().node;
        assert!(ds.contains(&vid_1));
        assert_eq!(ds.len(), 1);
    }

    #[test]
    fn test_dominating_set_incoming() {
        let g = Graph::new();
        // 2 -> 1, 3 -> 1, 4 -> 1
        g.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
        g.add_edge(0, 3, 1, NO_PROPS, None).unwrap();
        g.add_edge(0, 4, 1, NO_PROPS, None).unwrap();
        
        let ds = compute_dominating_set(&g, Direction::IN);
        assert!(is_dominating_set(&g, &ds, Direction::IN));
        let vid_1 = g.node(1).unwrap().node;
        assert!(ds.contains(&vid_1));
        assert_eq!(ds.len(), 1);
    }

    #[test]
    fn test_dominating_set_disconnected() {
        let g = Graph::new();
        g.add_node(0, 1, NO_PROPS, None).unwrap();
        g.add_node(0, 2, NO_PROPS, None).unwrap();
        
        let ds = compute_dominating_set(&g, Direction::BOTH);
        assert!(is_dominating_set(&g, &ds, Direction::BOTH));
        assert_eq!(ds.len(), 2);
    }

    #[test]
    fn test_dominating_set_cycle_graph() {
        let g = Graph::new();
        // 1 -> 2 -> 3 -> 1
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        g.add_edge(0, 3, 1, NO_PROPS, None).unwrap();
        
        let ds = compute_dominating_set(&g, Direction::OUT);
        assert!(is_dominating_set(&g, &ds, Direction::OUT));
        assert!(ds.len() >= 2); 
    }

    #[test]
    fn test_dominating_set_complete_graph() {
         let g = Graph::new();
         // Complete graph K4
         for i in 1..=4 {
             for j in 1..=4 {
                 if i != j {
                     g.add_edge(0, i, j, NO_PROPS, None).unwrap();
                 }
             }
         }
         
         let ds = compute_dominating_set(&g, Direction::OUT);
         assert!(is_dominating_set(&g, &ds, Direction::OUT));
         // In a complete graph, any single node dominates all others.
         assert_eq!(ds.len(), 1);
    }
}