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
            if actual_uncovered_count > 0 {
                queue.insert(index, actual_uncovered_count);
            }
        }
    }
    dominating_set
}

pub fn is_dominating_set<G: StaticGraphViewOps>(g: &G, dominating_set: &HashSet<VID>) -> bool {
    let n_nodes = g.count_nodes();
    let mut covered_nodes: Vec<bool> = vec![false; n_nodes];
    let mut covered_count = 0;
    for &vid in dominating_set {
        if !covered_nodes[vid.index()] {
            covered_nodes[vid.index()] = true;
            covered_count += 1;
        }
        let node = g.node(vid).unwrap();
        for neighbor in node.neighbours() {
            if !covered_nodes[neighbor.node.index()] {
                covered_nodes[neighbor.node.index()] = true;
                covered_count += 1;
            }
        }
    }
    covered_count == n_nodes
}

