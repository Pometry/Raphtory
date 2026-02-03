use crate::db::api::view::StaticGraphViewOps;
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


