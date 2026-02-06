use crate::db::api::state::ops::node;
use crate::db::graph::edge::EdgeView;
use crate::db::graph::node::NodeView;
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
use rayon::iter::ParallelIterator;
use std::collections::HashSet;
use std::hash::Hash;
use std::{
    collections::{HashMap},
};
use rayon::prelude::*;
use super::super::pathing::{reweighting::get_johnson_reweighting_function, dijkstra::dijkstra_single_source_shortest_paths_algorithm, to_prop};

pub fn diameter_approximation<G: StaticGraphViewOps>(
    g: &G,
    weight: Option<&str>,
    direction: Direction,
    s: usize
) -> Result<f64, GraphError> {
    let n_nodes = g.nodes().len();
    let weight_fn = get_johnson_reweighting_function(g, weight, direction)?;
    let cost_val = to_prop(g, weight, 0.0)?;
    let max_val = to_prop(g, weight, f64::MAX)?;
    let nodes_of_s_tree_with_max_depth = g.nodes().par_iter().map(|node| {
        k_ordered_paths_and_max_depth(g, direction, node.node, s, cost_val, max_val, weight_fn) 
    }).max_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).map(|(k_ordered, _)| k_ordered).unwrap();    
    let max_depth = nodes_of_s_tree_with_max_depth.par_iter().map(|node_vid| {
        k_ordered_paths_and_max_depth(g, direction, *node_vid, n_nodes - 1, cost_val, max_val, weight_fn) 
    }).max_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).map(|(_, dist)| dist).unwrap();    


}

fn k_ordered_paths_and_max_depth<G: StaticGraphViewOps>(g: &G, direction: Direction, node_vid: VID, s: usize, cost_val: Prop, max_val: Prop, weight_fn: impl Fn(&EdgeView<G>) -> Option<Prop>) -> (Vec<VID>, f64) {
    let node = g.node(&node_vid).unwrap();
    let (distances, _, k_ordered) = dijkstra_single_source_shortest_paths_algorithm::<G, _, _, HashMap<VID, Prop>, HashMap<VID, VID>, HashSet<VID>>(g, node, direction, s, cost_val, max_val, weight_fn).unwrap();  
    let max_vid = k_ordered[k_ordered.len() - 1];
    let max_distance_val = distances.get(&max_vid).unwrap();
    (k_ordered, max_distance_val.as_f64().unwrap())
}

