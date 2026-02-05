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
use std::{
    collections::{HashMap},
};
use super::super::pathing::{reweighting::get_johnson_reweighting_function, dijkstra::dijkstra_single_source_shortest_paths_algorithm, to_prop};

pub fn diameter_approximation<G: StaticGraphViewOps>(
    g: &G,
    weight: Option<&str>,
    direction: Direction,
    s: usize
) -> Result<f64, GraphError> {
    let n_nodes = g.count_nodes();
    let mut idx_to_vid = vec![VID::default(); n_nodes]; 
    for node in g.nodes().iter() {
        idx_to_vid[node.node.index()] = node.node;
    }
    let weight_fn = get_johnson_reweighting_function(g, weight, direction)?;
    let cost_val = to_prop(g, weight, 0.0)?;
    let max_val = to_prop(g, weight, f64::MAX)?;
    let max_vertex = g.nodes().par_iter().map(|node| {
        let mut max_distance = 0.0;
        let mut max_idx = node.node.index();
        let (distances, _) = dijkstra_single_source_shortest_paths_algorithm(g, node, direction, s, cost_val, max_val, weight_fn)?;  
    }).max();    
}