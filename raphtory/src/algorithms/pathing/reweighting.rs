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
use super::super::pathing::{bellman_ford::bellman_ford_single_source_shortest_paths_algorithm, dijkstra::dijkstra_single_source_shortest_paths_algorithm, to_prop};

// pub fn nonegative_weight_function<G: StaticGraphViewOps>(
//     g: &G,
//     weight: Option<&str>,
//     direction: Direction,
// ) {
//     let dist_val = to_prop(g, weight, 0.0)?;
//     let weight_fn = |edge: &EdgeView<G>| -> Option<Prop> {
//         let edge_val = match weight{
//             None => Some(Prop::U8(1)),
//             Some(weight) => match edge.properties().get(weight) {
//                 Some(prop) => Some(prop),
//                 _ => None
//             }
//          };
//          edge_val
//     };
//     let source_node = if let Some(source_node) = g.nodes().iter().next() {
//         source_node
//     } else {
//         return;
//     };  
//     let distances = bellman_ford_single_source_shortest_paths_algorithm(g, None, None, direction, dist_val, dist_val, weight_fn)?;
// }