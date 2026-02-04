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
use super::bellman_ford::bellman_ford_single_source_shortest_paths_algorithm;
use super::to_prop;

pub fn johnson_all_pairs_shortest_paths<G: StaticGraphViewOps>(
    g: &G,
    weight: Option<&str>,
    direction: Direction,
) -> Result<NodeState<'static, HashMap<VID, (f64, Nodes<'static, G>)>, G>, GraphError> {
}