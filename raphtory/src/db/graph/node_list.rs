use crate::{
    core::{entities::VID, Direction},
    db::{api::view::DynamicGraph, graph::node::NodeView},
    prelude::GraphViewOps,
};
use std::marker::PhantomData;

pub struct Hop<'graph, G: GraphViewOps<'graph>> {
    base_graph: G,
    graph: DynamicGraph,
    direction: Direction,
    _marker: PhantomData<&'graph ()>,
}
