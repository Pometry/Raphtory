use raphtory_arrow::{edge::Edge, nodes::Node};
use std::option::Option;

use crate::{
    core::entities::VID,
    db::graph::{edge::EdgeView, node::NodeView},
    prelude::GraphViewOps,
};
pub trait HopState: Send + Sync + Clone + std::fmt::Debug {
    fn hop_with_state(&self, node: Node, edge: Edge) -> Option<Self>;
}

pub trait StaticGraphHopState: Send + Sync + Clone + std::fmt::Debug {
    fn start<'a, G: GraphViewOps<'a>>(&self, node: &NodeView<&'a G>) -> Self;
    fn hop_with_state<'a, G: GraphViewOps<'a>>(
        &self,
        node: &NodeView<&'a G>,
        edge: &EdgeView<&'a G>,
    ) -> Option<Self>;
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct NoState;

impl Default for NoState {
    fn default() -> Self {
        Self::new()
    }
}

impl NoState {
    pub fn new() -> Self {
        NoState
    }
}

impl HopState for NoState {
    fn hop_with_state(&self, _node: Node, _edge: Edge) -> Option<Self> {
        Some(NoState)
    }
}
#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct VecState(pub Vec<VID>);

impl VecState {
    pub fn new(node: Node) -> Self {
        VecState(vec![node.vid()])
    }
}

impl HopState for VecState {
    fn hop_with_state(&self, node: Node, _edge: Edge) -> Option<VecState> {
        let VecState(mut vec) = self.clone();
        vec.push(node.vid());
        Some(VecState(vec))
    }
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct Count(usize);

impl HopState for Count {
    fn hop_with_state(&self, _node: Node, _edge: Edge) -> Option<Self> {
        let Count(count) = self;
        Some(Count(*count + 1))
    }
}
