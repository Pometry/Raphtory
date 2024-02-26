use crate::{
    arrow::{edge::Edge, nodes::Node},
    core::entities::VID,
};
pub trait HopState: Send + Sync + Clone + std::fmt::Debug {
    fn with_next(&self, node: Node, edge: Edge) -> Self;

    fn with_next_2(&self, node: Node, edge: Edge) -> Option<Self> {
        Some(self.clone())
    }
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct NoState;

impl NoState {
    pub fn new() -> Self {
        NoState
    }
}

impl HopState for NoState {
    fn with_next(&self, _node: Node, _edge: Edge) -> Self {
        NoState
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

    fn with_next(&self, node: Node, edge: Edge) -> Self {
        let VecState(mut vec) = self.clone();
        vec.push(node.vid());
        VecState(vec)
    }
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct Count(usize);

impl HopState for Count {

    fn with_next(&self, _node: Node, _edge: Edge) -> Self {
        let Count(count) = self;
        Count(*count + 1)
    }
}
