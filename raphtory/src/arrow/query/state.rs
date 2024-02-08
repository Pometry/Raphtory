use crate::{arrow::edge::Edge, arrow::nodes::Node, core::entities::VID};
pub trait HopState: Send + Sync + Clone + std::fmt::Debug {
    fn new(node: Node) -> Self;
    fn with_next(&self, node: Node, edge: Edge) -> Self;
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct NoState;

impl HopState for NoState {
    fn new(_node: Node) -> Self {
        NoState
    }

    fn with_next(&self, _node: Node, _edge: Edge) -> Self {
        NoState
    }
}
#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct VecState(pub Vec<VID>);

impl HopState for VecState {
    fn new(node: Node) -> Self {
        VecState(vec![node.vid()])
    }

    fn with_next(&self, node: Node, edge: Edge) -> Self {
        let VecState(mut vec) = self.clone();
        vec.push(node.vid());
        VecState(vec)
    }
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub struct Count(usize);

impl HopState for Count {
    fn new(_node: Node) -> Self {
        Count(1)
    }

    fn with_next(&self, _node: Node, _edge: Edge) -> Self {
        let Count(count) = self;
        Count(*count + 1)
    }
}

