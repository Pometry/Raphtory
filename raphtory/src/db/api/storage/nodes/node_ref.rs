use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        ArcStr, Direction,
    },
    db::api::storage::{arrow::nodes::ArrowNode, direction_variants::DirectionVariants},
};
use arrow2::Either;
use crate::db::api::view::internal::NodeAdditions;

#[derive(Copy, Clone, Debug)]
pub enum NodeStorageRef<'a> {
    Mem(&'a NodeStore),
    #[cfg(feature = "arrow")]
    Arrow(ArrowNode<'a>),
}

impl<'a> NodeStorageRef<'a> {
    pub fn degree(self, dir: Direction, layers: &LayerIds) -> usize {
        match self {
            NodeStorageRef::Mem(node) => node.degree(layers, dir),
            #[cfg(feature = "arrow")]
            NodeStorageRef::Arrow(node) => node.degree(layers, dir),
        }
    }
    
    pub fn timestamps(self) -> NodeAdditions {
        match self {
            NodeStorageRef::Mem(node) => {NodeAdditions::Mem(node.timestamps())}
            NodeStorageRef::Arrow(node) => {node.}
        }
    }

    pub fn edges_iter(
        self,
        dir: Direction,
        layers: &'a LayerIds,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        match self {
            NodeStorageRef::Mem(node) => Either::Left(node.edge_tuples(layers, dir)),
            #[cfg(feature = "arrow")]
            NodeStorageRef::Arrow(node) => Either::Right(match dir {
                Direction::OUT => DirectionVariants::Out(node.out_edges(layers)),
                Direction::IN => DirectionVariants::In(node.in_edges(layers)),
                Direction::BOTH => DirectionVariants::Both(node.edges(layers)),
            }),
        }
    }

    pub fn node_type_id(&self) -> usize {
        match self {
            NodeStorageRef::Mem(node) => node.node_type,
            NodeStorageRef::Arrow(_) => 0,
        }
    }

    pub fn vid(&self) -> VID {
        match self {
            NodeStorageRef::Mem(node) => node.vid,
            NodeStorageRef::Arrow(node) => node.vid(),
        }
    }
}
