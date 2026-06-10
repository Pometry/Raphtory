use crate::db::api::state::ops::ArrowNodeOp;
pub(crate) use crate::db::api::{
    state::{generic_node_state::InputNodeStateValue, ops::IntoDynNodeOp, NodeOp},
    view::internal::{filtered_node::FilteredNodeStorageOps, FilterOps, FilterState, GraphView},
};
use raphtory_api::core::{
    entities::{GID, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct Name;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NameStruct {
    name: String,
}
impl From<String> for NameStruct {
    fn from(name: String) -> Self {
        NameStruct { name }
    }
}

impl NodeOp for Name {
    type Output = String;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }
}

impl ArrowNodeOp for Name {
    type ArrowOutput = NameStruct;
}

impl IntoDynNodeOp for Name {}

#[derive(Debug, Copy, Clone)]
pub struct Id;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IdStruct {
    id: GID,
}
impl From<GID> for IdStruct {
    fn from(id: GID) -> Self {
        IdStruct { id }
    }
}

impl NodeOp for Id {
    type Output = GID;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }
}

impl ArrowNodeOp for Id {
    type ArrowOutput = IdStruct;
}

impl IntoDynNodeOp for Id {}

#[derive(Debug, Copy, Clone)]
pub struct Type;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TypeStruct {
    node_type: Option<ArcStr>,
}
impl From<Option<ArcStr>> for TypeStruct {
    fn from(node_type: Option<ArcStr>) -> Self {
        TypeStruct { node_type }
    }
}

impl NodeOp for Type {
    type Output = Option<ArcStr>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }
}

impl ArrowNodeOp for Type {
    type ArrowOutput = TypeStruct;
}

impl IntoDynNodeOp for Type {}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TypeIdStruct {
    type_id: usize,
}
impl From<usize> for TypeIdStruct {
    fn from(type_id: usize) -> Self {
        TypeIdStruct { type_id }
    }
}

impl NodeOp for TypeId {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }
}

impl ArrowNodeOp for TypeId {
    type ArrowOutput = TypeIdStruct;
}

impl IntoDynNodeOp for TypeId {}

#[derive(Debug, Clone)]
pub struct Degree<G> {
    pub(crate) dir: Direction,
    pub(crate) view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DegreeStruct {
    degree: usize,
}
impl From<usize> for DegreeStruct {
    fn from(degree: usize) -> Self {
        DegreeStruct { degree }
    }
}

impl<G: GraphView> NodeOp for Degree<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        let node = storage.core_node(node);
        if matches!(self.view.filter_state(), FilterState::Neither) {
            node.degree(self.view.layer_ids(), self.dir)
        } else {
            node.filtered_neighbours_iter(&self.view, self.view.layer_ids(), self.dir)
                .count()
        }
    }
}

impl<G: GraphView> ArrowNodeOp for Degree<G> {
    type ArrowOutput = DegreeStruct;
}

impl<G: GraphView + 'static> IntoDynNodeOp for Degree<G> {}
