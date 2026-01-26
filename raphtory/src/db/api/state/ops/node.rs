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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NameStruct {
    name: String,
}

impl NodeOp for Name {
    type Output = String;
    type ArrowOutput = NameStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        NameStruct {
            name: self.apply(storage, node),
        }
    }
}

impl IntoDynNodeOp for Name {}

#[derive(Debug, Copy, Clone)]
pub struct Id;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IdStruct {
    id: GID,
}

impl NodeOp for Id {
    type Output = GID;
    type ArrowOutput = IdStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        IdStruct {
            id: self.apply(storage, node),
        }
    }
}

impl IntoDynNodeOp for Id {}

#[derive(Debug, Copy, Clone)]
pub struct Type;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TypeStruct {
    node_type: Option<ArcStr>,
}

impl NodeOp for Type {
    type Output = Option<ArcStr>;
    type ArrowOutput = TypeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        TypeStruct {
            node_type: self.apply(storage, node),
        }
    }
}

impl IntoDynNodeOp for Type {}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TypeIdStruct {
    type_id: usize,
}

impl NodeOp for TypeId {
    type Output = usize;
    type ArrowOutput = TypeIdStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        TypeIdStruct {
            type_id: self.apply(storage, node),
        }
    }
}

impl IntoDynNodeOp for TypeId {}

#[derive(Debug, Clone)]
pub struct Degree<G> {
    pub(crate) dir: Direction,
    pub(crate) view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DegreeStruct {
    degree: usize,
}

impl<G: GraphView> NodeOp for Degree<G> {
    type Output = usize;
    type ArrowOutput = DegreeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        let node = storage.core_node(node);
        if matches!(self.view.filter_state(), FilterState::Neither) {
            node.degree(self.view.layer_ids(), self.dir)
        } else {
            node.filtered_neighbours_iter(&self.view, self.view.layer_ids(), self.dir)
                .count()
        }
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        DegreeStruct {
            degree: self.apply(storage, node),
        }
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for Degree<G> {}
