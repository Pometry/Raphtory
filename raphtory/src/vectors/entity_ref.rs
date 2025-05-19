use raphtory_api::core::entities::GID;

use crate::db::api::storage::graph::edges::edge_storage_ops::EdgeStorageOps;
use crate::db::api::view::StaticGraphViewOps;
use crate::db::graph::{edge::EdgeView, node::NodeView};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum EntityRef {
    Node(usize), // TODO: store this as a u32
    Edge(usize),
}

impl EntityRef {
    // TODO: there are some places where I should be using this and Im not
    pub(super) fn as_node_view<G: StaticGraphViewOps>(&self, view: &G) -> Option<NodeView<G>> {
        let node = self.as_node(view)?;
        view.node(node)
    }

    // TODO: there are some places where I should be using this and Im not
    pub(super) fn as_edge_view<G: StaticGraphViewOps>(&self, view: &G) -> Option<EdgeView<G>> {
        let (src, dst) = self.as_edge(view)?;
        view.edge(src, dst)
    }

    pub(super) fn as_node<G: StaticGraphViewOps>(&self, view: &G) -> Option<GID> {
        if let EntityRef::Node(id) = self {
            Some(view.node_id(raphtory_api::core::entities::VID(*id)))
        } else {
            None
        }
    }

    pub(super) fn as_edge<G: StaticGraphViewOps>(&self, view: &G) -> Option<(GID, GID)> {
        if let EntityRef::Edge(id) = self {
            let edge = view.core_edge((*id).into());
            let src = view.node_id(edge.src());
            let dst = view.node_id(edge.dst());
            Some((src, dst))
        } else {
            None
        }
    }

    pub(super) fn as_u32(&self) -> u32 {
        match self {
            EntityRef::Node(id) => *id as u32,
            EntityRef::Edge(id) => *id as u32,
        }
    }
}
