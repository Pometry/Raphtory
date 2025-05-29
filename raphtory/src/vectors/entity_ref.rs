use either::Either;
use raphtory_api::core::entities::GID;

use crate::db::{
    api::{storage::graph::edges::edge_storage_ops::EdgeStorageOps, view::StaticGraphViewOps},
    graph::{edge::EdgeView, node::NodeView},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum EntityRef {
    Node(u32),
    Edge(u32),
}

impl<G: StaticGraphViewOps> From<NodeView<G>> for EntityRef {
    fn from(value: NodeView<G>) -> Self {
        EntityRef::Node(value.into_db_id())
    }
}

impl<G: StaticGraphViewOps> From<EdgeView<G>> for EntityRef {
    fn from(value: EdgeView<G>) -> Self {
        EntityRef::Edge(value.into_db_id())
    }
}

impl EntityRef {
    pub(super) fn id(&self) -> u32 {
        match self {
            EntityRef::Node(id) => *id,
            EntityRef::Edge(id) => *id,
        }
    }

    pub(super) fn resolve_entity<G: StaticGraphViewOps>(
        &self,
        graph: &G,
    ) -> Option<Either<NodeView<G>, EdgeView<G>>> {
        match self.resolve_entity_gids(graph) {
            Either::Left(node) => Some(Either::Left(graph.node(node)?)),
            Either::Right((src, dst)) => Some(Either::Right(graph.edge(src, dst)?)),
        }
    }

    pub(super) fn as_node_view<G: StaticGraphViewOps>(&self, graph: &G) -> Option<NodeView<G>> {
        self.resolve_entity(graph)?.left()
    }

    pub(super) fn as_edge_view<G: StaticGraphViewOps>(&self, graph: &G) -> Option<EdgeView<G>> {
        self.resolve_entity(graph)?.right()
    }

    pub(super) fn as_node_gid<G: StaticGraphViewOps>(&self, graph: &G) -> Option<GID> {
        self.resolve_entity_gids(graph).left()
    }

    pub(super) fn as_edge_gids<G: StaticGraphViewOps>(&self, graph: &G) -> Option<(GID, GID)> {
        self.resolve_entity_gids(graph).right()
    }

    fn resolve_entity_gids<G: StaticGraphViewOps>(&self, graph: &G) -> Either<GID, (GID, GID)> {
        match self {
            EntityRef::Node(id) => {
                Either::Left(graph.node_id(raphtory_api::core::entities::VID((*id) as usize)))
            }
            EntityRef::Edge(id) => {
                let edge = graph.core_edge((*id as usize).into());
                let src = graph.node_id(edge.src());
                let dst = graph.node_id(edge.dst());
                Either::Right((src, dst))
            }
        }
    }
}

pub(super) trait IntoDbId {
    fn into_db_id(self) -> u32;
}

impl<G: StaticGraphViewOps> IntoDbId for NodeView<G> {
    fn into_db_id(self) -> u32 {
        self.node.index() as u32
    }
}

impl<G: StaticGraphViewOps> IntoDbId for EdgeView<G> {
    fn into_db_id(self) -> u32 {
        self.edge.pid().0 as u32
    }
}
