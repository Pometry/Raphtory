use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, VertexViewOps},
};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) enum EntityId {
    Node { id: u64 },
    Edge { src: u64, dst: u64 },
}

impl EntityId {

    pub(crate) fn from_node<G: StaticGraphViewOps>(node: &VertexView<G>) -> Self {
        Self::Node { id: node.id() }
    }

    pub(crate) fn from_edge<G: StaticGraphViewOps>(edge: &EdgeView<G>) -> Self {
        Self::Edge {
            src: edge.src().id(),
            dst: edge.dst().id(),
        }
    }

    pub(crate) fn is_node(&self) -> bool {
        match self {
            EntityId::Node { .. } => true,
            EntityId::Edge { .. } => false,
        }
    }

    pub(crate) fn is_edge(&self) -> bool {
        match self {
            EntityId::Node { .. } => false,
            EntityId::Edge { .. } => true,
        }
    }
}

impl Display for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityId::Node { id } => f.serialize_u64(*id),
            EntityId::Edge { src, dst } => {
                f.serialize_u64(*src)
                    .expect("src ID couldn't be serialized");
                f.write_str("-")
                    .expect("edge ID separator couldn't be serialized");
                f.serialize_u64(*dst)
            }
        }
    }
}
