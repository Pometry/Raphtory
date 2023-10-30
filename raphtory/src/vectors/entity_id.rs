use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) enum EntityId {
    Node { id: u64 },
    Edge { src: u64, dst: u64 },
}

impl<G: GraphViewOps> From<&VertexView<G>> for EntityId {
    fn from(value: &VertexView<G>) -> Self {
        EntityId::Node { id: value.id() }
    }
}

impl<G: GraphViewOps> From<VertexView<G>> for EntityId {
    fn from(value: VertexView<G>) -> Self {
        EntityId::Node { id: value.id() }
    }
}

impl<G: GraphViewOps> From<&EdgeView<G>> for EntityId {
    fn from(value: &EdgeView<G>) -> Self {
        EntityId::Edge {
            src: value.src().id(),
            dst: value.dst().id(),
        }
    }
}

impl<G: GraphViewOps> From<EdgeView<G>> for EntityId {
    fn from(value: EdgeView<G>) -> Self {
        EntityId::Edge {
            src: value.src().id(),
            dst: value.dst().id(),
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
