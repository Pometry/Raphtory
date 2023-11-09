use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
};
use serde::{Deserialize, Serialize, Serializer};
use std::{
    borrow::Borrow,
    fmt::{Display, Formatter},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) enum EntityId {
    Node { id: u64 },
    Edge { src: u64, dst: u64 },
}

impl EntityId {
    pub(crate) fn from_node_id(id: u64) -> Self {
        Self::Node { id }
    }

    pub(crate) fn from_edge_id(src: u64, dst: u64) -> Self {
        Self::Edge { src, dst }
    }

    pub(crate) fn from_node<G: GraphViewOps>(node: &VertexView<G>) -> Self {
        Self::Node { id: node.id() }
    }

    pub(crate) fn from_edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Self {
        Self::Edge {
            src: edge.src().id(),
            dst: edge.dst().id(),
        }
    }
}

// impl<G: GraphViewOps> From<&VertexView<G>> for EntityId {
//     fn from(value: &VertexView<G>) -> Self {
//         EntityId::Node { id: value.id() }
//     }
// }
//
// impl<G: GraphViewOps> From<VertexView<G>> for EntityId {
//     fn from(value: VertexView<G>) -> Self {
//         EntityId::Node { id: value.id() }
//     }
// }
//
// impl<G: GraphViewOps> From<&EdgeView<G>> for EntityId {
//     fn from(value: &EdgeView<G>) -> Self {
//         EntityId::Edge {
//             src: value.src().id(),
//             dst: value.dst().id(),
//         }
//     }
// }
//
// impl<G: GraphViewOps> From<EdgeView<G>> for EntityId {
//     fn from(value: EdgeView<G>) -> Self {
//         EntityId::Edge {
//             src: value.src().id(),
//             dst: value.dst().id(),
//         }
//     }
// }

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
