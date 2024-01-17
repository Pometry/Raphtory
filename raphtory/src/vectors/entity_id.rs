use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) enum EntityId {
    Graph { name: String },
    Node { id: u64 },
    Edge { src: u64, dst: u64 },
}

impl EntityId {
    pub(crate) fn from_graph<G: StaticGraphViewOps>(graph: &G) -> Self {
        Self::Graph {
            name: graph
                .properties()
                .get("name")
                .expect("A graph should have a 'name' property in order to make a document for it")
                .to_string(),
        }
    }

    pub(crate) fn from_node<G: StaticGraphViewOps>(node: &NodeView<G>) -> Self {
        Self::Node { id: node.id() }
    }

    pub(crate) fn from_edge<G: StaticGraphViewOps>(edge: &EdgeView<G>) -> Self {
        Self::Edge {
            src: edge.src().id(),
            dst: edge.dst().id(),
        }
    }

    pub(crate) fn is_graph(&self) -> bool {
        match self {
            EntityId::Graph { .. } => true,
            EntityId::Node { .. } => false,
            EntityId::Edge { .. } => false,
        }
    }

    pub(crate) fn is_node(&self) -> bool {
        match self {
            EntityId::Graph { .. } => false,
            EntityId::Node { .. } => true,
            EntityId::Edge { .. } => false,
        }
    }

    pub(crate) fn is_edge(&self) -> bool {
        match self {
            EntityId::Graph { .. } => false,
            EntityId::Node { .. } => false,
            EntityId::Edge { .. } => true,
        }
    }
}

impl Display for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityId::Graph { name } => f.write_str(&format!("graph:{name}")), // TODO: review, Im not sure what Im using this for smth
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
