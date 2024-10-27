use crate::{
    db::graph::{edge::EdgeView, node::NodeView},
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
};
use raphtory_api::core::entities::GID;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) enum EntityId {
    Graph { name: Option<String> },
    Node { id: GID },
    Edge { src: GID, dst: GID },
}

impl EntityId {
    pub(crate) fn for_graph(name: Option<String>) -> Self {
        Self::Graph { name }
    }

    pub(crate) fn from_node<'graph, G: GraphViewOps<'graph>>(node: NodeView<G>) -> Self {
        Self::Node { id: node.id() }
    }

    pub(crate) fn from_edge<'graph, G: GraphViewOps<'graph>>(edge: EdgeView<G>) -> Self {
        Self::Edge {
            src: edge.src().id(),
            dst: edge.dst().id(),
        }
    }

    #[allow(dead_code)]
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
            EntityId::Graph { name } => {
                let graph_name = name.clone().unwrap_or("_unnamed".to_owned());
                f.write_str(&format!("graph:{graph_name}"))
            }
            EntityId::Node { id } => f.write_str(&id.to_str()),
            EntityId::Edge { src, dst } => {
                f.write_str(&src.to_str())
                    .expect("src ID couldn't be serialized");
                f.write_str("-")
                    .expect("edge ID separator couldn't be serialized");
                f.write_str(&dst.to_str())
            }
        }
    }
}
