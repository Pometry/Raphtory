//! Defines the `Edge` struct, which represents an edge in the graph.

use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{EdgeListOps, EdgeViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct EdgeView<G: GraphViewInternalOps> {
    graph: Arc<G>,
    edge: EdgeRef,
}

impl<G: GraphViewInternalOps> Debug for EdgeView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EdgeView({}, {})",
            self.edge.src_g_id, self.edge.dst_g_id
        )
    }
}

impl<G: GraphViewInternalOps> EdgeView<G> {
    pub(crate) fn new(graph: Arc<G>, edge: EdgeRef) -> Self {
        EdgeView { graph, edge }
    }

    pub fn as_ref(&self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewInternalOps> Into<EdgeRef> for EdgeView<G> {
    fn into(self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewInternalOps + 'static + Send + Sync> EdgeViewOps for EdgeView<G> {
    type Vertex = VertexView<G>;

    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError> {
        self.graph.temporal_edge_props_vec(self.edge, name)
    }

    fn src(&self) -> Self::Vertex {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };
        Self::Vertex::new(self.graph.clone(), vertex)
    }

    fn dst(&self) -> Self::Vertex {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.dst_g_id,
            pid: None,
        };
        Self::Vertex::new(self.graph.clone(), vertex)
    }

    fn id(&self) -> usize {
        self.edge.edge_id
    }
}

impl<G: GraphViewInternalOps + 'static + Send + Sync> EdgeListOps
    for Box<dyn Iterator<Item = EdgeView<G>> + Send>
{
    type Vertex = VertexView<G>;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + Send>;
    type Edge = EdgeView<G>;
    type IterType = Box<dyn Iterator<Item = Self::Edge> + Send>;

    fn src(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.dst()))
    }
}
