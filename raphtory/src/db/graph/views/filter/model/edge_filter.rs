use crate::{
    db::{
        api::view::internal::{GraphView, Static},
        graph::views::filter::model::{
            edge_expr::{ops::EdgeEndpointNodeOp, EdgeOp},
            expr::DynCreateHistory,
            node_expr::{CreateOp, EntityExpr},
            EntityMarker,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{Deserialize, Serialize};
use std::{fmt, fmt::Display, sync::Arc};

// User facing entry for building edge filters.
#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct EdgeFilter;

impl Static for EdgeFilter {}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    Src,
    Dst,
}

// Generic wrapper that pairs a node-side expression with a concrete endpoint,
// carrying the endpoint through the chain so the compiled node op can be
// applied to the edge's src or dst node.
#[derive(Debug, Clone)]
pub struct EdgeEndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
}

impl<T: Display> Display for EdgeEndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> EdgeEndpointWrapper<T> {
    #[inline]
    pub fn new(inner: T, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }

    #[inline]
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EdgeEndpointWrapper<U> {
        EdgeEndpointWrapper {
            inner: f(self.inner),
            endpoint: self.endpoint,
        }
    }
}

impl<T: EntityExpr> EntityExpr for EdgeEndpointWrapper<T> {
    fn entity(&self) -> EntityMarker {
        EntityMarker::Edge
    }
}

impl<T: CreateOp> CreateOp for EdgeEndpointWrapper<T> {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let node_op = self.inner.create_node_op(graph)?;
        Ok(Arc::new(EdgeEndpointNodeOp {
            node_op,
            endpoint: self.endpoint,
        }))
    }

    /// A history read on the node at this end of the edge.
    fn history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        let inner = self.inner.history()?;
        Some(Arc::new(EdgeEndpointWrapper::new(inner, self.endpoint)))
    }
}
