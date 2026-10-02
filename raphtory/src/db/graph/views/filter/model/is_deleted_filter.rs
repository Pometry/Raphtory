use crate::db::{api::state::ops::GraphView, graph::views::filter::model::ComposableFilter};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsDeletedEdge;

impl fmt::Display for IsDeletedEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_DELETED_EDGE")
    }
}

impl ComposableFilter for IsDeletedEdge {}

// ── expr layer: the predicate as a boolean expression over the eval view ──

use crate::db::graph::views::filter::model::{
    edge_expr::{ops::IsDeletedEdgePropOp, EdgeOp},
    edge_filter::EdgeFilter as EdgeFilterMarker,
    node_expr::{CreateOp, EntityExpr},
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

impl EntityExpr for IsDeletedEdge {
    type Marker = EdgeFilterMarker;

    fn entity(&self) -> EdgeFilterMarker {
        EdgeFilterMarker
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl CreateOp for IsDeletedEdge {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, crate::errors::GraphError> {
        Ok(Arc::new(IsDeletedEdgePropOp { graph }))
    }
}
