use crate::db::{
    api::state::ops::{GraphView, HistoryOp, NodeOp},
    graph::views::filter::model::ComposableFilter,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveNode;

impl fmt::Display for IsActiveNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_NODE")
    }
}

impl ComposableFilter for IsActiveNode {}

// ── expr layer: the predicate as a boolean expression over the eval view ──

use crate::db::graph::views::filter::model::{
    node_expr::{CreateOp, EntityExpr},
    node_filter::NodeFilter as NodeFilterMarker,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

impl EntityExpr for IsActiveNode {
    type Marker = NodeFilterMarker;

    fn entity(&self) -> NodeFilterMarker {
        NodeFilterMarker
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl CreateOp for IsActiveNode {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, crate::errors::GraphError> {
        Ok(Arc::new(
            HistoryOp::new(graph).map(|h| Some(Prop::Bool(!h.is_empty()))),
        ))
    }
}
