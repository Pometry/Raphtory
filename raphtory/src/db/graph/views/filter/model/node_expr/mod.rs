pub mod ops;

pub use crate::db::graph::views::filter::model::expr::builder::EntityAggOps;
pub use ops::*;

use crate::{
    db::{
        api::{state::NodeOp, view::internal::DynGraphArc},
        graph::views::filter::model::expr::{
            typing::{require_bool, truthy},
            NodeExpr,
        },
    },
    errors::GraphError,
};
use std::sync::Arc;

impl NodeExpr {
    /// The per-node test this boolean tree is. A tree built only from
    /// constants answers the same for every node, so it is refused.
    pub(super) fn node_test<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'g>, GraphError> {
        self.refuse_constant()?;
        let op = self.value_op(graph)?;
        require_bool(&self.resolved_type(&op), "a filter")?;
        Ok(Arc::new(op.map(|v| truthy(&v))))
    }
}
