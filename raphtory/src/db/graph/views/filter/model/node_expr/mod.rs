pub mod ops;

pub use crate::db::graph::views::filter::model::expr::builder::EntityAggOps;
pub use ops::*;

use crate::{
    db::{
        api::{state::NodeOp, view::internal::DynGraphArc},
        graph::views::filter::model::expr::{typing::truthy, NodeExpr},
    },
    errors::GraphError,
};
use std::sync::Arc;

impl NodeExpr {
    /// The per-node test this boolean tree is; a tree built only from
    /// constants is refused.
    pub(super) fn node_test<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'g>, GraphError> {
        Ok(Arc::new(
            self.filter_value(graph)?.map(|value| truthy(&value)),
        ))
    }
}
