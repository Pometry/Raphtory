use datafusion::{
    common::Column,
    error::DataFusionError,
    logical_expr::{Expr, Join, LogicalPlan},
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
};
use raphtory::{arrow::graph_impl::ArrowGraph, core::Direction};

pub struct HopRule {
    pub graph: ArrowGraph,
}

impl OptimizerRule for HopRule {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Join(join) = plan {
            let Join {
                
                right,
                on,
                
                ..
            } = join;

            if on.len() != 1 {
                return Ok(None);
            }

            // (Direction::OUT, Direction::OUT) => ("dst", "src"),
            // (Direction::OUT, Direction::IN) => ("dst", "dst"),
            // (Direction::IN, Direction::OUT) => ("src", "src"),
            // (Direction::IN, Direction::IN) => ("src", "dst"),

            let (_hop_from_col, _hop_to_col, _direction) = if let (
                Expr::Column(Column {
                    name: hop_from_col, ..
                }),
                Expr::Column(Column {
                    name: hop_to_col, ..
                }),
            ) = &on[0]
            {
                let direction = match (hop_from_col.as_ref(), hop_to_col.as_ref()) {
                    ("dst", "src") => Direction::OUT,
                    ("dst", "dst") => Direction::IN,
                    ("src", "src") => Direction::OUT,
                    ("src", "dst") => Direction::IN,
                    cols => {
                        return Err(DataFusionError::Plan(format!(
                            "Invalid hop columns: {:?}",
                            cols
                        )));
                    }
                };
                (hop_from_col, hop_to_col, direction)
            } else {
                return Ok(None);
            };

            // find the column we're hopping from on the left plan
            println!("HopRule::try_optimize on plan right: {right:?}");
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        "hop"
    }
}
