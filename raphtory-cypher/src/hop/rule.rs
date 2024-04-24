use datafusion::{
    common::Column,
    error::DataFusionError,
    logical_expr::{Expr, Join, LogicalPlan},
    optimizer::{optimize_children, optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
};
use raphtory::{arrow::graph_impl::ArrowGraph, core::Direction};

pub struct HopRule {
    pub graph: ArrowGraph,
}

impl HopRule {
    pub fn new(graph: ArrowGraph) -> Self {
        Self { graph }
    }
}

impl OptimizerRule for HopRule {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Join(join) = plan {
            let Join {
                right, on, left, ..
            } = join;

            println!("right: {right:?}");
            println!("left: {left:?}");

            if on.len() != 1 {
                return optimize_children(self, plan, config);
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
                return optimize_children(self, plan, config);
            };

            // find the column we're hopping from on the left plan
            println!("HopRule::try_optimize on plan right: {right:?}");
        }
        optimize_children(self, plan, config)
    }

    fn name(&self) -> &str {
        "hop"
    }
}

#[cfg(test)]
mod test {
    use raphtory::arrow::graph_impl::ArrowGraph;
    use tempfile::tempdir;

    use crate::prepare_plan;

    #[tokio::test]
    async fn double_hop_edge_to_edge() {
        let graph_dir = tempdir().unwrap();
        let edges = vec![(0u64, 1u64, 0i64, 2.)];
        let g = ArrowGraph::make_simple_graph(graph_dir, &edges, 10, 10);
        let (_, plan) = prepare_plan("MATCH ()-[e1]->()-[e2]->() RETURN *", &g)
            .await
            .unwrap();

        println!("PLAN {plan:?}");
    }

    #[tokio::test]
    async fn double_hop_edge_to_edge_with_pushdown_filter() {
        let graph_dir = tempdir().unwrap();
        let edges = vec![(0u64, 1u64, 0i64, 2.)];
        let g = ArrowGraph::make_simple_graph(graph_dir, &edges, 10, 10);
        let (_, plan) = prepare_plan("MATCH ()-[e1]->()-[e2]->() WHERE e2.weight > 5 RETURN *", &g)
            .await
            .unwrap();

        println!("PLAN {plan:?}");
    }
}
