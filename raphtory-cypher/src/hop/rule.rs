use std::sync::Arc;

use super::execution::HopExec;
use crate::hop::operator::HopPlan;
use async_trait::async_trait;
use datafusion::{
    common::{tree_node::Transformed, Column},
    error::DataFusionError,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{Expr, Extension, Join, LogicalPlan, UserDefinedLogicalNode},
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use raphtory::{core::Direction, disk_graph::DiskGraphStorage};

pub struct HopRule {
    pub graph: DiskGraphStorage,
}

impl HopRule {
    pub fn new(graph: DiskGraphStorage) -> Self {
        Self { graph }
    }
}

impl OptimizerRule for HopRule {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Join(join) = &plan {
            let Join {
                right,
                on,
                left,
                schema,
                ..
            } = join;

            if on.len() != 1 {
                return Ok(Transformed::no(plan));
            }

            let (hop_from_col, _hop_to_col, direction) = if let (
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
                    _ => return Ok(Transformed::no(plan)),
                };
                (hop_from_col, hop_to_col, direction)
            } else {
                return Ok(Transformed::no(plan));
            };

            // simplest form Any -> TableScan
            if let (l_tbl, LogicalPlan::SubqueryAlias(r_tbl)) = (left.as_ref(), right.as_ref()) {
                if let LogicalPlan::TableScan(r_tbl) = r_tbl.input.as_ref() {
                    let plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(HopPlan::from_table_scans(
                            self.graph.clone(),
                            direction,
                            schema.clone(),
                            l_tbl,
                            r_tbl.clone(),
                            hop_from_col.clone(),
                            on.clone(),
                        )),
                    });
                    return Ok(Transformed::yes(plan));
                }
            }
        }
        Ok(Transformed::no(plan))
    }

    fn name(&self) -> &str {
        "hop"
    }
}

pub struct HopQueryPlanner;

#[async_trait]
impl QueryPlanner for HopQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(HopPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

struct HopPlanner;

#[async_trait]
impl ExtensionPlanner for HopPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(node) = node.as_any().downcast_ref::<HopPlan>() {
            let exec_plan = HopExec::new(node, physical_inputs);
            Ok(Some(Arc::new(exec_plan)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::util::pretty::print_batches;
    use raphtory::disk_graph::DiskGraphStorage;
    use tempfile::tempdir;

    use crate::prepare_plan;

    #[tokio::test]
    async fn double_hop_edge_to_edge() {
        let graph_dir = tempdir().unwrap();
        let edges = vec![(0u64, 1u64, 0i64, 2.)];
        let g = DiskGraphStorage::make_simple_graph(graph_dir, &edges, 10, 10);
        let (_, plan) = prepare_plan("MATCH ()-[e1]->()-[e2]->() RETURN *", &g, true)
            .await
            .unwrap();

        println!("PLAN {plan:?}");
    }

    #[tokio::test]
    async fn double_hop_edge_to_edge_with_pushdown_filter_e2() {
        let graph_dir = tempdir().unwrap();
        let edges = vec![(0u64, 1u64, 0i64, 2.)];
        let g = DiskGraphStorage::make_simple_graph(graph_dir, &edges, 10, 10);
        let (_, plan) = prepare_plan(
            "MATCH ()-[e1]->()-[e2]->() WHERE e2.weight > 5 RETURN *",
            &g,
            true,
        )
        .await
        .unwrap();

        println!("PLAN {plan:?}");
    }

    #[tokio::test]
    async fn double_hop_edge_to_edge_with_pushdown_filter_e1() {
        let graph_dir = tempdir().unwrap();
        let edges = vec![(0u64, 1u64, 0i64, 2.)];
        let g = DiskGraphStorage::make_simple_graph(graph_dir, &edges, 10, 10);
        let (_, plan) = prepare_plan(
            "MATCH ()-[e1]->()-[e2]->() WHERE e1.weight > 5 RETURN *",
            &g,
            true,
        )
        .await
        .unwrap();

        println!("PLAN {plan:?}");
    }

    #[tokio::test]
    async fn as_physical_plan_e1() {
        // +----+----------+-----+-----+----------+--------+----+----------+-----+-----+----------+--------+
        // | id | layer_id | src | dst | rap_time | weight | id | layer_id | src | dst | rap_time | weight |
        // +----+----------+-----+-----+----------+--------+----+----------+-----+-----+----------+--------+
        // | 0  | 0        | 0   | 1   | 0        | 2.0    | 1  | 0        | 1   | 2   | 1        | 3.0    |
        // | 1  | 0        | 1   | 2   | 1        | 3.0    | 2  | 0        | 2   | 3   | 2        | 4.0    |
        // +----+----------+-----+-----+----------+--------+----+----------+-----+-----+----------+--------+
        let graph_dir = tempdir().unwrap();
        let edges = vec![(0u64, 1u64, 0i64, 2.), (1, 2, 1, 3.), (2, 3, 2, 4.)];
        let g = DiskGraphStorage::make_simple_graph(graph_dir, &edges, 10, 10);

        // let (ctx, plan) = prepare_plan("MATCH ()-[e1]->() RETURN *", &g)
        let (ctx, plan) = prepare_plan("MATCH ()-[e1]->()-[e2]->() RETURN *", &g, true)
            .await
            .unwrap();

        println!("PLAN {plan:?}");
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let out = df.collect().await.unwrap();
        print_batches(&out).expect("print_batches");
    }
}
