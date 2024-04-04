use std::sync::Arc;

use arrow2::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan;
use raphtory::core::Direction;

pub struct HopPlanNode {
    input: Arc<LogicalPlan>,
    dir: Direction,

    schema: SchemaRef,
}
