use std::sync::Arc;

use raphtory::core::Direction;

use super::{expr::Expr, Context, DataBlock};

trait Operator {
    fn execute(&self, input: Vec<DataBlock>, ctx: &Context) -> Result<(), super::ExecError>;
}

pub enum PhysicalOperator {
    Expand(Expand),
}

struct Expand{
    dir: Direction,
    from_col: usize,
    filter: Expr,
}

struct EdgeScan {
    layer: String,
    columns: Arc<[usize]>
}

struct NodeScan{
    columns: Arc<[usize]> // name could be one column
}