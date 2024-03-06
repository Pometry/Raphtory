use std::{iter, sync::Arc};

use raphtory::core::Direction;

use super::{expr::Expr, Context, DataBlock};

pub trait Operator {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock>;
}

#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    Expand(Expand),
}

#[derive(Debug, Clone)]
pub struct Expand{
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

impl Operator for Expand {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock> {
        iter::empty()
    }
}