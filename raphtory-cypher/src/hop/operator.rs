use std::sync::Arc;

use datafusion::{
    common::DFSchemaRef,
    logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, UserDefinedLogicalNodeCore},
};
use raphtory::core::Direction;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HopPlan {
    input: Arc<LogicalPlan>,
    dir: Direction,
    schema: DFSchemaRef,
    expressions: Vec<(Expr, Expr)>,
}

impl UserDefinedLogicalNodeCore for HopPlan {
    fn name(&self) -> &str {
        "Hop"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        self.expressions
            .iter()
            .map(|(l, r)| Expr::eq(l.clone(), r.clone()))
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Hop: dir={:?}", self.dir)
    }

    fn from_template(&self, exprs: &[datafusion::prelude::Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1);
        assert_eq!(exprs.len(), 1); // (eg JOIN on edge1.src = edge2.dst for -[]->()-[]->)
        match exprs.first().unwrap() {
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::Eq,
                left,
                right,
            }) => HopPlan {
                dir: self.dir,
                input: Arc::new(inputs[0].clone()),
                schema: self.schema.clone(),
                expressions: vec![(left.as_ref().clone(), right.as_ref().clone())],
            },
            _ => panic!("Only Equality BinaryExpr allowed"),
        }
    }
}
