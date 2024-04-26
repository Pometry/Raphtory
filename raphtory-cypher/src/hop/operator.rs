use std::sync::Arc;

use datafusion::logical_expr::expr::Alias;
use datafusion::{
    common::DFSchemaRef,
    logical_expr::{
        BinaryExpr, Expr, LogicalPlan, Operator, TableScan, UserDefinedLogicalNodeCore,
    },
};
use raphtory::{
    arrow::{graph::TemporalGraph, graph_impl::ArrowGraph},
    core::Direction,
};

#[derive(Debug, PartialEq, Hash, Eq)]
pub struct HopPlan {
    graph: GraphHolder,
    input: Arc<LogicalPlan>,
    dir: Direction,
    out_schema: DFSchemaRef,
    right_schema: DFSchemaRef, // helps pick the columns from the edge list we're hopping onto
    expressions: Vec<(Expr, Expr)>, // [left.col == right.col]
}

#[derive(Clone)]
struct GraphHolder {
    pub graph: ArrowGraph,
}

impl GraphHolder {
    pub fn new(graph: ArrowGraph) -> Self {
        GraphHolder { graph }
    }
}

impl AsRef<TemporalGraph> for GraphHolder {
    fn as_ref(&self) -> &TemporalGraph {
        self.graph.as_ref()
    }
}

impl PartialEq for GraphHolder {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl std::hash::Hash for GraphHolder {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        0.hash(state);
    }
}

impl Eq for GraphHolder {}

impl std::fmt::Debug for GraphHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let num_nodes = self.as_ref().num_nodes();
        write!(f, "Graph num_nodes: {num_nodes}")
    }
}

impl HopPlan {
    pub fn from_table_scans(
        graph: ArrowGraph,
        dir: Direction,
        schema: DFSchemaRef,
        left: TableScan,
        right: TableScan,
        on: &[(Expr, Expr)],
    ) -> Self {
        Self {
            graph: GraphHolder::new(graph),
            input: Arc::new(LogicalPlan::TableScan(left)),
            dir,
            out_schema: schema.clone(),
            right_schema: right.projected_schema,
            expressions: on.iter().cloned().collect(),
        }
    }
}

impl UserDefinedLogicalNodeCore for HopPlan {
    fn name(&self) -> &str {
        "Hop"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.out_schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        self.expressions
            .iter()
            .map(|(l, r)| Expr::eq(l.clone(), r.clone()))
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Hop: dir={:?}, right_projection={:?}, out_projection={:?}",
            self.dir,
            self.right_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
            self.out_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        )
    }

    fn from_template(&self, exprs: &[datafusion::prelude::Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1);
        assert_eq!(exprs.len(), 1); // (eg JOIN on edge1.src = edge2.dst for -[]->()-[]->)
        let expr = exprs.first().unwrap();
        let (left, right) = extract_eq_exprs(expr).unwrap();
        HopPlan {
            graph: self.graph.clone(),
            dir: self.dir,
            input: Arc::new(inputs[0].clone()),
            out_schema: self.out_schema.clone(),
            right_schema: self.right_schema.clone(),
            expressions: vec![(left, right)],
        }
    }
}

fn extract_eq_exprs(expr: &Expr) -> Option<(Expr, Expr)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Eq,
            left,
            right,
        }) => Some((left.as_ref().clone(), right.as_ref().clone())),
        Expr::Alias(Alias { expr, .. }) => extract_eq_exprs(expr.as_ref()),
        _ => None,
    }
}
