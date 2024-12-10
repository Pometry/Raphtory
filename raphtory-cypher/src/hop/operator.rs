use datafusion::{
    common::DFSchemaRef,
    logical_expr::{Expr, LogicalPlan, TableScan, UserDefinedLogicalNodeCore},
};
use raphtory::{core::Direction, disk_graph::DiskGraphStorage};
use std::{cmp::Ordering, sync::Arc};

#[derive(Debug, PartialEq, Hash, Eq)]
pub struct HopPlan {
    graph: GraphHolder,
    input: Arc<LogicalPlan>,
    pub dir: Direction,
    pub left_col: String,
    pub out_schema: DFSchemaRef,
    pub right_schema: DFSchemaRef, // helps pick the columns from the edge list we're hopping onto
    pub right_layers: Vec<String>, // what layers are we hopping onto
    pub expressions: Vec<(Expr, Expr)>, // [left.col == right.col]
    pub right_proj: Option<Vec<usize>>,
}

impl PartialOrd for HopPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.input
                .partial_cmp(&other.input)?
                .then(self.dir.partial_cmp(&other.dir)?),
        )
    }
}

#[derive(Clone)]
struct GraphHolder {
    pub graph: DiskGraphStorage,
}

impl GraphHolder {
    pub fn new(graph: DiskGraphStorage) -> Self {
        GraphHolder { graph }
    }
}

impl AsRef<DiskGraphStorage> for GraphHolder {
    fn as_ref(&self) -> &DiskGraphStorage {
        &self.graph
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
        let num_nodes = self.as_ref().as_ref().num_nodes();
        write!(f, "Graph num_nodes: {num_nodes}")
    }
}

impl HopPlan {
    pub fn graph(&self) -> DiskGraphStorage {
        self.graph.as_ref().clone()
    }

    pub fn from_table_scans(
        graph: DiskGraphStorage,
        dir: Direction,
        schema: DFSchemaRef,
        left: &LogicalPlan,
        right: TableScan,
        left_col: String,
        on: Vec<(Expr, Expr)>,
    ) -> Self {
        Self {
            graph: GraphHolder::new(graph),
            input: Arc::new(left.clone()),
            dir,
            out_schema: schema.clone(),
            left_col: left_col.to_string(),
            right_schema: right.projected_schema,
            right_layers: vec![right.table_name.to_string()],
            expressions: on.iter().cloned().collect(),
            right_proj: right.projection,
        }
    }
}
impl UserDefinedLogicalNodeCore for HopPlan {
    fn name(&self) -> &str {
        "HopPlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.out_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Hop: dir={:?}, right_layers={:?}, right_projection={:?}, out_projection={:?}",
            self.dir,
            self.right_layers,
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

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1);
        assert_eq!(exprs.len(), 0); // (eg JOIN on edge1.src = edge2.dst for -[]->()-[]->)
                                    // let expr = exprs.first().unwrap();
                                    // let (left, right) = extract_eq_exprs(expr).unwrap();
        Ok(HopPlan {
            graph: self.graph.clone(),
            dir: self.dir,
            left_col: self.left_col.clone(),
            input: Arc::new(inputs[0].clone()),
            out_schema: self.out_schema.clone(),
            right_schema: self.right_schema.clone(),
            right_layers: self.right_layers.clone(),
            expressions: self.expressions.clone(),
            right_proj: self.right_proj.clone(),
        })
    }
}
