use std::sync::Arc;
use crate::{
    core::tgraph::tgraph::InnerTemporalGraph, db::{view_api::internal::{InternalMaterialize, MaterializedGraph}, graph::InternalGraph},
    db::graph::Graph,
};

impl<const N: usize> InternalMaterialize for InnerTemporalGraph<N> {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::EventGraph(Graph::new_from_inner(Arc::new(graph)))
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
