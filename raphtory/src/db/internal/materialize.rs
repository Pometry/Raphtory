use crate::core::tgraph::graph::tgraph::InnerTemporalGraph;
use crate::{
    db::graph::Graph,
    db::{
        graph::InternalGraph,
        view_api::internal::{InternalMaterialize, MaterializedGraph},
    },
};
use std::sync::Arc;

impl<const N: usize> InternalMaterialize for InnerTemporalGraph<N> {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::EventGraph(Graph::new_from_inner(Arc::new(graph)))
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
