use crate::core::tgraph::graph::tgraph::InnerTemporalGraph;
use crate::db::api::view::internal::{InternalMaterialize, MaterializedGraph};
use crate::{db::graph::graph::Graph, db::graph::graph::InternalGraph};
use std::sync::Arc;

impl<const N: usize> InternalMaterialize for InnerTemporalGraph<N> {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::EventGraph(Graph::new_from_inner(Arc::new(graph)))
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
