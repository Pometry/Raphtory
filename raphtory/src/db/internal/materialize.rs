use crate::{
    core::entities::graph::tgraph::InternalGraph,
    db::{
        api::view::internal::{InternalMaterialize, MaterializedGraph},
        graph::graph::Graph,
    },
};
use std::sync::Arc;

impl InternalMaterialize for InternalGraph {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::EventGraph(Graph::from_internal_graph(Arc::new(graph)))
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
