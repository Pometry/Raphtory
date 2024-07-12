use crate::{
    core::entities::graph::tgraph::InternalGraph,
    db::{
        api::view::internal::{InternalMaterialize, MaterializedGraph},
        graph::graph::Graph,
    },
};

impl InternalMaterialize for InternalGraph {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::EventGraph(Graph::from_internal_graph(&graph))
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
