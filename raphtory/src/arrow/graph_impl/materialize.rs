use crate::db::{
    api::view::{internal::InternalMaterialize, MaterializedGraph},
    graph::graph::InternalGraph,
};

use super::Graph2;

impl InternalMaterialize for Graph2 {
    fn new_base_graph(&self, _graph: InternalGraph) -> MaterializedGraph {
        todo!()
    }

    fn include_deletions(&self) -> bool {
        todo!()
    }
}
