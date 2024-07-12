use crate::{
    db::api::view::{internal::InternalMaterialize, MaterializedGraph},
    prelude::Graph,
};

use super::GraphStorage;

impl InternalMaterialize for GraphStorage {
    fn new_base_graph(&self, graph: GraphStorage) -> MaterializedGraph {
        match self {
            GraphStorage::Mem(_) => {
                MaterializedGraph::EventGraph(Graph::from_internal_graph(&graph))
            }
            GraphStorage::Unlocked(_) => {
                MaterializedGraph::EventGraph(Graph::from_internal_graph(&graph))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => unimplemented!("Figure out how to implement this"),
        }
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
