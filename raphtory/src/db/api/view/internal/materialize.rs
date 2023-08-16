use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::{Base, DynamicGraph, IntoDynamic},
        graph::{
            graph::{Graph, InternalGraph},
            views::deletion_graph::GraphWithDeletions,
        },
    },
};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub enum MaterializedGraph {
    EventGraph(Graph),
    PersistentGraph(GraphWithDeletions),
}

impl From<Graph> for MaterializedGraph {
    fn from(value: Graph) -> Self {
        MaterializedGraph::EventGraph(value)
    }
}

impl From<GraphWithDeletions> for MaterializedGraph {
    fn from(value: GraphWithDeletions) -> Self {
        MaterializedGraph::PersistentGraph(value)
    }
}

impl IntoDynamic for MaterializedGraph {
    fn into_dynamic(self) -> DynamicGraph {
        match self {
            MaterializedGraph::EventGraph(g) => g.into_dynamic(),
            MaterializedGraph::PersistentGraph(g) => g.into_dynamic(),
        }
    }
}

impl MaterializedGraph {
    pub fn into_events(self) -> Option<Graph> {
        match self {
            MaterializedGraph::EventGraph(g) => Some(g),
            MaterializedGraph::PersistentGraph(_) => None,
        }
    }
    pub fn into_persistent(self) -> Option<GraphWithDeletions> {
        match self {
            MaterializedGraph::EventGraph(_) => None,
            MaterializedGraph::PersistentGraph(g) => Some(g),
        }
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphError> {
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        Ok(bincode::deserialize_from(&mut reader)?)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        let f = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(f);
        Ok(bincode::serialize_into(&mut writer, self)?)
    }
}

pub trait InternalMaterialize {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph;

    fn include_deletions(&self) -> bool;
}

pub trait InheritMaterialize: Base {}

impl<G: InheritMaterialize> InternalMaterialize for G
where
    G::Base: InternalMaterialize,
{
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        self.base().new_base_graph(graph)
    }

    fn include_deletions(&self) -> bool {
        self.base().include_deletions()
    }
}
