use std::{hash::Hash, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::db::api::storage::{graph::GraphStorage, storage::Storage};

use super::graph::Graph;

/// A graph view where an edge remains active from the time it is added until it is explicitly marked as deleted.
///
/// Note that the graph will give you access to all edges that were added at any point in time, even those that are marked as deleted.
/// The deletion only has an effect on the exploded edge view that are returned. An edge is included in a windowed view of the graph if
/// it is considered active at any point in the window.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PersistentGraph(pub(crate) Arc<Storage>);

impl PersistentGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_storage(storage: Arc<Storage>) -> Self {
        Self(storage)
    }

    pub fn from_internal_graph(internal_graph: GraphStorage) -> Self {
        Self(Arc::new(Storage::from_inner(internal_graph)))
    }

    /// Get event graph
    pub fn event_graph(&self) -> Graph {
        Graph::from_storage(self.0.clone())
    }
    pub fn persistent_graph(&self) -> PersistentGraph {
        self.clone()
    }

    pub fn inner(&self) -> &Storage {
        &self.0
    }
}

impl PartialEq for PersistentGraph {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

impl Hash for PersistentGraph {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        todo!()
    }
}
