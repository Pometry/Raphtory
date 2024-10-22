use std::{
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::db::api::{storage::{graph::GraphStorage, storage::Storage}, view::internal::inherit::Base};

use super::deletion_graph::PersistentGraph;

#[repr(transparent)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Graph {
    pub(crate) inner: Arc<Storage>,
}

impl Base for Graph {
    type Base = Storage;

    #[inline(always)]
    fn base(&self) -> &Self::Base {
        self.inner()
    }
}

impl Display for Graph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl PartialEq for Graph {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

impl Hash for Graph {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        todo!()
    }
}

impl Graph {
    /// Create a new graph
    ///
    /// Returns:
    ///
    /// A raphtory graph
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Storage::default()),
        }
    }

    pub fn inner(&self) -> &Storage {
        &self.inner
    }

    /// Create a new graph with specified number of shards
    ///
    /// Returns:
    ///
    /// A raphtory graph
    pub fn new_with_shards(num_shards: usize) -> Self {
        Self {
            inner: Arc::new(Storage::new(num_shards)),
        }
    }

    pub fn from_storage(inner: Arc<Storage>) -> Self {
        Self { inner }
    }

    pub fn from_internal_graph(graph_storage: GraphStorage) -> Self {
        let inner = Arc::new(Storage::from_inner(graph_storage));
        Self { inner }
    }

    pub fn event_graph(&self) -> Graph {
        self.clone()
    }

    /// Get persistent graph
    pub fn persistent_graph(&self) -> PersistentGraph {
        PersistentGraph::from_storage(self.inner.clone())
    }
}
