use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{core::entities::graph::tgraph::TemporalGraph, db::api::view::internal::inherit::Base};

use super::graph::GraphStorage;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Storage {
    graph: GraphStorage,
    #[cfg(feature = "proto")]
    #[serde(skip)]
    pub(crate) cache: OnceCell<GraphWriter>,
    // search index (tantivy)
    // vector index
}

impl Base for Storage {
    type Base = GraphStorage;

    #[inline]
    fn base(&self) -> &Self::Base {
        self.graph()
    }
}

impl Storage {
    pub fn graph(&self) -> &GraphStorage {
        &self.graph
    }

    pub(crate) fn new(num_locks: usize) -> Self {
        Self {
            graph: GraphStorage::Unlocked(Arc::new(TemporalGraph::new(num_locks))),
            #[cfg(feature = "proto")]
            cache: OnceCell::new(),
        }
    }

    pub fn from_inner(graph: GraphStorage) -> Self {
        Self {
            graph,
            #[cfg(feature = "proto")]
            cache: OnceCell::new(),
        }
    }

    #[cfg(feature = "proto")]
    #[inline]
    fn if_cache(&self, map_fn: impl FnOnce(&GraphWriter)) {
        if let Some(cache) = self.cache.get() {
            map_fn(cache)
        }
    }
}
impl Display for Storage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.graph, f)
    }
}
