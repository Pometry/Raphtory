use crate::{graph::GraphWithVectors, paths::ValidGraphPaths, rayon::EVICT_POOL};
use ahash::HashMap;
use dashmap::{DashMap, Entry};
use parking_lot::Mutex;
use quick_cache::{unsync::Cache, DefaultHashBuilder, Lifecycle, UnitWeighter, Weighter};
use raphtory::{
    db::api::{storage::storage::PersistenceStrategy, view::internal::InternalStorageOps},
    prelude::AdditionOps,
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{future::Future, marker::PhantomData, sync::Arc};
use tokio::join;
use tracing::{debug, error};

#[derive(Default, Clone)]
pub struct ArcPinned {
    dropping: Arc<HashMap<String, Arc<GraphWithVectors>>>,
}

pub struct CacheShard {
    dropping: ArcPinned,
    cache: Cache<String, Arc<GraphWithVectors>, UnitWeighter, DefaultHashBuilder, ArcPinned>,
}

fn drop_graph(val: Arc<GraphWithVectors>) -> () {
    let graph = val.graph;
    if let Err(e) = graph.flush() {
        error!("Failed to flush graph {}: {e}", val.folder.local_path())
    }
    if let Err(e) = val.folder.replace_graph_data(graph) {
        error!("Failed to write graph {}: {e}", val.folder.local_path())
    }
}

impl Lifecycle<String, Arc<GraphWithVectors>> for ArcPinned {
    type RequestState = ();

    #[inline]
    fn is_pinned(&self, _key: &String, val: &Arc<GraphWithVectors>) -> bool {
        Arc::strong_count(val) > 1
    }

    #[inline]
    fn begin_request(&self) -> Self::RequestState {
        ()
    }

    fn on_evict(&self, _state: &mut Self::RequestState, key: String, val: Arc<GraphWithVectors>) {
        if val.is_dirty() {
            self.dropping.insert(key.clone(), val.clone());
            let dropping_map = self.dropping.clone();
            EVICT_POOL.spawn(move || {
                debug!(
                    "Graph {} removed from cache (flushing)",
                    val.folder.local_path()
                );
                drop_graph(val);
                dropping_map.remove(&key);
            })
        } else {
            debug!(
                "Graph {} removed from cache (clean)",
                val.folder.local_path()
            )
        }
    }
}

pub struct GraphCache {
    cache: Cache<String, Arc<GraphWithVectors>, UnitWeighter, DefaultHashBuilder, ArcPinned>,
    dropping: ArcPinned,
}

impl GraphCache {
    pub fn new(items_capacity: usize) -> Self {
        let dropping = ArcPinned::default();
        let cache = Cache::with(
            items_capacity,
            items_capacity as u64,
            Default::default(),
            Default::default(),
            dropping.clone(),
        );
        Self { cache, dropping }
    }

    /// Get item for key, resurrecting it if it is currently being dropped or looking it up in the cache
    pub fn get(&self, key: String) -> Option<Arc<GraphWithVectors>> {
        match self.dropping.dropping.entry(key) {
            Entry::Occupied(entry) => {
                let (key, value) = entry.remove_entry();
                self.cache.insert(key, value.clone());
                Some(value)
            }
            Entry::Vacant(entry) => self.cache.get(&entry.into_key()),
        }
    }

    /// Get item for key, resurrecting it if it is currently being dropped or looking it up in the cache.
    /// If the item is not found, insert it using the provided future
    pub async fn get_or_insert<E>(
        &self,
        key: String,
        with: impl Future<Output = Result<Arc<GraphWithVectors>, E>>,
    ) -> Result<Arc<GraphWithVectors>, E> {
        match self.dropping.dropping.entry(key) {
            Entry::Occupied(entry) => {
                let (key, value) = entry.remove_entry();
                self.cache.insert(key, value.clone());
                Ok(value)
            }
            Entry::Vacant(entry) => {
                self.cache
                    .get_or_insert_async(&entry.into_key(), with)
                    .await
            }
        }
    }

    pub async fn insert_with<E>(
        &self,
        key: String,
        with: impl Future<Output = Result<Arc<GraphWithVectors>>, E>,
    ) -> Result<(), E> {
        self.dropping.dropping.remove(&key); // make sure we don't resurrect the old graph if it is still being dropped
        let cache_guard = tokio::spawn(
            self.cache
                .entry_async(&key, |key, value| EntryAction::<()>::ReplaceWithGuard),
        );
        let new_graph = tokio::spawn(with);
        let (guard, graph) = join!(cache_guard, new_graph);

        match res {
            EntryResult::Replaced(guard, _) | EntryResult::Vacant(guard) => {}
            _ => {
                unreachable!()
            }
        }
    }

    /// drain all items from the cache
    pub fn drain(
        &self,
    ) -> Drain<'_, String, Arc<GraphWithVectors>, UnitWeighter, DefaultHashBuilder, ArcPinned> {
        self.cache.drain()
    }

    /// remove a graph from the cache without triggering the eviction drop logic
    pub fn remove(&self, key: &str) -> Option<Arc<GraphWithVectors>> {
        self.cache
            .remove(key)
            .or_else(|| self.dropping.dropping.remove(key))
            .map(|(_, v)| v)
    }
}
