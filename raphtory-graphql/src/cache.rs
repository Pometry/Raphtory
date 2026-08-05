use crate::{
    data::{InsertionError, MutationErrorInner},
    graph::GraphWithVectors,
    paths::ValidGraphPaths,
    rayon::{blocking_compute, EVICT_POOL},
};
use quick_cache::{
    sync::{Cache, EntryAction, EntryResult},
    DefaultHashBuilder, Lifecycle, UnitWeighter,
};
use std::future::Future;
use tracing::{debug, error};

#[derive(Default, Copy, Clone)]
pub struct ArcPinned;

fn flush_graph(val: GraphWithVectors) -> () {
    if let Err(e) = val.persist() {
        error!("Failed to flush graph {}: {e}", val.folder().local_path())
    }
}

impl Lifecycle<String, GraphWithVectors> for ArcPinned {
    type RequestState = ();

    #[inline]
    fn is_pinned(&self, _key: &String, val: &GraphWithVectors) -> bool {
        if val.ref_count() > 1 {
            return true;
        }

        if val.is_dirty() {
            if !val.is_flushing() {
                let graph = val.clone();
                EVICT_POOL.spawn(move || {
                    debug!("Flushing graph {}", graph.folder().local_path());
                    flush_graph(graph);
                })
            }
            return true;
        }

        val.is_flushing()
    }

    #[inline]
    fn on_evict(&self, _state: &mut Self::RequestState, _key: String, graph: GraphWithVectors) {
        debug_assert_eq!(
            graph.ref_count(),
            1,
            "We should have the only reference to the graph on eviction"
        );
        debug_assert!(!graph.is_dirty(), "Graph should be clean on eviction");
        debug_assert!(
            !graph.is_flushing(),
            "Graph should be already flushed on eviction"
        );

        debug!(
            "Graph {} removed from cache (clean)",
            graph.folder().local_path()
        );
    }
}

pub struct GraphCache {
    cache: Cache<String, GraphWithVectors, UnitWeighter, DefaultHashBuilder, ArcPinned>,
}

impl GraphCache {
    pub fn new(items_capacity: usize) -> Self {
        let cache = Cache::with(
            items_capacity,
            items_capacity as u64,
            Default::default(),
            Default::default(),
            Default::default(),
        );
        Self { cache }
    }

    /// Get item for key if it is cached
    pub fn get(&self, key: &str) -> Option<GraphWithVectors> {
        self.cache.get(key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, GraphWithVectors)> + use<'_> {
        self.cache.iter()
    }

    /// Get item for key. If the item is not found, insert it using the provided future
    pub async fn get_or_insert<E>(
        &self,
        key: &str,
        with: impl Future<Output = Result<GraphWithVectors, E>>,
    ) -> Result<GraphWithVectors, E> {
        self.cache.get_or_insert_async(key, with).await
    }

    /// clear all items from the cache, flushing them if needed
    pub fn flush_and_clear(&self) {
        for (_, graph) in self.cache.drain() {
            flush_graph(graph);
        }
    }

    /// remove a graph from the cache, locking the cache entry until the graph is dropped
    pub async fn remove(&self, key: &str) {
        let res = self
            .cache
            .entry_async(key, |_, _| EntryAction::<()>::ReplaceWithGuard)
            .await;

        match res {
            EntryResult::Replaced(_guard, graph) => {
                blocking_compute(move || flush_graph(graph)).await;
            }
            _ => {}
        }
    }

    /// Insert a new item into the cache, replacing an existing item if it exists
    /// The closure to create the new graph is invoked while holding a guard for the cache key
    pub async fn insert_or_replace_with<E, F>(
        &self,
        key: &str,
        with: impl FnOnce(Option<GraphWithVectors>) -> F,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<GraphWithVectors, E>>,
        E: From<InsertionError>,
    {
        let cache_guard = self
            .cache
            .entry_async(key, |_, _| EntryAction::<()>::ReplaceWithGuard)
            .await;
        let (guard, old_graph) = match cache_guard {
            EntryResult::Replaced(guard, old_graph) => (guard, Some(old_graph)),
            EntryResult::Vacant(guard) => (guard, None),
            _ => {
                unreachable!()
            }
        };
        let new_graph = with(old_graph).await?;
        guard
            .insert(new_graph)
            .map_err(|_| InsertionError::Insertion {
                graph: key.to_string(),
                error: MutationErrorInner::CacheReplacementError,
            })?;
        Ok(())
    }

    /// remove a graph from the cache, locking the cache entry until the future has completed.
    /// if the graph exists, it is passed as input to the closure.
    pub async fn invalidate_with<E, F>(
        &self,
        key: &str,
        with: impl FnOnce(Option<GraphWithVectors>) -> F,
    ) -> E
    where
        F: Future<Output = E>,
    {
        let guard = self
            .cache
            .entry_async(key, |_, _| EntryAction::<()>::ReplaceWithGuard)
            .await;

        match guard {
            EntryResult::Replaced(_guard, graph) => with(Some(graph)).await,
            _ => with(None).await,
        }
    }
}
