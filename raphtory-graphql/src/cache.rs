use crate::{
    data::{InsertionError, MutationErrorInner},
    graph::GraphWithVectors,
    paths::ValidGraphPaths,
    rayon::{blocking_compute, EVICT_POOL},
    GQLError,
};
use ahash::HashMap;
use dashmap::{DashMap, Entry};
use parking_lot::Mutex;
use quick_cache::{
    sync::{Cache, Drain, EntryAction, EntryResult},
    DefaultHashBuilder, Lifecycle, UnitWeighter, Weighter,
};
use raphtory::{
    db::api::{storage::storage::PersistenceStrategy, view::internal::InternalStorageOps},
    prelude::AdditionOps,
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{future::Future, marker::PhantomData, sync::Arc};
use tokio::{join, sync::Notify};
use tracing::{debug, error};

#[derive(Default, Copy, Clone)]
pub struct ArcPinned;

pub struct CacheShard {
    cache: Cache<String, GraphWithVectors, UnitWeighter, DefaultHashBuilder, ArcPinned>,
}

fn flush_graph(val: GraphWithVectors) -> () {
    val.set_flushing(true);
    val.set_dirty(false); // make sure this is reset before the flush so any mutation that gets triggered afterwards will set the graph back to dirty
    let graph = val.graph();
    if let Err(e) = graph.flush() {
        error!("Failed to flush graph {}: {e}", val.folder().local_path())
    }
    if let Err(e) = val.folder().replace_graph_data(graph.clone()) {
        error!("Failed to write graph {}: {e}", val.folder().local_path())
    }
    val.set_flushing(false);
}

impl Lifecycle<String, GraphWithVectors> for ArcPinned {
    type RequestState = ();

    #[inline]
    fn begin_request(&self) -> Self::RequestState {}

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
    fn on_evict(&self, state: &mut Self::RequestState, key: String, graph: GraphWithVectors) {
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

    /// Insert a new item into the cache, replacing an existing item if it exists
    pub async fn insert_with<E>(
        &self,
        key: &str,
        with: impl Future<Output = Result<GraphWithVectors, E>>,
    ) -> Result<(), InsertionError>
    where
        InsertionError: From<E>,
    {
        let new_graph = with.await?;
        let cache_guard = self
            .cache
            .entry_async(key, |key, value| EntryAction::<()>::ReplaceWithGuard)
            .await;
        match cache_guard {
            EntryResult::Replaced(guard, old_graph) => {
                drop(old_graph);
                guard.insert(new_graph)
            }
            EntryResult::Vacant(guard) => guard.insert(new_graph),
            _ => {
                unreachable!()
            }
        }
        .map_err(|_| InsertionError::Insertion {
            graph: key.to_string(),
            error: MutationErrorInner::CacheReplacementError,
        })?;
        Ok(())
    }

    /// clear all items from the cache, flushing them if needed
    pub fn flush_and_clear(&self) {
        for (_, graph) in self.cache.drain() {
            flush_graph(graph);
        }
    }

    /// remove a graph from the cache without triggering the eviction drop logic
    /// Note that the cache entry is available again immediately!
    pub async fn remove(&self, key: &str) -> Option<GraphWithVectors> {
        let res = self
            .cache
            .entry_async(key, |key, graph| EntryAction::<()>::Remove)
            .await;
        match res {
            EntryResult::Removed(_, graph) => Some(graph),
            _ => None,
        }
    }

    /// remove a graph from the cache, locking the cache entry until the graph is dropped
    /// this is different from remove which returns the graph and unlocks the entry immediately
    pub async fn delete(&self, key: &str) {
        let res = self
            .cache
            .entry_async(key, |key, graph| EntryAction::<()>::ReplaceWithGuard)
            .await;

        match res {
            EntryResult::Replaced(_guard, graph) => {
                blocking_compute(move || drop(graph)).await;
            }
            _ => {}
        }
    }

    /// remove a graph from the cache, locking the cache entry until the graph is dropped and the future has completed.
    /// if the graph exists, it is dropped first before the future runs
    pub async fn invalidate_with<E>(&self, key: &str, with: impl Future<Output = E>) -> E {
        let guard = self
            .cache
            .entry_async(key, |key, graph| EntryAction::<()>::ReplaceWithGuard)
            .await;

        match guard {
            EntryResult::Replaced(_guard, graph) => {
                blocking_compute(move || drop(graph)).await;
                with.await
            }
            _ => with.await,
        }
    }
}
