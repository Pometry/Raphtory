use crate::{graph::GraphWithVectors, paths::ValidGraphPaths, rayon::EVICT_POOL};
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

#[derive(Clone)]
enum DroppingState {
    Dropping {
        wait: Arc<Notify>,
        graph: Arc<GraphWithVectors>,
    },
    Replacing {
        wait: Arc<Notify>,
    },
    DroppedWhileReplacing {
        wait: Arc<Notify>,
    },
}

impl DroppingState {
    fn into_wait(self) -> Arc<Notify> {
        match self {
            Self::Dropping { wait, .. }
            | Self::Replacing { wait }
            | Self::DroppedWhileReplacing { wait } => wait,
        }
    }

    fn as_wait(&self) -> &Arc<Notify> {
        match self {
            Self::Dropping { wait, .. }
            | Self::Replacing { wait }
            | Self::DroppedWhileReplacing { wait } => wait,
        }
    }

    fn new_dropping(graph: Arc<GraphWithVectors>) -> Self {
        let wait = Arc::new(Notify::new());
        Self::Dropping { wait, graph }
    }

    fn as_dropping(&mut self, dropping_graph: Arc<GraphWithVectors>) {
        match self {
            DroppingState::Dropping { graph, .. } => {
                *graph = dropping_graph;
            }
            DroppingState::Replacing { wait } => {
                *self = DroppingState::DroppedWhileReplacing { wait: wait.clone() }
            }
            DroppingState::DroppedWhileReplacing { .. } => {}
        }
    }
}

#[derive(Default, Clone)]
pub struct ArcPinned {
    dropping: Arc<DashMap<String, DroppingState>>,
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
        if Arc::strong_count(val) > 1 {
            return true;
        }
        if val.is_dirty() {

            return true;
        }
        false
    }

    #[inline]
    fn begin_request(&self) -> Self::RequestState {
        ()
    }

    fn on_evict(&self, _state: &mut Self::RequestState, key: String, graph: Arc<GraphWithVectors>) {
        debug_assert_eq!(
            Arc::strong_count(&graph),
            1,
            "We should have the only reference to the graph on eviction"
        );
        if graph.is_dirty() {
            match self.dropping.entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().as_dropping(graph.clone());
                }
                Entry::Vacant(entry) => {
                    entry.insert(DroppingState::new_dropping(graph.clone()));
                }
            };
            let dropping_map = self.dropping.clone();
            EVICT_POOL.spawn(move || {
                debug!(
                    "Graph {} removed from cache (flushing)",
                    graph.folder.local_path()
                );
                drop_graph(graph);
                if let Some((_, state)) = dropping_map.remove(&key) {
                    state.into_wait().notify_waiters() // this makes sure graph is fully dropped before waking up other tasks
                };
            })
        } else {
            debug!(
                "Graph {} removed from cache (clean)",
                graph.folder.local_path()
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

    fn resurrect(&self, key: &str, graph: &Arc<GraphWithVectors>) {
        // resurrect the graph
        let entry = self
            .cache
            .entry(&key, None, |key, graph| EntryAction::Retain(()));
        if let EntryResult::Vacant(placeholder) = entry {
            placeholder.insert(graph.clone()).unwrap_or_else(|graph| {
                error!("Failed to resurrect graph {}", graph.folder.local_path());
            });
        }
    }

    /// Get item for key, resurrecting it if it is currently being dropped or looking it up in the cache
    pub async fn get(&self, key: String) -> Option<Arc<GraphWithVectors>> {
        let wait = match self.dropping.dropping.entry(key.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                DroppingState::Dropping { graph, .. } => {
                    self.resurrect(&key, graph);
                    return Some(graph.clone());
                }
                DroppingState::Replacing { wait }
                | DroppingState::DroppedWhileReplacing { wait } => wait,
            },
            Entry::Vacant(entry) => return self.cache.get(&entry.into_key()),
        };
        // have to wait for replacement to finish before trying again
        wait.notified().await;
        self.get(key).await
    }

    /// Get item for key, resurrecting it if it is currently being dropped or looking it up in the cache.
    /// If the item is not found, insert it using the provided future
    pub async fn get_or_insert<E>(
        &self,
        key: String,
        with: impl Future<Output = Result<Arc<GraphWithVectors>, E>>,
    ) -> Result<Arc<GraphWithVectors>, E> {
        let wait = match self.dropping.dropping.entry(key) {
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    DroppingState::Dropping { graph, .. } => {
                        self.resurrect(entry.key(), graph);
                        return Ok(graph.clone())
                    }
                    DroppingState::Replacing { wait } |
                    DroppingState::DroppedWhileReplacing { wait } => {wait}
                }
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
        with: impl Future<Output = Result<Arc<GraphWithVectors>, E>>,
    ) -> Result<(), E> {
        self.dropping.dropping.remove(&key); // make sure we don't resurrect the old graph if it is still being dropped
        let cache_guard = tokio::spawn(
            self.cache
                .entry_async(&key, |key, value| EntryAction::<()>::ReplaceWithGuard),
        );
        let new_graph = tokio::spawn(with);
        let (guard, graph) = join!(cache_guard, new_graph);

        match guard? {
            EntryResult::Replaced(guard, _) | EntryResult::Vacant(guard) => guard.insert(graph??),
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
