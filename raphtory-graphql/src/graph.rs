use crate::{
    paths::{ExistingGraphFolder, UnlockedGraphFolder, ValidGraphPaths},
    rayon::blocking_load,
};
use raphtory::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            storage::storage::Args,
            view::{
                internal::{
                    InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps, Static,
                },
                Base, InheritViewOps, MaterializedGraph,
            },
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::{GraphError, GraphResult},
    prelude::{AdditionOps, EdgeViewOps, StableDecode},
};
use raphtory_api::core::storage::graph_folder::GraphPaths;
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    layer_ops::InheritLayerOps,
    mutation::InheritMutationOps,
};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tracing::debug;

#[cfg(feature = "vectors")]
use {
    raphtory_vectors::{storage::LazyDiskVectorCache, vectorised_graph::VectorisedGraph},
    tracing::error,
};

/// The element stored in the optional vectors slot of a graph. With the
/// `vectors` feature this is a real `VectorisedGraph`; without it the slot is
/// uninhabited so it is always empty, keeping `GraphWithVectors` and all its
/// call sites identical across both builds.
#[cfg(feature = "vectors")]
pub type GraphVectors = VectorisedGraph<MaterializedGraph>;
#[cfg(not(feature = "vectors"))]
pub type GraphVectors = ();

#[derive(Clone)]
pub struct GraphWithVectors {
    inner: Arc<GraphWithVectorsInner>,
}

pub struct GraphWithVectorsInner {
    pub graph: MaterializedGraph,
    pub vectors: Option<GraphVectors>,
    pub folder: UnlockedGraphFolder,
    pub is_dirty: AtomicBool,
    pub is_flushing: AtomicBool,
    pub plugin_data: PluginData,
}

/// Values server plugins keep for one served graph, one per type, dropped when the server drops
/// the graph.
#[derive(Clone, Default)]
pub struct PluginData(Arc<Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>);

impl PluginData {
    /// The value of type `T`, made by `init` on first use.
    pub fn get_or_init<T: Any + Send + Sync>(&self, init: impl FnOnce() -> T) -> Arc<T> {
        let mut values = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let value = values
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Arc::new(init()));
        value.clone().downcast().expect("keyed by its type")
    }
}

impl GraphWithVectors {
    pub fn new(
        graph: MaterializedGraph,
        vectors: Option<GraphVectors>,
        folder: ExistingGraphFolder,
    ) -> Self {
        let inner = Arc::new(GraphWithVectorsInner {
            graph,
            vectors,
            folder: folder.unlock(),
            is_dirty: AtomicBool::new(false),
            is_flushing: AtomicBool::new(false),
            plugin_data: PluginData::default(),
        });
        Self { inner }
    }

    pub fn plugin_data(&self) -> &PluginData {
        &self.inner.plugin_data
    }

    /// Calls `Arc::try_unwrap` on the underlying Arc until we hold the only reference and returns it
    pub async fn into_inner(self) -> GraphWithVectorsInner {
        let mut inner = self.inner;
        loop {
            match Arc::try_unwrap(inner) {
                Ok(inner) => return inner,
                Err(shared) => {
                    inner = shared;
                    // shortcut: polls, as nothing signals the last other handle's drop; a
                    // `Pending` with no waker registered was never polled again.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Swap in a read-only handle for the graph. No-op with a warning if the inner
    /// state is unexpectedly shared (only call right after construction).
    pub(crate) fn into_read_only(self) -> Self {
        match Arc::try_unwrap(self.inner) {
            Ok(mut inner) => {
                inner.graph = inner.graph.read_only();
                Self {
                    inner: Arc::new(inner),
                }
            }
            Err(inner) => {
                tracing::warn!("graph handle shared during load; serving it without read-only");
                Self { inner }
            }
        }
    }

    pub fn graph(&self) -> &MaterializedGraph {
        &self.inner.graph
    }

    pub fn vectors(&self) -> Option<&GraphVectors> {
        self.inner.vectors.as_ref()
    }

    pub fn folder(&self) -> &UnlockedGraphFolder {
        &self.inner.folder
    }
    pub fn set_dirty(&self, is_dirty: bool) {
        self.inner.is_dirty.store(is_dirty, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty.load(Ordering::Acquire)
    }

    pub fn is_flushing(&self) -> bool {
        self.inner.is_flushing.load(Ordering::Acquire)
    }

    pub fn set_flushing(&self, is_flushing: bool) {
        self.inner.is_flushing.store(is_flushing, Ordering::Release)
    }

    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Flush in-memory writes to the storage engine and rewrite the on-disk
    /// metadata sidecar, so cache-miss namespace listings report accurate
    /// counts. The dirty flag is cleared up front so a mutation racing the
    /// flush re-marks the graph dirty. Both steps are attempted independently
    /// and the first error is returned; callers decide whether a failure
    /// should re-mark the graph dirty for a later retry.
    pub fn persist(&self) -> Result<(), GraphError> {
        let graph = self.graph();
        let is_immutable = graph.core_graph().is_immutable();
        if !is_immutable {
            self.set_flushing(true);
            self.set_dirty(false);
            let flushed = graph.flush();
            let written = self
                .folder()
                .replace_graph_data(self.graph().clone())
                .map_err(|e| GraphError::ExternalError(Arc::new(e)));
            self.set_flushing(false);
            flushed.and(written)
        } else {
            Ok(())
        }
    }

    /// Generates and stores embeddings for a batch of nodes.
    pub(crate) async fn update_node_embeddings<T: AsNodeRef>(
        &self,
        nodes: Vec<T>,
    ) -> GraphResult<()> {
        #[cfg(feature = "vectors")]
        if let Some(vectors) = &self.inner.vectors {
            vectors.update_nodes(nodes).await?;
        }
        #[cfg(not(feature = "vectors"))]
        let _ = nodes;

        Ok(())
    }

    /// Generates and stores embeddings for a batch of edges.
    pub(crate) async fn update_edge_embeddings<T: AsNodeRef>(
        &self,
        edges: Vec<(T, T)>,
    ) -> GraphResult<()> {
        #[cfg(feature = "vectors")]
        if let Some(vectors) = &self.inner.vectors {
            vectors.update_edges(edges).await?;
        }
        #[cfg(not(feature = "vectors"))]
        let _ = edges;

        Ok(())
    }

    pub(crate) async fn read_from_folder(
        folder: &ExistingGraphFolder,
        #[cfg(feature = "vectors")] cache: &LazyDiskVectorCache,
        args: Args,
    ) -> Result<Self, GraphError> {
        let folder_clone = folder.clone();
        let graph_folder = folder.graph_folder();
        let graph = if graph_folder.read_graph_meta()?.meta.is_diskgraph {
            blocking_load(move || {
                MaterializedGraph::load_with_config(folder_clone.graph_folder(), args)
            })
            .await?
        } else {
            blocking_load(move || {
                MaterializedGraph::decode_with_config(folder_clone.graph_folder(), args)
            })
            .await?
        };

        #[cfg(feature = "vectors")]
        let vectors = {
            let vectors_path = folder.vectors_path()?;
            match VectorisedGraph::read_from_path(&vectors_path, graph.clone(), cache).await {
                Ok(vectors) => Some(vectors),
                Err(error) => {
                    // a graph that was never vectorised has no vectors dir, that is not a failure
                    if vectors_path.exists() {
                        error!(
                            "Could not load the vectors of graph {}: {error}",
                            folder.local_path()
                        );
                    }
                    None
                }
            }
        };

        #[cfg(not(feature = "vectors"))]
        let vectors = None;

        debug!("Graph loaded = {}", folder.local_path());

        Ok(Self::new(graph, vectors, folder.clone()))
    }
}

impl Base for GraphWithVectors {
    type Base = MaterializedGraph;
    #[inline]
    fn base(&self) -> &Self::Base {
        &self.inner.graph
    }
}

impl Static for GraphWithVectors {}

impl InheritViewOps for GraphWithVectors {}

impl InheritCoreGraphOps for GraphWithVectors {}

impl InheritLayerOps for GraphWithVectors {}

impl InheritNodeHistoryFilter for GraphWithVectors {}

impl InheritEdgeHistoryFilter for GraphWithVectors {}

impl InheritMutationOps for GraphWithVectors {}

impl InheritStorageOps for GraphWithVectors {}

pub(crate) trait UpdateEmbeddings {
    async fn update_embeddings(&self) -> GraphResult<()>;
}

impl UpdateEmbeddings for NodeView<'static, GraphWithVectors> {
    async fn update_embeddings(&self) -> GraphResult<()> {
        self.graph.update_node_embeddings(vec![self.node]).await
    }
}

impl UpdateEmbeddings for EdgeView<GraphWithVectors> {
    async fn update_embeddings(&self) -> GraphResult<()> {
        self.graph
            .update_edge_embeddings(vec![(self.src().node, self.dst().node)])
            .await
    }
}

#[cfg(test)]
mod plugin_data_tests {
    use super::PluginData;
    use std::sync::Arc;

    #[test]
    fn plugin_data_keeps_one_value_per_type() {
        let data = PluginData::default();
        let first = data.get_or_init(|| 1u32);
        assert!(Arc::ptr_eq(&first, &data.get_or_init(|| 2u32)));
        assert_eq!(*data.clone().get_or_init(|| 3u32), 1);
        assert_eq!(*data.get_or_init(|| "other type"), "other type");
    }
}
