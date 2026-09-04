use crate::{
    model::schema::cache::SchemaCache,
    paths::{ExistingGraphFolder, UnlockedGraphFolder, ValidGraphPaths},
    rayon::blocking_load,
};
use raphtory::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            storage::storage::Config,
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
    core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps, mutation::InheritMutationOps,
};
use std::{
    future::poll_fn,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Poll,
};
use tracing::debug;

#[cfg(feature = "vectors")]
use {
    raphtory::vectors::{storage::LazyDiskVectorCache, vectorised_graph::VectorisedGraph},
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
    /// Cache of computed node/edge schemas for the unfiltered base view of this graph.
    /// Cleared on every mutation.
    pub(crate) schema_cache: Arc<SchemaCache>,
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
            schema_cache: Arc::new(SchemaCache::new()),
        });
        Self { inner }
    }

    /// Calls `Arc::into_inner` on the underlying Arc until we hold the only reference and returns it
    pub async fn into_inner(self) -> GraphWithVectorsInner {
        let mut inner = Some(self.inner);
        let future = poll_fn(move |_ctx| {
            match inner.take() {
                None => {
                    unreachable!("poll called after ready returned")
                }
                Some(inner_arc) => {
                    match Arc::try_unwrap(inner_arc) {
                        Ok(inner) => Poll::Ready(inner),
                        Err(inner_arc) => {
                            inner = Some(inner_arc); // put back
                            Poll::Pending
                        }
                    }
                }
            }
        });
        future.await
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

    /// Handle to this graph's schema cache.
    pub(crate) fn schema_cache(&self) -> Arc<SchemaCache> {
        self.inner.schema_cache.clone()
    }

    /// Drop all cached schemas. Called after every mutation.
    pub fn invalidate_schema_cache(&self) {
        self.inner.schema_cache.invalidate();
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
        self.set_flushing(true);
        self.set_dirty(false);
        let flushed = self.graph().flush();
        let written = self
            .folder()
            .replace_graph_data(self.graph().clone())
            .map_err(|e| GraphError::ExternalError(Arc::new(e)));
        self.set_flushing(false);
        flushed.and(written)
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
        config: Config,
    ) -> Result<Self, GraphError> {
        let folder_clone = folder.clone();
        let graph_folder = folder.graph_folder();
        let graph = if graph_folder.read_metadata()?.is_diskgraph {
            blocking_load(move || {
                MaterializedGraph::load_with_config(folder_clone.graph_folder(), config)
            })
            .await?
        } else {
            blocking_load(move || {
                MaterializedGraph::decode_with_config(folder_clone.graph_folder(), config)
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
