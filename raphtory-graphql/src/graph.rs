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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::debug;

#[cfg(feature = "vectors")]
use {
    raphtory_vectors::{storage::LazyDiskVectorCache, vectorised_graph::VectorisedGraph},
    tracing::error,
};

const INTO_INNER_RETRY_INTERVAL: Duration = Duration::from_millis(5);

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
        });
        Self { inner }
    }

    /// Waits until we hold the only reference to the underlying Arc and returns its contents.
    /// Other holders (in-flight requests, background eviction flushes) don't notify us when they
    /// drop, so poll for it.
    pub async fn into_inner(self) -> GraphWithVectorsInner {
        let mut inner = self.inner;
        loop {
            match Arc::try_unwrap(inner) {
                Ok(inner) => return inner,
                Err(shared) => {
                    inner = shared;
                    tokio::time::sleep(INTO_INNER_RETRY_INTERVAL).await;
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

    /// Marks the graph as flushing, returning false if a flush was already in progress.
    pub fn try_start_flush(&self) -> bool {
        self.inner
            .is_flushing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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
    ///
    /// The flushing flag is always cleared on return, including when an eviction scan
    /// claimed it through `try_start_flush` before handing the graph over.
    pub fn persist(&self) -> Result<(), GraphError> {
        self.set_flushing(true);
        let result = self.write_to_storage();
        self.set_flushing(false);
        result
    }

    fn write_to_storage(&self) -> Result<(), GraphError> {
        let graph = self.graph();
        if graph.core_graph().is_immutable() {
            return Ok(());
        }
        self.set_dirty(false);
        let flushed = graph.flush();
        let written = self
            .folder()
            .replace_graph_data(graph.clone())
            .map_err(|e| GraphError::ExternalError(Arc::new(e)));
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
        args: Args,
    ) -> Result<Self, GraphError> {
        let folder_clone = folder.clone();
        let graph_folder = folder.graph_folder();
        let graph = if graph_folder.read_metadata()?.is_diskgraph {
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
mod graph_with_vectors_tests {
    use super::GraphWithVectors;
    use crate::{data::Data, paths::ExistingGraphFolder};
    use raphtory::prelude::{Graph, StableEncode};
    use std::time::Duration;

    #[tokio::test]
    async fn into_inner_waits_for_other_references_to_drop() {
        let tmp_work_dir = tempfile::tempdir().unwrap();
        Graph::new().encode(tmp_work_dir.path().join("g")).unwrap();
        let data = Data::new(tmp_work_dir.path(), &Default::default(), Default::default());
        let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, "g").unwrap();
        let graph = GraphWithVectors::new(Graph::new().into(), None, folder);

        let clone = graph.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(clone);
        });

        let unwrapped = tokio::spawn(graph.into_inner());
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(2), unwrapped).await,
                Ok(Ok(_))
            ),
            "into_inner never woke up after the other reference was dropped"
        );
    }
}
