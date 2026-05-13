use crate::{
    paths::{ExistingGraphFolder, ValidGraphPaths},
    rayon::blocking_compute,
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
    prelude::{EdgeViewOps, StableDecode},
    serialise::GraphPaths,
    vectors::{storage::LazyDiskVectorCache, vectorised_graph::VectorisedGraph},
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps, mutation::InheritMutationOps,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[cfg(feature = "search")]
use raphtory::prelude::IndexMutationOps;

#[derive(Clone)]
pub struct GraphWithVectors {
    inner: Arc<GraphWithVectorsInner>,
}

struct GraphWithVectorsInner {
    graph: MaterializedGraph,
    vectors: Option<VectorisedGraph<MaterializedGraph>>,
    folder: ExistingGraphFolder,
    is_dirty: AtomicBool,
    is_flushing: AtomicBool,
}

impl GraphWithVectors {
    pub fn new(
        graph: MaterializedGraph,
        vectors: Option<VectorisedGraph<MaterializedGraph>>,
        folder: ExistingGraphFolder,
    ) -> Self {
        let inner = Arc::new(GraphWithVectorsInner {
            graph,
            vectors,
            folder,
            is_dirty: AtomicBool::new(false),
            is_flushing: AtomicBool::new(false),
        });
        Self { inner }
    }

    pub fn graph(&self) -> &MaterializedGraph {
        &self.inner.graph
    }

    pub fn vectors(&self) -> Option<&VectorisedGraph<MaterializedGraph>> {
        self.inner.vectors.as_ref()
    }

    pub fn folder(&self) -> &ExistingGraphFolder {
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

    /// Generates and stores embeddings for a batch of nodes.
    pub(crate) async fn update_node_embeddings<T: AsNodeRef>(
        &self,
        nodes: Vec<T>,
    ) -> GraphResult<()> {
        if let Some(vectors) = &self.inner.vectors {
            vectors.update_nodes(nodes).await?;
        }

        Ok(())
    }

    /// Generates and stores embeddings for a batch of edges.
    pub(crate) async fn update_edge_embeddings<T: AsNodeRef>(
        &self,
        edges: Vec<(T, T)>,
    ) -> GraphResult<()> {
        if let Some(vectors) = &self.inner.vectors {
            vectors.update_edges(edges).await?;
        }

        Ok(())
    }

    pub(crate) async fn read_from_folder(
        folder: &ExistingGraphFolder,
        cache: &LazyDiskVectorCache,
        create_index: bool,
        config: Config,
    ) -> Result<Self, GraphError> {
        let folder_clone = folder.clone();
        let graph_folder = folder.graph_folder();
        let graph = if graph_folder.read_metadata()?.is_diskgraph {
            blocking_compute(move || {
                MaterializedGraph::load_with_config(folder_clone.graph_folder(), config)
            })
            .await?
        } else {
            blocking_compute(move || {
                MaterializedGraph::decode_with_config(folder_clone.graph_folder(), config)
            })
            .await?
        };
        let vectors =
            VectorisedGraph::read_from_path(&folder.vectors_path()?, graph.clone(), cache)
                .await
                .ok();

        println!("Graph loaded = {}", folder.local_path());
        #[cfg(feature = "search")]
        if create_index {
            graph.create_index()?;
        }

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
