use crate::paths::{ExistingGraphFolder, ValidGraphPaths};
#[cfg(feature = "search")]
use raphtory::prelude::IndexMutationOps;
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
    prelude::EdgeViewOps,
    serialise::{GraphPaths, StableDecode},
    vectors::{cache::VectorCache, vectorised_graph::VectorisedGraph},
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps, mutation::InheritMutationOps,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing::info;

#[derive(Clone)]
pub struct GraphWithVectors {
    pub graph: MaterializedGraph,
    pub vectors: Option<VectorisedGraph<MaterializedGraph>>,
    pub(crate) folder: ExistingGraphFolder,
    pub(crate) is_dirty: Arc<AtomicBool>,
}

impl GraphWithVectors {
    pub(crate) fn new(
        graph: MaterializedGraph,
        vectors: Option<VectorisedGraph<MaterializedGraph>>,
        folder: ExistingGraphFolder,
    ) -> Self {
        Self {
            graph,
            vectors,
            folder,
            is_dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn set_dirty(&self, is_dirty: bool) {
        self.is_dirty.store(is_dirty, Ordering::SeqCst);
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    /// Generates and stores embeddings for a batch of nodes.
    pub(crate) async fn update_node_embeddings<T: AsNodeRef>(
        &self,
        nodes: Vec<T>,
    ) -> GraphResult<()> {
        if let Some(vectors) = &self.vectors {
            vectors.update_nodes(nodes).await?;
        }

        Ok(())
    }

    /// Generates and stores embeddings for a batch of edges.
    pub(crate) async fn update_edge_embeddings<T: AsNodeRef>(
        &self,
        edges: Vec<(T, T)>,
    ) -> GraphResult<()> {
        if let Some(vectors) = &self.vectors {
            vectors.update_edges(edges).await?;
        }

        Ok(())
    }

    pub(crate) fn read_from_folder(
        folder: &ExistingGraphFolder,
        cache: Option<VectorCache>,
        create_index: bool,
        config: Config,
    ) -> Result<Self, GraphError> {
        let graph_folder = folder.graph_folder();
        let graph = if graph_folder.read_metadata()?.is_diskgraph {
            MaterializedGraph::load_from_path_with_config(graph_folder, config)?
        } else {
            MaterializedGraph::decode_with_config(graph_folder, config)?
        };
        let vectors = cache.and_then(|cache| {
            VectorisedGraph::read_from_path(&folder.vectors_path().ok()?, graph.clone(), cache).ok()
        });

        info!("Graph loaded = {}", folder.local_path());

        #[cfg(feature = "search")]
        if create_index {
            graph.create_index()?;
        }

        Ok(Self {
            graph: graph.clone(),
            vectors,
            folder: folder.clone().into(),
            is_dirty: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl Base for GraphWithVectors {
    type Base = MaterializedGraph;
    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
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
