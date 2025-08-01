use crate::paths::ExistingGraphFolder;
use once_cell::sync::OnceCell;
use raphtory::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::view::{
            internal::{
                InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps, Static,
            },
            Base, InheritViewOps, MaterializedGraph,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::{GraphError, GraphResult},
    prelude::{CacheOps, EdgeViewOps, IndexMutationOps, NodeViewOps},
    serialise::GraphFolder,
    storage::core_ops::CoreGraphOps,
    vectors::{cache::VectorCache, vectorised_graph::VectorisedGraph},
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, graph::graph::GraphStorage, layer_ops::InheritLayerOps,
    mutation::InheritMutationOps,
};

#[cfg(feature = "storage")]
use {raphtory::prelude::IntoGraph, raphtory_storage::disk::DiskGraphStorage};

#[derive(Clone)]
pub struct GraphWithVectors {
    pub graph: MaterializedGraph,
    pub vectors: Option<VectorisedGraph<MaterializedGraph>>,
    pub(crate) folder: OnceCell<GraphFolder>,
}

impl GraphWithVectors {
    pub(crate) fn new(
        graph: MaterializedGraph,
        vectors: Option<VectorisedGraph<MaterializedGraph>>,
    ) -> Self {
        Self {
            graph,
            vectors,
            folder: Default::default(),
        }
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

    pub(crate) fn write_updates(&self) -> Result<(), GraphError> {
        match self.graph.core_graph() {
            GraphStorage::Mem(_) | GraphStorage::Unlocked(_) => self.graph.write_updates(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => Ok(()),
        }
    }

    pub(crate) fn read_from_folder(
        folder: &ExistingGraphFolder,
        cache: Option<VectorCache>,
        create_index: bool,
    ) -> Result<Self, GraphError> {
        let graph_path = &folder.get_graph_path();
        let graph = if graph_path.is_dir() {
            get_disk_graph_from_path(folder)?
        } else {
            MaterializedGraph::load_cached(folder.clone())?
        };
        let vectors = cache.and_then(|cache| {
            VectorisedGraph::read_from_path(&folder.get_vectors_path(), graph.clone(), cache).ok()
        });
        println!("Graph loaded = {}", folder.get_original_path_str());
        if create_index {
            graph.create_index()?;
            graph.write_updates()?;
        }
        Ok(Self {
            graph: graph.clone(),
            vectors,
            folder: OnceCell::with_value(folder.clone().into()),
        })
    }
}

#[cfg(feature = "storage")]
fn get_disk_graph_from_path(path: &ExistingGraphFolder) -> Result<MaterializedGraph, GraphError> {
    let disk_graph = DiskGraphStorage::load_from_dir(&path.get_graph_path())
        .map_err(|e| GraphError::LoadFailure(e.to_string()))?;
    let graph: MaterializedGraph = disk_graph.into_graph().into(); // TODO: We currently have no way to identify disk graphs as MaterializedGraphs
    println!("Disk Graph loaded = {}", path.get_original_path().display());
    Ok(graph)
}

#[cfg(not(feature = "storage"))]
fn get_disk_graph_from_path(path: &ExistingGraphFolder) -> Result<MaterializedGraph, GraphError> {
    Err(GraphError::GraphNotFound(path.to_error_path()))
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
        self.graph.update_node_embeddings(vec![self.name()]).await
    }
}

impl UpdateEmbeddings for EdgeView<GraphWithVectors> {
    async fn update_embeddings(&self) -> GraphResult<()> {
        self.graph
            .update_edge_embeddings(vec![(self.src().name(), self.dst().name())])
            .await
    }
}
