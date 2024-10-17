use std::sync::Arc;

use once_cell::sync::OnceCell;
#[cfg(feature = "storage")]
use raphtory::disk_graph::DiskGraphStorage;
use raphtory::{
    core::{
        entities::nodes::node_ref::AsNodeRef,
        utils::errors::{GraphError, GraphResult},
    },
    db::{
        api::{
            mutation::internal::InheritMutationOps,
            view::{internal::Static, Base, InheritViewOps, MaterializedGraph},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{CacheOps, DeletionOps, EdgeViewOps, GraphViewOps, NodeViewOps},
    search::IndexedGraph,
    serialise::GraphFolder,
    vectors::{
        embedding_cache::EmbeddingCache, vectorised_graph::VectorisedGraph, EmbeddingFunction,
    },
};

use crate::paths::ExistingGraphFolder;

#[derive(Clone)]
pub struct GraphWithVectors {
    pub graph: MaterializedGraph,
    pub index: Option<IndexedGraph<MaterializedGraph>>,
    pub vectors: Option<VectorisedGraph<MaterializedGraph>>,
    folder: OnceCell<GraphFolder>,
}

impl GraphWithVectors {
    pub(crate) fn new(
        graph: MaterializedGraph,
        index: Option<IndexedGraph<MaterializedGraph>>,
        vectors: Option<VectorisedGraph<MaterializedGraph>>,
    ) -> Self {
        Self {
            graph,
            index,
            vectors,
            folder: Default::default(),
        }
    }

    pub(crate) async fn update_graph_embeddings(
        &self,
        graph_name: Option<String>,
    ) -> GraphResult<()> {
        if let Some(vectors) = &self.vectors {
            vectors.update_graph(graph_name).await?;
        }
        Ok(())
    }

    pub(crate) async fn update_node_embeddings<T: AsNodeRef>(&self, node: T) -> GraphResult<()> {
        if let Some(vectors) = &self.vectors {
            vectors.update_node(node).await?;
        }
        Ok(())
    }

    pub(crate) async fn update_edge_embeddings<T: AsNodeRef>(
        &self,
        src: T,
        dst: T,
    ) -> GraphResult<()> {
        if let Some(vectors) = &self.vectors {
            vectors.update_edge(src, dst).await?;
        }
        Ok(())
    }

    pub(crate) fn cache(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder = path.into();
        self.folder
            .get_or_try_init(|| Ok::<_, GraphError>(folder.clone()))?;
        self.graph.cache(folder)?;
        self.dump_vectors_to_disk()
    }

    pub(crate) fn write_updates(&self) -> Result<(), GraphError> {
        self.graph.write_updates()?;
        self.dump_vectors_to_disk()
    }

    fn dump_vectors_to_disk(&self) -> Result<(), GraphError> {
        if let Some(vectors) = &self.vectors {
            vectors.write_to_path(
                &self
                    .folder
                    .get()
                    .ok_or(GraphError::CacheNotInnitialised)?
                    .get_vectors_path(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn read_from_folder(
        folder: &ExistingGraphFolder,
        index: bool,
        embedding: Arc<dyn EmbeddingFunction>,
        cache: Arc<Option<EmbeddingCache>>,
    ) -> Result<Self, GraphError> {
        let graph_path = &folder.get_graph_path();
        let graph = if graph_path.is_dir() {
            get_disk_graph_from_path(folder)?
        } else {
            MaterializedGraph::load_cached(folder.clone())?
        };

        let vectors = VectorisedGraph::read_from_path(
            &folder.get_vectors_path(),
            graph.clone(),
            embedding,
            cache,
        );

        println!("Graph loaded = {}", folder.get_original_path_str());
        Ok(Self {
            graph: graph.clone(),
            index: index.then(|| graph.into()),
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
impl InheritMutationOps for GraphWithVectors {}

impl DeletionOps for GraphWithVectors {}

pub(crate) trait UpdateEmbeddings {
    async fn update_embeddings(&self) -> GraphResult<()>;
}

impl UpdateEmbeddings for NodeView<GraphWithVectors> {
    async fn update_embeddings(&self) -> GraphResult<()> {
        self.graph.update_node_embeddings(self.name()).await
    }
}

impl UpdateEmbeddings for EdgeView<GraphWithVectors> {
    async fn update_embeddings(&self) -> GraphResult<()> {
        self.graph
            .update_edge_embeddings(self.src().name(), self.dst().name())
            .await
    }
}
