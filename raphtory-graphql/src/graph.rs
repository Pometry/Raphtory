use std::{fs, sync::Arc};

use once_cell::sync::OnceCell;
#[cfg(feature = "storage")]
use raphtory::disk_graph::DiskGraphStorage;
use raphtory::{
    core::{
        entities::nodes::node_ref::AsNodeRef,
        utils::errors::{GraphError, InvalidPathReason::*},
    },
    db::{
        api::{
            mutation::internal::InheritMutationOps,
            view::{internal::Static, Base, InheritViewOps, MaterializedGraph},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{CacheOps, EdgeViewOps, NodeViewOps},
    search::IndexedGraph,
    serialise::GraphFolder,
    vectors::{
        embedding_cache::EmbeddingCache, embeddings::openai_embedding,
        vectorised_graph::VectorisedGraph, EmbeddingFunction,
    },
};

use crate::paths::ExistingGraphFolder;

#[derive(Clone)]
pub struct GraphWithVectors {
    pub graph: IndexedGraph<MaterializedGraph>,
    pub vectors: Option<VectorisedGraph<MaterializedGraph>>,
    folder: OnceCell<GraphFolder>,
}

// TODO: I don't think Im using these anywhere
// FIXME: need to also write to disk!!!!!!!!!
impl GraphWithVectors {
    pub(crate) fn new(
        graph: IndexedGraph<MaterializedGraph>,
        vectors: Option<VectorisedGraph<MaterializedGraph>>,
    ) -> Self {
        Self {
            graph,
            vectors,
            folder: Default::default(),
        }
    }

    pub(crate) async fn update_node_embeddings<T: AsNodeRef>(&self, node: T) {
        if let Some(vectors) = &self.vectors {
            vectors.update_node(node).await
        }
    }

    pub(crate) async fn update_edge_embeddings<T: AsNodeRef>(&self, src: T, dst: T) {
        if let Some(vectors) = &self.vectors {
            vectors.update_edge(src, dst).await
        }
    }

    pub(crate) fn cache(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder = path.into();
        self.folder
            .get_or_try_init(|| Ok::<_, GraphError>(folder.clone()))?;
        self.graph.cache(folder)
    }

    pub(crate) fn write_updates(&self) -> Result<(), GraphError> {
        if let Some(vectors) = &self.vectors {
            vectors.write_to_path(
                &self
                    .folder
                    .get()
                    .ok_or(GraphError::CacheNotInnitialised)?
                    .get_vectors_path(),
            );
        }
        self.graph.write_updates()
    }

    pub(crate) fn read_from_folder(
        folder: &ExistingGraphFolder,
        embedding: Arc<dyn EmbeddingFunction>,
        cache: Arc<Option<EmbeddingCache>>,
    ) -> Result<Self, GraphError> {
        let graph_path = &folder.get_graph_path();
        let graph = if graph_path.is_dir() {
            if is_disk_graph_dir(folder) {
                get_disk_graph_from_path(folder)?.ok_or(GraphError::DiskGraphNotFound)?
            } else {
                Err(PathIsDirectory(graph_path.clone()))?
            }
        } else {
            MaterializedGraph::load_cached(folder.clone())?
        };

        // TODO: use EmbeddingConf here
        let vectors = VectorisedGraph::read_from_path(
            &folder.get_vectors_path(),
            graph.clone(),
            embedding,
            cache,
        );

        println!("Graph loaded = {}", folder.get_original_path_str());
        Ok(Self {
            graph: IndexedGraph::from_graph(&graph)?,
            vectors,
            folder: OnceCell::with_value(folder.clone().into()),
        })
    }
}

// TODO: instead of defining is_disk_graph_dir, if it's a dir always call to
// get_disk_graph_from_path and if there is no disk graph inside simply return
// Err(PathIsDirectory(...))!

/// Check if the directory contains files specific to disk_graph graphs
fn is_disk_graph_dir(path: &ExistingGraphFolder) -> bool {
    let files = fs::read_dir(path.get_graph_path()).unwrap();
    files.into_iter().any(|file| {
        let filename = file.unwrap().file_name().into_string().unwrap();
        filename.ends_with(".ipc")
    })
}

#[cfg(feature = "storage")]
fn get_disk_graph_from_path(
    path: &ExistingGraphFolder,
) -> Result<Option<MaterializedGraph>, GraphError> {
    let disk_graph = DiskGraphStorage::load_from_dir(&path.get_graph_path())
        .map_err(|e| GraphError::LoadFailure(e.to_string()))?;
    let graph: MaterializedGraph = disk_graph.into_graph().into(); // TODO: We currently have no way to identify disk graphs as MaterializedGraphs
    println!("Disk Graph loaded = {}", path.get_original_path().display());
    Ok(Some(graph))
}

#[cfg(not(feature = "storage"))]
fn get_disk_graph_from_path(
    _path: &ExistingGraphFolder,
) -> Result<Option<MaterializedGraph>, GraphError> {
    Ok(None) // FIXME: Return Err() instead of Ok(None) ????
}

// TODO: review why this doesnt work
impl Base for GraphWithVectors {
    type Base = MaterializedGraph;
    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph.graph
    }
}

impl Static for GraphWithVectors {}

impl InheritViewOps for GraphWithVectors {}
impl InheritMutationOps for GraphWithVectors {}

pub(crate) trait UpdateEmbeddings {
    async fn update_embeddings(&self);
}

impl UpdateEmbeddings for NodeView<GraphWithVectors> {
    async fn update_embeddings(&self) {
        self.graph.update_node_embeddings(self.name()).await
    }
}

impl UpdateEmbeddings for EdgeView<GraphWithVectors> {
    async fn update_embeddings(&self) {
        self.graph
            .update_edge_embeddings(self.src().name(), self.dst().name())
            .await
    }
}
