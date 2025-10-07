use crate::paths::ExistingGraphFolder;
use once_cell::sync::OnceCell;
use raphtory::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            storage::storage::Storage,
            view::{
                internal::{
                    InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps,
                    InternalStorageOps, Static,
                },
                Base, InheritViewOps, MaterializedGraph,
            },
        },
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::PersistentGraph},
    },
    errors::{GraphError, GraphResult},
    prelude::{EdgeViewOps, Graph, IndexMutationOps, NodeViewOps, StableDecode},
    serialise::GraphFolder,
    vectors::{cache::VectorCache, vectorised_graph::VectorisedGraph},
};
use raphtory_api::GraphType;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps, mutation::InheritMutationOps,
};
use std::path::PathBuf;
use tracing::info;

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

    pub(crate) fn read_from_folder(
        folder: &ExistingGraphFolder,
        cache: Option<VectorCache>,
        create_index: bool,
    ) -> Result<Self, GraphError> {
        let graph = {
            // Create an empty graph just to test disk_storage_enabled
            let test = Graph::new();

            if test.disk_storage_enabled() {
                let metadata = folder.read_metadata()?;

                let graph = match metadata.graph_type {
                    GraphType::EventGraph => {
                        let graph = Graph::load_from_path(folder.get_graph_path());
                        MaterializedGraph::EventGraph(graph)
                    }
                    GraphType::PersistentGraph => {
                        let graph = PersistentGraph::load_from_path(folder.get_graph_path());
                        MaterializedGraph::PersistentGraph(graph)
                    }
                };

                #[cfg(feature = "search")]
                graph.load_index(&folder)?;

                graph
            } else {
                let path_for_decoded_graph = None;

                MaterializedGraph::decode(folder.clone(), path_for_decoded_graph)?
            }
        };

        let vectors = cache.and_then(|cache| {
            VectorisedGraph::read_from_path(&folder.get_vectors_path(), graph.clone(), cache).ok()
        });

        info!("Graph loaded = {}", folder.get_original_path_str());

        if create_index {
            graph.create_index()?;
        }

        Ok(Self {
            graph: graph.clone(),
            vectors,
            folder: OnceCell::with_value(folder.clone().into()),
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
