use super::{
    cache::VectorCache,
    entity_db::{EdgeDb, NodeDb},
    storage::{db_path, VectorMeta},
};
use crate::{
    db::api::view::{internal::IntoDynamic, StaticGraphViewOps},
    errors::GraphResult,
    prelude::GraphViewOps,
    vectors::{
        embeddings::compute_embeddings,
        entity_db::EntityDb,
        template::DocumentTemplate,
        vector_collection::{lancedb::LanceDb, VectorCollection, VectorCollectionFactory},
        vectorised_graph::VectorisedGraph,
    },
};
use async_trait::async_trait;
use std::{ops::Deref, path::Path};
use tracing::info;

#[async_trait]
pub trait Vectorisable<G: StaticGraphViewOps> {
    /// Create a VectorisedGraph from the current graph
    ///
    /// # Arguments:
    ///   * embedding - the embedding function to translate documents to embeddings
    ///   * cache - the file to be used as a cache to avoid calling the embedding function
    ///   * overwrite_cache - whether or not to overwrite the cache if there are new embeddings
    ///   * template - the template to use to translate entities into documents
    ///   * verbose - whether or not to print logs reporting the progress
    ///
    /// # Returns:
    ///   A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    async fn vectorise(
        &self,
        cache: VectorCache,
        template: DocumentTemplate,
        path: Option<&Path>,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>>;
}

#[async_trait]
impl<G: StaticGraphViewOps + IntoDynamic + Send> Vectorisable<G> for G {
    async fn vectorise(
        &self,
        cache: VectorCache,
        template: DocumentTemplate,
        path: Option<&Path>,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>> {
        let db_path = path
            .map(|path| Ok::<Box<dyn AsRef<Path> + Send>, std::io::Error>(Box::new(db_path(path))))
            .unwrap_or_else(|| Ok(Box::new(tempfile::tempdir()?)))?;
        let db_path_ref = db_path.as_ref().as_ref();
        let factory = LanceDb;
        let dim = cache.get_vector_sample().len();
        if verbose {
            info!("computing embeddings for nodes");
        }
        let nodes = self.nodes();
        let node_docs = nodes
            .iter()
            .filter_map(|node| template.node(node).map(|doc| (node.node.0 as u64, doc)));
        let node_vectors = compute_embeddings(node_docs, &cache);
        let node_db = NodeDb(factory.new_collection(db_path_ref, "nodes", dim).await?); // FIXME: dimension!!!!!!!!!!!!!!!!!!!!
        node_db.insert_vector_stream(node_vectors).await.unwrap();
        node_db.create_index().await;

        if verbose {
            info!("computing embeddings for edges");
        }
        let edges = self.edges();
        let edge_docs = edges.iter().filter_map(|edge| {
            template
                .edge(edge)
                .map(|doc| (edge.edge.pid().0 as u64, doc))
        });
        let edge_vectors = compute_embeddings(edge_docs, &cache);
        let edge_db = EdgeDb(factory.new_collection(db_path_ref, "edges", dim).await?); // FIXME: dimension!!!!!!!!!!!!!!!!!!!!
        edge_db.insert_vector_stream(edge_vectors).await.unwrap();
        edge_db.create_index().await;

        if let Some(path) = path {
            let meta = VectorMeta {
                template: template.clone(),
                sample: cache.get_vector_sample(),
            };
            meta.write_to_path(path)?;
        }

        // FIXME: here tempdir will be dropped and so the vector db will be destroyed!!!!!!!!!!!!!!

        Ok(VectorisedGraph {
            source_graph: self.clone(),
            template,
            cache,
            node_db,
            edge_db,
        })
    }
}
