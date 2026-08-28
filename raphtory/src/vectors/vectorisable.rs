use super::{
    entity_db::{EdgeDb, NodeDb},
    storage::{collection_names, db_path, meta_path, VectorMeta},
};
use crate::{
    db::api::view::{internal::IntoDynamic, StaticGraphViewOps},
    errors::{GraphError, GraphResult},
    prelude::GraphViewOps,
    vectors::{
        cache::CachedEmbeddingModel,
        embeddings::compute_embeddings,
        entity_db::EntityDb,
        template::DocumentTemplate,
        vector_collection::{
            lancedb::LanceDb, CollectionPath, LanceDbCollection, VectorCollection,
            VectorCollectionFactory,
        },
        vectorised_graph::VectorisedGraph,
    },
};
use async_trait::async_trait;
use roaring::RoaringTreemap;
use std::{path::Path, sync::Arc};
use tracing::info;

#[async_trait]
pub trait Vectorisable<G: StaticGraphViewOps> {
    /// Embed every document in the graph, replacing whatever was indexed before.
    ///
    /// Required after a change of template or embedding model, since every stored vector is then
    /// stale. The new documents are written to a fresh generation of collections and the index only
    /// switches to it once everything is in place, so the previous index keeps serving until then
    /// and a rebuild that never finishes leaves it untouched.
    ///
    /// # Arguments:
    ///   * model - the embedding function to translate documents to embeddings
    ///   * template - the template to use to translate entities into documents
    ///   * path - where to persist the index, or None for a temporary one
    ///   * verbose - whether or not to print logs reporting the progress
    async fn vectorise(
        &self,
        model: CachedEmbeddingModel,
        template: DocumentTemplate,
        path: Option<&Path>,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>>;

    /// Embed only the entities that are not in the index yet, leaving indexed rows untouched.
    ///
    /// Costs one scan of the stored ids plus the documents that are actually missing, so it is cheap
    /// enough to run routinely, and it is never destructive. Errors if there is no index yet, or if
    /// the stored template or model differs from the one given — the result would otherwise mix
    /// vectors from two different models.
    async fn vectorise_missing(
        &self,
        model: CachedEmbeddingModel,
        template: DocumentTemplate,
        path: &Path,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>>;
}

#[async_trait]
impl<G: StaticGraphViewOps + IntoDynamic + Send> Vectorisable<G> for G {
    async fn vectorise(
        &self,
        model: CachedEmbeddingModel,
        template: DocumentTemplate,
        path: Option<&Path>,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>> {
        let factory = LanceDb;
        let dim = model.dim().ok_or_else(|| GraphError::UnresolvedModel)?;
        let db_path: CollectionPath = match path {
            Some(path) => Arc::new(db_path(path)),
            None => Arc::new(tempfile::tempdir()?),
        };

        // a generation of its own, so the one in use is untouched until the meta switch at the end
        let generation = match path {
            Some(path) => match VectorMeta::read_from_path(&meta_path(path)).await {
                Ok(meta) => meta.generation + 1,
                Err(_) => 0,
            },
            None => 0,
        };
        let (node_table, edge_table) = collection_names(generation);
        let node_db = NodeDb(
            factory
                .new_collection(db_path.clone(), &node_table, dim)
                .await?,
        );
        let edge_db = EdgeDb(factory.new_collection(db_path, &edge_table, dim).await?);

        self.index_entities(
            model,
            template,
            path,
            generation,
            node_db,
            edge_db,
            &RoaringTreemap::new(),
            &RoaringTreemap::new(),
            verbose,
        )
        .await
    }

    async fn vectorise_missing(
        &self,
        model: CachedEmbeddingModel,
        template: DocumentTemplate,
        path: &Path,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>> {
        let factory = LanceDb;
        let dim = model.dim().ok_or_else(|| GraphError::UnresolvedModel)?;
        let meta = VectorMeta::read_from_path(&meta_path(path)).await?;
        if meta.template != template || meta.model != model.model {
            return Err(GraphError::VectorTemplateChanged);
        }

        let db_path: CollectionPath = Arc::new(db_path(path));
        let (node_table, edge_table) = collection_names(meta.generation);
        let node_db = NodeDb(factory.from_path(db_path.clone(), &node_table, dim).await?);
        let edge_db = EdgeDb(factory.from_path(db_path, &edge_table, dim).await?);
        let indexed_nodes = node_db.existing_ids().await?;
        let indexed_edges = edge_db.existing_ids().await?;

        self.index_entities(
            model,
            template,
            Some(path),
            meta.generation,
            node_db,
            edge_db,
            &indexed_nodes,
            &indexed_edges,
            verbose,
        )
        .await
    }
}

/// Shared body of the two vectorise operations: embed every entity that is not skipped into the
/// given collections, then publish the generation by writing the meta.
trait IndexEntities<G: StaticGraphViewOps> {
    #[allow(clippy::too_many_arguments)]
    async fn index_entities(
        &self,
        model: CachedEmbeddingModel,
        template: DocumentTemplate,
        path: Option<&Path>,
        generation: u64,
        node_db: NodeDb<LanceDbCollection>,
        edge_db: EdgeDb<LanceDbCollection>,
        skip_nodes: &RoaringTreemap,
        skip_edges: &RoaringTreemap,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>>;
}

impl<G: StaticGraphViewOps + IntoDynamic + Send> IndexEntities<G> for G {
    async fn index_entities(
        &self,
        model: CachedEmbeddingModel,
        template: DocumentTemplate,
        path: Option<&Path>,
        generation: u64,
        node_db: NodeDb<LanceDbCollection>,
        edge_db: EdgeDb<LanceDbCollection>,
        skip_nodes: &RoaringTreemap,
        skip_edges: &RoaringTreemap,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>> {
        if verbose {
            info!("computing embeddings for nodes");
        }
        // skipping happens before the template is rendered, so a call with nothing to do costs one
        // scan of the stored ids and no document building
        let nodes = self.nodes();
        let node_docs = nodes
            .iter()
            .map(|node| (node.node.0 as u64, node))
            .filter(|(id, _)| !skip_nodes.contains(*id))
            .filter_map(|(id, node)| template.node(node).map(|doc| (id, doc)));
        node_db
            .insert_vector_stream(compute_embeddings(node_docs, &model))
            .await?;
        node_db.create_or_update_index().await?;

        if verbose {
            info!("computing embeddings for edges");
        }
        let edges = self.edges();
        let edge_docs = edges
            .iter()
            .map(|edge| (edge.edge.pid().0 as u64, edge))
            .filter(|(id, _)| !skip_edges.contains(*id))
            .filter_map(|(id, edge)| template.edge(edge).map(|doc| (id, doc)));
        edge_db
            .insert_vector_stream(compute_embeddings(edge_docs, &model))
            .await?;
        edge_db.create_or_update_index().await?;

        // last: until this lands, readers and reloads still use the previous generation. Retired
        // generations stay on disk for the admin cleanup API to remove.
        if let Some(path) = path {
            let meta = VectorMeta {
                template: template.clone(),
                model: model.model.clone(),
                generation,
            };
            meta.write_to_path(path)?;
        }

        Ok(VectorisedGraph {
            source_graph: self.clone(),
            template,
            model,
            node_db,
            edge_db,
        })
    }
}
