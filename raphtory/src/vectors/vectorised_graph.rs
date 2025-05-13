use crate::{
    // core::entities::nodes::node_ref::AsNodeRef,
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphResult},
    db::{
        api::{
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
    vectors::{
        embedding_cache::EmbeddingCache, similarity_search_utils::find_top_k,
        template::DocumentTemplate, Embedding, EmbeddingFunction,
    },
};
use arroy::{distances::Cosine, Database as ArroyDatabase, Reader, Writer};
use rand::{rngs::StdRng, SeedableRng};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, OnceLock},
};
use tempfile::TempDir;

use super::{
    similarity_search_utils::apply_window, vector_selection::VectorSelection,
    vectorisable::compute_embeddings,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EntityRef {
    Node(usize), // TODO: store this as a u32
    Edge(usize),
}

impl EntityRef {
    // TODO: there are some places where I should be using this and Im not
    pub(crate) fn as_node_view<G: StaticGraphViewOps>(&self, view: &G) -> Option<NodeView<G>> {
        let node = self.as_node(view)?;
        view.node(node)
    }

    // TODO: there are some places where I should be using this and Im not
    pub(crate) fn as_edge_view<G: StaticGraphViewOps>(&self, view: &G) -> Option<EdgeView<G>> {
        let (src, dst) = self.as_edge(view)?;
        view.edge(src, dst)
    }

    pub(crate) fn as_node<G: StaticGraphViewOps>(&self, view: &G) -> Option<GID> {
        if let EntityRef::Node(id) = self {
            Some(view.node_id(raphtory_api::core::entities::VID(*id)))
        } else {
            None
        }
    }

    pub(crate) fn as_edge<G: StaticGraphViewOps>(&self, view: &G) -> Option<(GID, GID)> {
        if let EntityRef::Edge(id) = self {
            let edge = view.core_edge((*id).into());
            let src = view.node_id(edge.src());
            let dst = view.node_id(edge.dst());
            Some((src, dst))
        } else {
            None
        }
    }

    fn as_usize(&self) -> usize {
        match self {
            EntityRef::Node(id) => *id,
            EntityRef::Edge(id) => *id,
        }
    }

    fn as_u32(&self) -> u32 {
        match self {
            EntityRef::Node(id) => *id as u32,
            EntityRef::Edge(id) => *id as u32,
        }
    }
}

#[derive(Clone)]
pub(super) struct NodeDb(pub(super) VectorDb);

impl VectorSearch for NodeDb {
    fn get_db(&self) -> &VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Node(id as usize)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        view.has_node(entity.as_node(view).unwrap())
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = usize> {
        view.nodes().into_iter().map(|node| node.node.index())
    }
}

#[derive(Clone)]
pub(super) struct EdgeDb(pub(super) VectorDb);

impl VectorSearch for EdgeDb {
    fn get_db(&self) -> &VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Edge(id as usize)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        let (src, dst) = entity.as_edge(view).unwrap(); // TODO: remove this?
        view.has_edge(src, dst) // FIXME: there should be a quicker way!!!!!!!!!!!!!!!!!!!
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = usize> {
        view.edges().into_iter().map(|edge| edge.edge.pid().0)
    }
}

// FIXME: remove unwraps in here
// TODO: rename this to GraphVectorSearch
// TODO: merge this and VectorDb !!!!!!!!!!!!!!!!!???????
pub(super) trait VectorSearch {
    fn get_db(&self) -> &VectorDb;
    fn into_entity_ref(id: u32) -> EntityRef;
    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool;
    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = usize> + 'static;

    fn top_k<G: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        k: usize,
        view: Option<G>,
        filter: Option<HashSet<EntityRef>>,
    ) -> impl Iterator<Item = (EntityRef, f32)> {
        let candidates: Option<Box<dyn Iterator<Item = u32>>> = match (view, filter) {
            (None, None) => None,
            (view, Some(filter)) => Some(Box::new(
                filter
                    .into_iter()
                    .filter(move |entity| {
                        view.as_ref()
                            .map_or(true, |view| Self::view_has_entity(entity, view))
                    })
                    .map(|entity| entity.as_u32()),
            )),
            (view, None) => Some(Box::new(
                Self::all_valid_entities(view.unwrap()).map(|id| id as u32), // FIXME: unwrap here doesnt make sense
            )),
        };
        self.top_k_with_candidates(query, k, candidates)
    }

    fn top_k_with_candidates(
        &self,
        query: &Embedding,
        k: usize,
        candidates: Option<impl Iterator<Item = u32>>,
    ) -> impl Iterator<Item = (EntityRef, f32)> {
        let db = self.get_db();
        let rtxn = db.env.read_txn().unwrap();
        // FIXME: if the db has no edges, I get a MissingMetadata here. Handle that properly,
        // Maybe the edge db should not exist at all if there are no edge embeddings
        // because there might be some errors other than MissingMetadata
        let vectors = if let Ok(reader) = Reader::open(&rtxn, 0, db.vectors) {
            let mut query_builder = reader.nns(k);
            let candidates = candidates
                .map(|filter| roaring::RoaringBitmap::from_iter(filter.map(|id| id as u32)));
            let query_builder = if let Some(filter) = &candidates {
                query_builder.candidates(filter)
            } else {
                &query_builder
            };
            query_builder.by_vector(&rtxn, query.as_ref()).unwrap()
        } else {
            vec![]
        };
        vectors
            .into_iter()
            // for arroy, distance = (1.0 - score) / 2.0, where score is cosine: [-1, 1]
            .map(|(id, distance)| (Self::into_entity_ref(id), 1.0 - 2.0 * distance))
    }
}

#[derive(Clone)]
pub(crate) struct VectorDb {
    // FIXME: save index value in here !!!!!!!!!!!!!!!!!!!
    pub(crate) vectors: ArroyDatabase<Cosine>, // TODO: review is this safe to clone? does it point to the same thing?
    pub(crate) env: heed::Env,
    pub(crate) _tempdir: Option<Arc<TempDir>>, // do I really need, is the file open not enough
    pub(crate) dimensions: OnceLock<usize>,
}

// TODO: merge this with the above
impl VectorDb {
    pub(super) fn insert_vector(&self, id: usize, embedding: &Embedding) {
        // FIXME: remove unwraps
        let mut wtxn = self.env.write_txn().unwrap();

        let dimensions = self.dimensions.get_or_init(|| embedding.len());
        let writer = Writer::<Cosine>::new(self.vectors, 0, *dimensions);
        writer
            .add_item(&mut wtxn, id as u32, embedding.as_ref())
            .unwrap();

        let mut rng = StdRng::from_entropy();
        writer.builder(&mut rng).build(&mut wtxn).unwrap();

        wtxn.commit().unwrap();
    }

    pub(super) fn get_id(&self, id: usize) -> Embedding {
        let rtxn = self.env.read_txn().unwrap();
        let reader = Reader::open(&rtxn, 0, self.vectors).unwrap();
        let vector = reader.item_vector(&rtxn, id as u32).unwrap().unwrap();
        vector.into()
    }
}

#[derive(Clone)]
pub struct VectorisedGraph<G: StaticGraphViewOps> {
    pub(crate) source_graph: G,
    pub(crate) template: DocumentTemplate,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    pub(crate) cache_storage: Arc<Option<EmbeddingCache>>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) node_db: NodeDb,
    pub(crate) edge_db: EdgeDb,
}

// This has to be here so it is shared between python and graphql
pub type DynamicVectorisedGraph = VectorisedGraph<DynamicGraph>;

impl<G: StaticGraphViewOps + IntoDynamic> VectorisedGraph<G> {
    pub fn into_dynamic(self) -> VectorisedGraph<DynamicGraph> {
        VectorisedGraph {
            source_graph: self.source_graph.clone().into_dynamic(),
            template: self.template,
            embedding: self.embedding,
            cache_storage: self.cache_storage,
            node_db: self.node_db,
            edge_db: self.edge_db,
        }
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub async fn update_node<T: AsNodeRef>(&self, node: T) -> GraphResult<()> {
        if let Some(node) = self.source_graph.node(node) {
            let id = node.node.index();
            if let Some(doc) = self.template.node(node) {
                let vector = self.compute_embedding(doc).await?;
                self.node_db.0.insert_vector(id, &vector);
            }
        }
        Ok(())
    }

    pub async fn update_edge<T: AsNodeRef>(&self, src: T, dst: T) -> GraphResult<()> {
        if let Some(edge) = self.source_graph.edge(src, dst) {
            let id = edge.edge.pid().0;
            if let Some(doc) = self.template.edge(edge) {
                let vector = self.compute_embedding(doc).await?;
                self.edge_db.0.insert_vector(id, &vector);
            }
        }
        Ok(())
    }

    async fn compute_embedding(&self, doc: String) -> GraphResult<Embedding> {
        let mut vectors = compute_embeddings(
            std::iter::once((0, doc)),
            &self.embedding,
            &self.cache_storage,
        )
        .await?;
        Ok(vectors.remove(0).1) // FIXME: unwrap
    }

    /// Save the embeddings present in this graph to `file` so they can be further used in a call to `vectorise`
    pub fn save_embeddings(&self, file: PathBuf) {
        // let cache = EmbeddingCache::new(file);
        // let node_documents = self.node_documents.read();
        // let edge_documents = self.edge_documents.read();
        // chain!(node_documents.iter(), edge_documents.iter()).for_each(|(_, group)| {
        //     group.iter().for_each(|doc| {
        //         let original = doc.regenerate(&self.source_graph, &self.template);
        //         cache.upsert_embedding(&original.content, doc.embedding.clone());
        //     })
        // });
        // cache.dump_to_disk();
    }

    /// Return an empty selection of documents
    pub fn empty_selection(&self) -> VectorSelection<G> {
        VectorSelection::empty(self.clone())
    }

    /// Search the top scoring entities according to `query` with no more than `limit` entities
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of entities to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn entities_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        let view = apply_window(&self.source_graph, window);
        dbg!();
        let nodes = self.node_db.top_k(query, limit, view.clone(), None); // TODO: avoid this clone
        dbg!();
        let edges = self.edge_db.top_k(query, limit, view, None);
        let docs = find_top_k(nodes.chain(edges), limit).collect();
        VectorSelection::new(self.clone(), docs)
    }

    /// Search the top scoring nodes according to `query` with no more than `limit` nodes
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of nodes to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn nodes_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        let view = apply_window(&self.source_graph, window);
        let docs = self.node_db.top_k(query, limit, view, None);
        VectorSelection::new(self.clone(), docs.collect())
    }

    /// Search the top scoring edges according to `query` with no more than `limit` edges
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of edges to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn edges_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        let view = apply_window(&self.source_graph, window);
        let docs = self.edge_db.top_k(query, limit, view, None);
        VectorSelection::new(self.clone(), docs.collect())
    }
}
