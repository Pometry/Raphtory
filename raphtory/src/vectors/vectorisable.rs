use crate::{
    db::{
        api::view::{internal::IntoDynamic, StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    vectors::{
        document_ref::DocumentRef, embedding_cache::EmbeddingCache, entity_id::EntityId,
        template::DocumentTemplate, vectorised_graph::VectorisedGraph, EmbeddingFunction, Lifespan,
    },
};
use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tracing::info;

const CHUNK_SIZE: usize = 1000;

#[derive(Clone, Debug)]
struct IndexedDocumentInput {
    entity_id: EntityId,
    content: String,
    index: usize,
    life: Lifespan,
}

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
        embedding: Box<dyn EmbeddingFunction>,
        cache: Option<PathBuf>,
        overwrite_cache: bool,
        template: DocumentTemplate,
        verbose: bool,
    ) -> VectorisedGraph<G>;

    // TODO: docs
    async fn vectorise_with_cache(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Arc<Option<EmbeddingCache>>,
        overwrite_cache: bool,
        template: DocumentTemplate,
        verbose: bool,
    ) -> VectorisedGraph<G>;
}

#[async_trait] // TODO: review id this ?Send can be removed once we stop using the EmbeddingFunction trait
impl<G: StaticGraphViewOps + IntoDynamic + Send> Vectorisable<G> for G {
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Option<PathBuf>,
        overwrite_cache: bool,
        template: DocumentTemplate,
        verbose: bool,
    ) -> VectorisedGraph<G> {
        let cache: Arc<_> = cache.map(EmbeddingCache::from_path).into();
        Self::vectorise_with_cache(&self, embedding, cache, overwrite_cache, template, verbose)
            .await
    }

    async fn vectorise_with_cache(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Arc<Option<EmbeddingCache>>,
        overwrite_cache: bool,
        template: DocumentTemplate,
        verbose: bool,
    ) -> VectorisedGraph<G> {
        let graph_docs = indexed_docs_for_graph(self, &template);

        let node_iter = self.nodes().iter_owned();
        let nodes = node_iter
            .flat_map(|node| indexed_docs_for_node(node, &template))
            .collect_vec();

        let edge_iter = self.edges().iter();
        let edges = edge_iter
            .flat_map(|edge| indexed_docs_for_edge(edge, &template))
            .collect_vec();

        if verbose {
            info!("computing embeddings for graph");
        }
        let graph_refs =
            compute_entity_embeddings(graph_docs.into_iter(), embedding.as_ref(), &cache).await;

        if verbose {
            info!("computing embeddings for nodes");
        }
        let node_refs =
            compute_embedding_groups(nodes.into_iter(), embedding.as_ref(), &cache).await;

        if verbose {
            info!("computing embeddings for edges");
        }
        let edge_refs =
            compute_embedding_groups(edges.into_iter(), embedding.as_ref(), &cache).await; // FIXME: re-enable

        if overwrite_cache {
            cache.iter().for_each(|cache| cache.dump_to_disk());
        }

        VectorisedGraph::new(
            self.clone(),
            template,
            embedding.into(),
            cache.into(),
            graph_refs.into(),
            RwLock::new(node_refs).into(),
            RwLock::new(edge_refs).into(),
        )
    }
}

pub(crate) async fn vectorise_node<G: StaticGraphViewOps>(
    node: NodeView<G>,
    template: &DocumentTemplate,
    embedding: &Arc<dyn EmbeddingFunction>,
    cache_storage: &Option<EmbeddingCache>,
) -> Vec<DocumentRef> {
    let docs = indexed_docs_for_node(node, template);
    compute_entity_embeddings(docs, embedding.as_ref(), &cache_storage).await
}

pub(crate) async fn vectorise_edge<G: StaticGraphViewOps>(
    edge: EdgeView<G>,
    template: &DocumentTemplate,
    embedding: &Arc<dyn EmbeddingFunction>,
    cache_storage: &Option<EmbeddingCache>,
) -> Vec<DocumentRef> {
    let docs = indexed_docs_for_edge(edge, template);
    compute_entity_embeddings(docs, embedding.as_ref(), &cache_storage).await
}

fn indexed_docs_for_graph<'a, G: StaticGraphViewOps>(
    graph: &'a G,
    template: &DocumentTemplate,
) -> Vec<IndexedDocumentInput> {
    template
        .graph(graph)
        .enumerate()
        .map(move |(index, doc)| IndexedDocumentInput {
            entity_id: EntityId::from_graph(graph),
            content: doc.content,
            index,
            life: doc.life,
        })
        .collect()
}

fn indexed_docs_for_node<G: StaticGraphViewOps>(
    node: NodeView<G>,
    template: &DocumentTemplate,
) -> impl Iterator<Item = IndexedDocumentInput> + Send {
    template
        .node(&node)
        .enumerate()
        .map(move |(index, doc)| IndexedDocumentInput {
            entity_id: EntityId::from_node(&node),
            content: doc.content,
            index,
            life: doc.life,
        })
}

fn indexed_docs_for_edge<G: StaticGraphViewOps>(
    edge: EdgeView<G>,
    template: &DocumentTemplate,
) -> impl Iterator<Item = IndexedDocumentInput> + Send {
    template
        .edge(&edge)
        .enumerate()
        .map(move |(index, doc)| IndexedDocumentInput {
            entity_id: EntityId::from_edge(&edge),
            content: doc.content,
            index,
            life: doc.life,
        })
}

async fn compute_entity_embeddings<I>(
    documents: I,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
) -> Vec<DocumentRef>
where
    I: Iterator<Item = IndexedDocumentInput> + Send,
{
    // let documents = documents.collect_vec().into_iter();
    let map = compute_embedding_groups(documents, embedding, cache).await;
    // vec![]
    map.into_iter()
        .next()
        .map(|(_, refs)| refs)
        .unwrap_or_else(|| vec![]) // there should be only one value here, TODO: check that's true
}

async fn compute_embedding_groups<I>(
    documents: I,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
) -> HashMap<EntityId, Vec<DocumentRef>>
where
    I: Iterator<Item = IndexedDocumentInput>,
{
    let mut embedding_groups: HashMap<EntityId, Vec<DocumentRef>> = HashMap::new();
    let mut buffer = Vec::with_capacity(CHUNK_SIZE);

    for document in documents {
        buffer.push(document);
        if buffer.len() >= CHUNK_SIZE {
            let doc_refs = compute_chunk(&buffer, embedding, cache).await;
            for doc in doc_refs {
                match embedding_groups.get_mut(&doc.entity_id) {
                    Some(group) => group.push(doc),
                    None => {
                        embedding_groups.insert(doc.entity_id.clone(), vec![doc]);
                    }
                }
            }
            buffer.clear();
        }
    }

    // FIXME: repeated code above
    let doc_refs = compute_chunk(&buffer, embedding, cache).await;
    for doc in doc_refs {
        match embedding_groups.get_mut(&doc.entity_id) {
            Some(group) => group.push(doc),
            None => {
                embedding_groups.insert(doc.entity_id.clone(), vec![doc]);
            }
        }
    }

    embedding_groups
}

async fn compute_chunk(
    documents: &Vec<IndexedDocumentInput>,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
) -> Vec<DocumentRef> {
    let mut misses = vec![];
    let mut embedded = vec![];
    match cache {
        Some(cache) => {
            for doc in documents {
                let embedding = cache.get_embedding(&doc.content);
                match embedding {
                    Some(embedding) => embedded.push(DocumentRef::new(
                        doc.entity_id.clone(),
                        doc.index,
                        embedding,
                        doc.life,
                    )),
                    None => misses.push(doc),
                }
            }
        }
        None => misses = documents.iter().collect(),
    };

    let texts = misses.iter().map(|doc| doc.content.clone()).collect_vec();
    let embeddings = if texts.is_empty() {
        vec![]
    } else {
        embedding.call(texts).await
    };

    for (doc, embedding) in misses.into_iter().zip(embeddings) {
        if let Some(cache) = cache {
            cache.upsert_embedding(&doc.content, embedding.clone())
        };
        embedded.push(DocumentRef::new(
            doc.entity_id.clone(),
            doc.index,
            embedding,
            doc.life,
        ));
    }

    embedded
}
