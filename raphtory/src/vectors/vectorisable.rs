use crate::{
    db::api::view::{internal::IntoDynamic, StaticGraphViewOps},
    vectors::{
        document_ref::DocumentRef,
        document_template::{DefaultTemplate, DocumentTemplate},
        embedding_cache::EmbeddingCache,
        entity_id::EntityId,
        vectorised_graph::VectorisedGraph,
        EmbeddingFunction, Lifespan,
    },
};
use async_trait::async_trait;
use itertools::Itertools;
use std::{collections::HashMap, path::PathBuf};

const CHUNK_SIZE: usize = 1000;

#[derive(Clone, Debug)]
struct IndexedDocumentInput {
    entity_id: EntityId,
    content: String,
    index: usize,
    life: Lifespan,
}

#[async_trait(?Send)]
pub trait Vectorisable<G: StaticGraphViewOps> {
    /// Create a VectorisedGraph from the current graph
    ///
    /// # Arguments:
    ///   * embedding - the embedding function to translate documents to embeddings
    ///   * cache - the file to be used as a cache to avoid calling the embedding function
    ///   * overwrite_cache - whether or not to overwrite the cache if there are new embeddings
    ///   * verbose - whether or not to print logs reporting the progress
    ///   
    /// # Returns:
    ///   A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        override_cache: bool,
        verbose: bool,
    ) -> VectorisedGraph<G, DefaultTemplate>;

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
    async fn vectorise_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Option<PathBuf>,
        override_cache: bool,
        template: T,
        verbose: bool,
    ) -> VectorisedGraph<G, T>;
}

#[async_trait(?Send)]
impl<G: StaticGraphViewOps + IntoDynamic> Vectorisable<G> for G {
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Option<PathBuf>,
        overwrite_cache: bool,
        verbose: bool,
    ) -> VectorisedGraph<G, DefaultTemplate> {
        self.vectorise_with_template(embedding, cache, overwrite_cache, DefaultTemplate, verbose)
            .await
    }

    async fn vectorise_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Option<PathBuf>,
        overwrite_cache: bool,
        template: T,
        verbose: bool,
    ) -> VectorisedGraph<G, T> {
        let graph_docs =
            template
                .graph(self)
                .enumerate()
                .map(move |(index, doc)| IndexedDocumentInput {
                    entity_id: EntityId::from_graph(self),
                    content: doc.content,
                    index,
                    life: doc.life,
                });
        let nodes = self.nodes().iter_owned().flat_map(|node| {
            template
                .node(&node)
                .enumerate()
                .map(move |(index, doc)| IndexedDocumentInput {
                    entity_id: EntityId::from_node(&node),
                    content: doc.content,
                    index,
                    life: doc.life,
                })
        });
        let edges = self.edges().iter().flat_map(|edge| {
            template
                .edge(&edge)
                .enumerate()
                .map(move |(index, doc)| IndexedDocumentInput {
                    entity_id: EntityId::from_edge(&edge),
                    content: doc.content,
                    index,
                    life: doc.life,
                })
        });

        let cache_storage = cache.map(EmbeddingCache::from_path);

        if verbose {
            println!("computing embeddings for graph");
        }
        let graph_ref_map =
            compute_embedding_groups(graph_docs, embedding.as_ref(), &cache_storage).await;
        let graph_refs = graph_ref_map
            .into_iter()
            .next()
            .map(|(_, graph_refs)| graph_refs)
            .unwrap_or_else(|| vec![]); // there should be only one value here, TODO: check that's true

        if verbose {
            println!("computing embeddings for nodes");
        }
        let node_refs = compute_embedding_groups(nodes, embedding.as_ref(), &cache_storage).await;

        if verbose {
            println!("computing embeddings for edges");
        }
        let edge_refs = compute_embedding_groups(edges, embedding.as_ref(), &cache_storage).await; // FIXME: re-enable

        if overwrite_cache {
            cache_storage.iter().for_each(|cache| cache.dump_to_disk());
        }

        VectorisedGraph::new(
            self.clone(),
            template.into(),
            embedding.into(),
            graph_refs.into(),
            node_refs.into(),
            edge_refs.into(),
            vec![],
        )
    }
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
    for chunk in documents.chunks(CHUNK_SIZE).into_iter() {
        let doc_refs = compute_chunk(chunk, embedding, cache).await;
        for doc in doc_refs {
            match embedding_groups.get_mut(&doc.entity_id) {
                Some(group) => group.push(doc),
                None => {
                    embedding_groups.insert(doc.entity_id.clone(), vec![doc]);
                }
            }
        }
    }
    embedding_groups
}

async fn compute_chunk<I>(
    documents: I,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
) -> Vec<DocumentRef>
where
    I: Iterator<Item = IndexedDocumentInput>,
{
    let mut misses = vec![];
    let mut embedded = vec![];
    match cache {
        Some(cache) => {
            for doc in documents {
                let embedding = cache.get_embedding(&doc.content);
                match embedding {
                    Some(embedding) => embedded.push(DocumentRef::new(
                        doc.entity_id,
                        doc.index,
                        embedding,
                        doc.life,
                    )),
                    None => misses.push(doc),
                }
            }
        }
        None => misses = documents.collect_vec(),
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
            doc.entity_id,
            doc.index,
            embedding,
            doc.life,
        ));
    }

    embedded
}
