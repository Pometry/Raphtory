use crate::{
    db::api::view::internal::IntoDynamic,
    prelude::GraphViewOps,
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
pub trait Vectorisable<G: GraphViewOps> {
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        verbose: bool,
    ) -> VectorisedGraph<G, DefaultTemplate>;

    async fn vectorise_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        template: T,
        verbose: bool,
    ) -> VectorisedGraph<G, T>;
}

#[async_trait(?Send)]
impl<G: GraphViewOps + IntoDynamic> Vectorisable<G> for G {
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        verbose: bool,
    ) -> VectorisedGraph<G, DefaultTemplate> {
        self.vectorise_with_template(embedding, cache_file, DefaultTemplate, verbose)
            .await
    }

    async fn vectorise_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        template: T,
        verbose: bool,
    ) -> VectorisedGraph<G, T> {
        let nodes = self.vertices().iter().flat_map(|vertex| {
            template
                .node(&vertex)
                .enumerate()
                .map(move |(index, doc)| IndexedDocumentInput {
                    entity_id: EntityId::from_node(&vertex),
                    content: doc.content,
                    index,
                    life: doc.life,
                })
        });
        let edges = self.edges().flat_map(|edge| {
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

        let cache = cache_file.map(EmbeddingCache::from_path);

        if verbose {
            println!("compute embeddings for nodes");
        }
        let node_refs = compute_embedding_groups(nodes, embedding.as_ref(), &cache).await;

        if verbose {
            println!("compute embeddings for edges");
        }
        let edge_refs = compute_embedding_groups(edges, embedding.as_ref(), &cache).await; // FIXME: re-enable

        cache.iter().for_each(|cache| cache.dump_to_disk());

        VectorisedGraph::new(
            self.clone(),
            template.into(),
            embedding.into(),
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
                    embedding_groups.insert(doc.entity_id, vec![doc]);
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
