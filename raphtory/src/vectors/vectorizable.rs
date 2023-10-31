use crate::{
    core::entities::LayerIds,
    db::api::view::internal::IntoDynamic,
    prelude::GraphViewOps,
    vectors::{
        document_ref::DocumentRef,
        document_template::{DefaultTemplate, DocumentTemplate},
        embedding_cache::EmbeddingCache,
        entity_id::EntityId,
        vectorized_graph::VectorizedGraph,
        DocumentInput, EmbeddingFunction,
    },
};
use async_trait::async_trait;
use futures_util::future::join_all;
use itertools::Itertools;
use std::{
    collections::{hash_map::DefaultHasher, HashMap, VecDeque},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

const CHUNK_SIZE: usize = 1000;

#[derive(Clone)]
struct DocumentGroup {
    id: EntityId,
    documents: Vec<DocumentInput>,
}

impl DocumentGroup {
    fn new(id: EntityId, documents: Vec<DocumentInput>) -> Self {
        Self { id, documents }
    }
    fn hash(self) -> HashedDocumentGroup {
        let mut hasher = DefaultHasher::new();
        for doc in &self.documents {
            doc.content.hash(&mut hasher);
        }
        HashedDocumentGroup {
            id: self.id,
            hash: hasher.finish(),
            documents: self.documents,
        }
    }
}

#[derive(Clone)]
struct HashedDocumentGroup {
    id: EntityId,
    hash: u64,
    documents: Vec<DocumentInput>,
}

struct EmbeddedDocumentGroup {
    id: EntityId,
    hash: u64,
    documents: Vec<DocumentRef>,
}

#[async_trait]
pub trait Vectorizable<G: GraphViewOps> {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
    ) -> VectorizedGraph<G, DefaultTemplate>;

    async fn vectorize_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        template: T,
    ) -> VectorizedGraph<G, T>;
}

#[async_trait]
impl<G: GraphViewOps + IntoDynamic> Vectorizable<G> for G {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>, // TODO: make this optional maybe
    ) -> VectorizedGraph<G, DefaultTemplate> {
        self.vectorize_with_template(embedding, cache_file, DefaultTemplate)
            .await
    }

    async fn vectorize_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_file: Option<PathBuf>,
        template: T,
    ) -> VectorizedGraph<G, T> {
        let nodes = self.vertices().iter().map(|vertex| {
            let documents = template.node(&vertex).collect_vec();
            DocumentGroup::new(vertex.into(), documents)
        });
        let edges = self.edges().map(|edge| {
            let documents = template.edge(&edge).collect_vec();
            DocumentGroup::new(edge.into(), documents)
        });

        let cache = cache_file.map(EmbeddingCache::from_path);
        let node_refs = attach_embeddings(nodes, &embedding, &cache).await;
        let edge_refs = attach_embeddings(edges, &embedding, &cache).await;
        cache.map(|cache| cache.dump_to_disk());

        VectorizedGraph::new(self.clone(), template, embedding, node_refs, edge_refs)
    }
}

async fn attach_embeddings<I>(
    doc_groups: I,
    embedding: &Box<dyn EmbeddingFunction>,
    cache: &Option<EmbeddingCache>,
) -> HashMap<EntityId, Vec<DocumentRef>>
where
    I: Iterator<Item = DocumentGroup>,
{
    let hashed_doc_groups = doc_groups.map(|entity_documents| (entity_documents.hash()));
    let mut document_groups = HashMap::new();
    let mut misses = vec![];

    if let Some(cache) = cache {
        for group in hashed_doc_groups {
            match retrieve_embeddings_from_cache(&group, cache) {
                Some(document_refs) => {
                    document_groups.insert(group.id, document_refs);
                }
                None => misses.push(group),
            }
        }
    } else {
        misses = hashed_doc_groups.collect_vec();
    }

    let embedding_tasks = misses
        .chunks(CHUNK_SIZE)
        .map(|chunk| compute_embeddings(chunk.to_vec(), embedding));
    let new_computed_groups = join_all(embedding_tasks).await.into_iter().flatten();
    for group in new_computed_groups {
        if let Some(cache) = cache {
            update_cache(cache, &group);
            // might be better if we do this as soon as each task finishes instead of
            // synchronously at the end, but that might complicate having an async impl
        }
        document_groups.insert(group.id, group.documents);
    }

    document_groups
}

async fn compute_embeddings(
    doc_groups: Vec<HashedDocumentGroup>,
    embedding: &Box<dyn EmbeddingFunction>,
) -> Vec<EmbeddedDocumentGroup> {
    let texts = doc_groups
        .iter()
        .flat_map(|group| group.documents.iter().map(|doc| doc.content.clone()))
        .collect_vec();

    let embeddings = embedding.call(texts).await;
    let mut embeddings_queue = VecDeque::from(embeddings);
    // VecDeque for efficient drain from the front of it
    let mut embedded_groups = vec![];

    for group in doc_groups {
        let size = group.documents.len();
        let entity_embeddings = embeddings_queue.drain(..size);
        let document_refs = group
            .documents
            .into_iter()
            .enumerate()
            .zip(entity_embeddings)
            .map(|((index, doc), embedding)| DocumentRef::new(group.id, index, embedding, doc.life))
            .collect_vec();
        let embedded_group = EmbeddedDocumentGroup {
            id: group.id,
            hash: group.hash,
            documents: document_refs,
        };
        embedded_groups.push(embedded_group);
    }
    embedded_groups
}

fn retrieve_embeddings_from_cache(
    doc_group: &HashedDocumentGroup,
    cache: &EmbeddingCache,
) -> Option<Vec<DocumentRef>> {
    cache
        .get_embeddings(doc_group.id, doc_group.hash)
        .map(|embeddings| {
            doc_group
                .documents
                .iter()
                .zip(embeddings)
                .enumerate()
                .map(|(index, (doc, embedding))| {
                    DocumentRef::new(doc_group.id, index, embedding, doc.life.clone())
                })
                .collect_vec()
        })
}

fn update_cache(cache: &EmbeddingCache, group: &EmbeddedDocumentGroup) {
    let embeddings = group
        .documents
        .iter()
        .map(|doc| doc.embedding.clone())
        .collect_vec();
    cache.upsert_embeddings(group.id, group.hash, embeddings);
}
