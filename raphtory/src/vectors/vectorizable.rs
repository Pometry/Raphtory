use crate::{
    core::entities::LayerIds,
    db::api::view::internal::IntoDynamic,
    prelude::GraphViewOps,
    vectors::{
        document_ref::DocumentRef,
        document_template::{DefaultTemplate, DocumentTemplate},
        entity_id::EntityId,
        vectorized_graph::VectorizedGraph,
        DocumentInput, Embedding, EmbeddingFunction,
    },
};
use async_trait::async_trait;
use futures_util::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, VecDeque},
    fs::{create_dir_all, File},
    hash::{Hash, Hasher},
    io::{BufReader, BufWriter},
    path::Path,
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

#[derive(Serialize, Deserialize)]
struct EmbeddingCache {
    hash: u64,
    embeddings: Vec<Embedding>,
}

#[async_trait]
pub trait Vectorizable<G: GraphViewOps> {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G, DefaultTemplate>;

    async fn vectorize_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        template: T,
    ) -> VectorizedGraph<G, T>;
}

#[async_trait]
impl<G: GraphViewOps + IntoDynamic> Vectorizable<G> for G {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path, // TODO: make this optional maybe
    ) -> VectorizedGraph<G, DefaultTemplate> {
        self.vectorize_with_template(embedding, cache_dir, DefaultTemplate)
            .await
    }

    async fn vectorize_with_template<T: DocumentTemplate<G>>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        template: T,
    ) -> VectorizedGraph<G, T> {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        println!(
            "Computing embeddings for {} vertices and {} edges",
            self.vertices_len(LayerIds::All, None),
            self.edges_len(LayerIds::All, None),
        );

        let nodes = self.vertices().iter().map(|vertex| {
            let documents = template.node(&vertex).collect_vec();
            DocumentGroup::new(vertex.into(), documents)
        });
        let edges = self.edges().map(|edge| {
            let documents = template.edge(&edge).collect_vec();
            DocumentGroup::new(edge.into(), documents)
        });

        let node_refs = attach_embeddings(nodes, &embedding, cache_dir).await;
        let edge_refs = attach_embeddings(edges, &embedding, cache_dir).await;

        println!(
            "Successfully computed embeddings for {} vertices and {} edges",
            node_refs.len(),
            edge_refs.len(),
        );

        VectorizedGraph::new(self.clone(), template, embedding, node_refs, edge_refs)
    }
}

async fn attach_embeddings<I>(
    doc_groups: I,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> HashMap<EntityId, Vec<DocumentRef>>
where
    I: Iterator<Item = DocumentGroup>,
{
    let hashed_doc_groups = doc_groups.map(|entity_documents| (entity_documents.hash()));
    let mut document_groups = HashMap::new();
    let mut misses = vec![];

    for group in hashed_doc_groups {
        match retrieve_embeddings_from_cache(&group, cache_dir) {
            Some(document_refs) => {
                document_groups.insert(group.id, document_refs);
            }
            None => misses.push(group),
        }
    }

    let embedding_tasks = misses
        .chunks(CHUNK_SIZE)
        .map(|chunk| compute_embeddings_updating_cache(chunk.to_vec(), embedding, cache_dir));
    let new_computed_groups = join_all(embedding_tasks).await.into_iter().flatten();
    for group in new_computed_groups {
        document_groups.insert(group.id, group.documents);
    }

    document_groups
}

async fn compute_embeddings_updating_cache(
    docs: Vec<HashedDocumentGroup>,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> Vec<EmbeddedDocumentGroup> {
    // TODO: use this naming convention everywhere, "groups" for sets of documents referring to the same entity
    let embedded_document_groups = compute_embeddings(docs, embedding).await;

    for group in &embedded_document_groups {
        let embeddings = group
            .documents
            .iter()
            .map(|doc| doc.embedding.clone())
            .collect_vec();
        let embedding_cache = EmbeddingCache {
            hash: group.hash,
            embeddings,
        };
        let doc_path = cache_dir.join(group.id.to_string());
        let doc_file =
            File::create(doc_path).expect("Couldn't create file to store embedding cache");
        let mut doc_writer = BufWriter::new(doc_file);
        bincode::serialize_into(&mut doc_writer, &embedding_cache)
            .expect("Couldn't serialize embedding cache");
    }

    embedded_document_groups
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
    cache_dir: &Path,
) -> Option<Vec<DocumentRef>> {
    let doc_path = cache_dir.join(doc_group.id.to_string());
    let doc_file = File::open(doc_path).ok()?;
    let mut doc_reader = BufReader::new(doc_file);
    let embedding_cache: EmbeddingCache = bincode::deserialize_from(&mut doc_reader).ok()?;
    if doc_group.hash == embedding_cache.hash {
        let id = doc_group.id;
        let document_refs = doc_group
            .documents
            .iter()
            .map(|doc| doc.life.clone())
            .zip(embedding_cache.embeddings)
            .enumerate()
            .map(|(index, (life, embedding))| DocumentRef::new(id, index, embedding, life))
            .collect_vec();
        Some(document_refs)
    } else {
        None // this means a cache miss
    }
}
