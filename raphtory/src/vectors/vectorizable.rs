use crate::{
    db::{
        api::view::internal::IntoDynamic,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{
        document_ref::{DocumentRef, Life},
        entity_id::EntityId,
        graph_entity::GraphEntity,
        vectorized_graph::VectorizedGraph,
        Embedding, EmbeddingFunction, EntityDocuments, HashedEntityDocuments, InputDocument,
    },
};
use async_trait::async_trait;
use futures_util::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, VecDeque},
    convert::identity,
    fs::{create_dir_all, File},
    hash::{Hash, Hasher},
    io::{BufReader, BufWriter},
    marker::PhantomData,
    path::Path,
};

const CHUNK_SIZE: usize = 1000;

#[derive(Serialize, Deserialize)]
struct EmbeddingsCache {
    hash: u64,
    embeddings: Vec<Embedding>,
}

// TODO: move this to different file
pub trait DocumentTemplate: Send + Sync {
    fn template_node<G: GraphViewOps>(
        vertex: &VertexView<G>,
    ) -> Box<dyn Iterator<Item = InputDocument>>;
    fn template_edge<G: GraphViewOps>(
        edge: &EdgeView<G>,
    ) -> Box<dyn Iterator<Item = InputDocument>>;
}

pub struct DefaultTemplate;

impl DocumentTemplate for DefaultTemplate {
    fn template_node<G: GraphViewOps>(
        vertex: &VertexView<G>,
    ) -> Box<dyn Iterator<Item = InputDocument>> {
        let name = vertex.name();
        let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
        let content = format!("The entity {name} has the following details:\n{property_list}");
        Box::new(std::iter::once(content.into()))
    }

    fn template_edge<G: GraphViewOps>(
        edge: &EdgeView<G>,
    ) -> Box<dyn Iterator<Item = InputDocument>> {
        let src = edge.src().name();
        let dst = edge.dst().name();
        // TODO: property list

        let layer_lines = edge.layer_names().map(|layer| {
            let times = edge
                .layer(layer.clone())
                .unwrap()
                .history()
                .iter()
                .join(", ");
            match layer.as_ref() {
                "_default" => format!("{src} interacted with {dst} at times: {times}"),
                layer => format!("{src} {layer} {dst} at times: {times}"),
            }
        });
        let content: String =
            itertools::Itertools::intersperse(layer_lines, "\n".to_owned()).collect();
        Box::new(std::iter::once(content.into()))
    }
}

#[async_trait]
pub trait Vectorizable<G: GraphViewOps> {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G, DefaultTemplate>;

    async fn vectorize_with_template<T: DocumentTemplate>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G, T>;
}

#[async_trait]
impl<G: GraphViewOps + IntoDynamic> Vectorizable<G> for G {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path, // TODO: make this optional maybe
    ) -> VectorizedGraph<G, DefaultTemplate> {
        self.vectorize_with_template::<DefaultTemplate>(embedding, cache_dir)
            .await
    }

    async fn vectorize_with_template<T: DocumentTemplate>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G, T> {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        let node_docs = self.vertices().iter().map(|vertex| {
            let documents = T::template_node(&vertex).collect_vec();
            EntityDocuments::new(vertex.into(), documents)
        });
        let edge_docs = self.edges().map(|edge| {
            let documents = T::template_edge(&edge).collect_vec();
            EntityDocuments::new(edge.into(), documents)
        });

        let node_embeddings = generate_embeddings(node_docs, &embedding, cache_dir).await;
        let edge_embeddings = generate_embeddings(edge_docs, &embedding, cache_dir).await;

        VectorizedGraph::new(
            self.clone(),
            embedding,
            node_embeddings,
            edge_embeddings,
            PhantomData,
        )
    }
}

// TODO: rename to attach_embeddings. Input
async fn generate_embeddings<I>(
    docs_by_entity: I,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> HashMap<EntityId, Vec<DocumentRef>>
where
    I: Iterator<Item = EntityDocuments>,
{
    let hashed_docs_by_entity = docs_by_entity.map(|entity_documents| (entity_documents.hash()));
    let mut document_refs = HashMap::new();
    let mut misses = vec![];

    for doc in hashed_docs_by_entity {
        match retrieve_embeddings_from_cache(&doc, cache_dir) {
            Some(embedding) => {
                document_refs.insert(doc.id, embedding);
            }
            None => misses.push(doc),
        }
    }

    let embedding_tasks = misses
        .chunks(CHUNK_SIZE)
        .map(|chunk| compute_embeddings_updating_cache(chunk.to_vec(), embedding, cache_dir));
    let computed_embeddings = join_all(embedding_tasks).await.into_iter().flatten();
    for (id, documents) in computed_embeddings {
        document_refs.insert(id, documents);
    }

    document_refs
}

async fn compute_embeddings_updating_cache(
    docs: Vec<HashedEntityDocuments>,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> impl Iterator<Item = (EntityId, Vec<DocumentRef>)> {
    // TODO: use this naming convention everywhere, "groups" for sets of documents referring to the same entity
    let embedded_document_groups = compute_embeddings(docs, embedding).await;

    for group in &embedded_document_groups {
        let embeddings = group
            .documents
            .iter()
            .map(|doc| doc.embedding.clone())
            .collect_vec();
        let embedding_cache = EmbeddingsCache {
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
        .into_iter()
        .map(|group| (group.id, group.documents))
}

struct EmbeddedEntityDocuments {
    id: EntityId,
    hash: u64,
    documents: Vec<(DocumentRef)>,
}

async fn compute_embeddings(
    docs_by_entity: Vec<HashedEntityDocuments>,
    embedding: &Box<dyn EmbeddingFunction>,
) -> Vec<EmbeddedEntityDocuments> {
    let texts = docs_by_entity
        .iter()
        .flat_map(|docs| docs.documents.iter().map(|doc| doc.content.clone()))
        .collect_vec();

    let embeddings = embedding.call(texts).await;
    let mut embeddings_queue = VecDeque::from(embeddings);
    let mut embedded_entity_docs = vec![];

    for entity_docs in docs_by_entity {
        let size = entity_docs.documents.len();
        let entity_embeddings = embeddings_queue.drain(..size);
        let document_refs = entity_docs
            .documents
            .into_iter()
            .enumerate()
            .zip(entity_embeddings)
            .map(|((index, doc), embedding)| {
                DocumentRef::new(entity_docs.id, index, embedding, doc.life)
            })
            .collect_vec();
        let embedded = EmbeddedEntityDocuments {
            id: entity_docs.id,
            hash: entity_docs.hash,
            documents: document_refs,
        };
        embedded_entity_docs.push(embedded);
    }
    embedded_entity_docs
}

fn retrieve_embeddings_from_cache(
    entity_docs: &HashedEntityDocuments,
    cache_dir: &Path,
) -> Option<Vec<DocumentRef>> {
    let doc_path = cache_dir.join(entity_docs.id.to_string());
    let doc_file = File::open(doc_path).ok()?;
    let mut doc_reader = BufReader::new(doc_file);
    let embedding_cache: EmbeddingsCache = bincode::deserialize_from(&mut doc_reader).ok()?;
    if entity_docs.hash == embedding_cache.hash {
        let id = entity_docs.id;
        let document_refs = entity_docs
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
