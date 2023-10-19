use crate::{
    db::{
        api::view::internal::IntoDynamic,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{
        document_source::DocumentSource, entity_id::EntityId, graph_entity::GraphEntity,
        vectorized_graph::VectorizedGraph, Embedding, EmbeddingFunction, EntityDocuments,
    },
};
use async_trait::async_trait;
use futures_util::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    convert::identity,
    fs::{create_dir_all, File},
    hash::{Hash, Hasher},
    io::{BufReader, BufWriter},
    path::Path,
};

const CHUNK_SIZE: usize = 1000;

#[derive(Clone)]
pub(crate) struct HashedEntityDocuments {
    id: EntityId,
    hash: u64,
    documents: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct EmbeddingsCache {
    hash: u64,
    embeddings: Vec<Embedding>,
}

// TODO: move this to different file
pub trait DocumentTemplate: Send + Sync {
    fn template_node<G: GraphViewOps>(vertex: &VertexView<G>) -> Box<dyn Iterator<Item = String>>;
    fn template_edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Box<dyn Iterator<Item = String>>;
}

pub struct DefaultTemplate;

impl DocumentTemplate for DefaultTemplate {
    fn template_node<G: GraphViewOps>(vertex: &VertexView<G>) -> Box<dyn Iterator<Item = String>> {
        let name = vertex.name();
        let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
        Box::new(std::iter::once(format!(
            "The entity {name} has the following details:\n{property_list}"
        )))
    }

    fn template_edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Box<dyn Iterator<Item = String>> {
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
        Box::new(std::iter::once(content))
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
        template: T, // FIXME: I tried to put templates behind an option but didn't work and hadn't time to fix it
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

    async fn vectorize_with_template<T: DocumentTemplate>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        template: T,
    ) -> VectorizedGraph<G, T> {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        let node_docs = self
            .vertices()
            .iter()
            .map(|vertex| vertex.generate_docs(&template));
        let edge_docs = self.edges().map(|edge| edge.generate_docs(&template));

        let node_embeddings = generate_embeddings(node_docs, &embedding, cache_dir).await;
        let edge_embeddings = generate_embeddings(edge_docs, &embedding, cache_dir).await;

        VectorizedGraph::new(
            self.clone(),
            embedding,
            node_embeddings,
            edge_embeddings,
            template,
        )
    }
}

async fn generate_embeddings<I>(
    docs: I,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> HashMap<EntityId, Vec<Embedding>>
where
    I: Iterator<Item = EntityDocuments>,
{
    let docs = docs.map(|doc| hash_doc(doc));
    let mut embeddings = HashMap::new();
    let mut misses = vec![];

    for doc in docs {
        match retrieve_embeddings_from_cache(&doc, cache_dir) {
            Some(embedding) => {
                embeddings.insert(doc.id, embedding);
            }
            None => misses.push(doc),
        }
    }

    let embedding_tasks = misses
        .chunks(CHUNK_SIZE)
        .map(|chunk| compute_embeddings_updating_cache(chunk.to_vec(), embedding, cache_dir));
    let computed_embeddings = join_all(embedding_tasks).await.into_iter().flatten();
    for (id, embedding) in computed_embeddings {
        embeddings.insert(id, embedding);
    }

    embeddings
}

async fn compute_embeddings_updating_cache(
    docs: Vec<HashedEntityDocuments>,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> Vec<(EntityId, Vec<Embedding>)> {
    // TODO: to avoid cloning below, I could here save an iterator of ids, hashed an number of docs
    let ungrouped_documents = docs
        .iter()
        .flat_map(|doc| {
            doc.documents
                .clone()
                .into_iter()
                .map(|text| (doc.id.clone(), text))
        })
        .collect_vec(); // because we need to use this twice

    let ungrouped_document_ids = ungrouped_documents
        .iter()
        .map(move |(id, _)| id.clone()) // TODO: review use of move
        .collect_vec();

    let texts = ungrouped_documents
        .into_iter()
        .map(|(_, text)| text)
        .collect_vec();
    let embeddings = embedding.call(texts).await;

    let grouped_embeddings = ungrouped_document_ids
        .into_iter()
        .zip(embeddings.into_iter())
        .group_by(|(id, _)| id.clone())
        .into_iter()
        .map(|(id, group)| {
            let embeddings = group.map(|(id, embedding)| embedding).collect_vec();
            (id.clone(), embeddings) // TODO: remove clone if not needed
        })
        .collect_vec();

    let results_for_caching = grouped_embeddings
        .iter()
        .zip(docs)
        .map(|((id, embeddings), doc)| (id, doc.hash, embeddings));

    for (id, hash, embeddings) in results_for_caching {
        let embeddings = embeddings.to_vec(); // TODO remove to_vec if we can consume the iterator
        let embedding_cache = EmbeddingsCache { hash, embeddings };
        let doc_path = cache_dir.join(id.to_string());
        let doc_file =
            File::create(doc_path).expect("Couldn't create file to store embedding cache");
        let mut doc_writer = BufWriter::new(doc_file);
        bincode::serialize_into(&mut doc_writer, &embedding_cache)
            .expect("Couldn't serialize embedding cache");
    }

    grouped_embeddings
}

fn retrieve_embeddings_from_cache(
    doc: &HashedEntityDocuments,
    cache_dir: &Path,
) -> Option<Vec<Embedding>> {
    let doc_path = cache_dir.join(doc.id.to_string());
    let doc_file = File::open(doc_path).ok()?;
    let mut doc_reader = BufReader::new(doc_file);
    let embedding_cache: EmbeddingsCache = bincode::deserialize_from(&mut doc_reader).ok()?;
    if doc.hash == embedding_cache.hash {
        Some(embedding_cache.embeddings)
    } else {
        None // this means a cache miss
    }
}

fn hash_doc(doc: EntityDocuments) -> HashedEntityDocuments {
    let mut hasher = DefaultHasher::new();
    doc.documents.hash(&mut hasher);
    HashedEntityDocuments {
        id: doc.id,
        hash: hasher.finish(),
        documents: doc.documents,
    }
}
