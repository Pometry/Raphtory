use crate::{
    db::{
        api::view::internal::IntoDynamic,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{
        document_source::DocumentSource, entity_id::EntityId, graph_entity::GraphEntity,
        vectorized_graph::VectorizedGraph, Embedding, EmbeddingFunction, EntityDocument,
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

#[derive(Serialize, Deserialize)]
struct EmbeddingCache {
    doc_hash: u64,
    embedding: Embedding,
}

#[async_trait]
pub trait Vectorizable<G: GraphViewOps> {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G>;

    async fn vectorize_with_templates<N, E>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        node_template: N,
        edge_template: E,
        // FIXME: I tried to put templates behind an option but didn't work and hadn't time to fix it
    ) -> VectorizedGraph<G>
    where
        N: Fn(&VertexView<G>) -> String + Sync + Send + 'static,
        E: Fn(&EdgeView<G>) -> String + Sync + Send + 'static;
}

#[async_trait]
impl<G: GraphViewOps + IntoDynamic> Vectorizable<G> for G {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path, // TODO: make this optional maybe
    ) -> VectorizedGraph<G> {
        let node_template = |vertex: &VertexView<G>| default_node_template(vertex);
        let edge_template = |edge: &EdgeView<G>| default_edge_template(edge);

        self.vectorize_with_templates(embedding, cache_dir, node_template, edge_template)
            .await
    }

    async fn vectorize_with_templates<N, E>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        node_template: N,
        edge_template: E,
    ) -> VectorizedGraph<G>
    where
        N: Fn(&VertexView<G>) -> String + Sync + Send + 'static,
        E: Fn(&EdgeView<G>) -> String + Sync + Send + 'static,
    {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        let node_docs = self
            .vertices()
            .iter()
            .map(|vertex| vertex.generate_doc(&node_template));
        let edge_docs = self.edges().map(|edge| edge.generate_doc(&edge_template));

        let node_embeddings = generate_embeddings(node_docs, &embedding, cache_dir).await;
        let edge_embeddings = generate_embeddings(edge_docs, &embedding, cache_dir).await;

        VectorizedGraph::new(
            self.clone(),
            embedding,
            node_embeddings,
            edge_embeddings,
            Box::new(node_template),
            Box::new(edge_template),
        )
    }
}

fn default_node_template<G: GraphViewOps>(vertex: &VertexView<G>) -> String {
    let name = vertex.name();
    let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
    format!("The entity {name} has the following details:\n{property_list}")
}

fn default_edge_template<G: GraphViewOps>(edge: &EdgeView<G>) -> String {
    let src = edge.src().name();
    let dst = edge.dst().name();
    // TODO: property list

    edge.layer_names()
        .iter()
        .map(|layer| {
            let times = edge.layer(layer).unwrap().history().iter().join(", ");
            match layer.as_str() {
                "_default" => format!("{src} interacted with {dst} at times: {times}"),
                layer => format!("{src} {layer} {dst} at times: {times}"),
            }
        })
        .intersperse("\n".to_owned())
        .collect()
}

async fn generate_embeddings<I>(
    docs: I,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> HashMap<EntityId, Embedding>
where
    I: Iterator<Item = EntityDocument>,
{
    let mut embeddings = HashMap::new();
    let mut misses = vec![];

    for doc in docs {
        match retrieve_embedding_from_cache(&doc, cache_dir) {
            Some(embedding) => {
                embeddings.insert(doc.id, embedding);
            }
            None => misses.push(doc),
        }
    }

    let embedding_tasks = misses
        .chunks(CHUNK_SIZE)
        .map(|chunk| compute_embeddings_with_cache(chunk.to_vec(), embedding, cache_dir));
    let computed_embeddings = join_all(embedding_tasks).await.into_iter().flatten();
    for (id, embedding) in computed_embeddings {
        embeddings.insert(id, embedding);
    }

    embeddings
}

async fn compute_embeddings_with_cache(
    docs: Vec<EntityDocument>,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> Vec<(EntityId, Embedding)> {
    let texts = docs.iter().map(|doc| doc.content.clone()).collect_vec();
    let embeddings = embedding.call(texts).await;
    docs.into_iter()
        .zip(embeddings)
        .map(|(doc, embedding)| {
            let doc_hash = hash_doc(&doc); // FIXME: I'm hashing twice
            let embedding_cache = EmbeddingCache {
                doc_hash,
                embedding,
            };
            let doc_path = cache_dir.join(doc.id.to_string());
            let doc_file =
                File::create(doc_path).expect("Couldn't create file to store embedding cache");
            let mut doc_writer = BufWriter::new(doc_file);
            bincode::serialize_into(&mut doc_writer, &embedding_cache)
                .expect("Couldn't serialize embedding cache");
            (doc.id, embedding_cache.embedding)
        })
        .collect_vec()
}

fn retrieve_embedding_from_cache(doc: &EntityDocument, cache_dir: &Path) -> Option<Embedding> {
    let doc_path = cache_dir.join(doc.id.to_string());
    let doc_file = File::open(doc_path).ok()?;
    let mut doc_reader = BufReader::new(doc_file);
    let embedding_cache: EmbeddingCache = bincode::deserialize_from(&mut doc_reader).ok()?;
    let doc_hash = hash_doc(doc);
    if doc_hash == embedding_cache.doc_hash {
        Some(embedding_cache.embedding)
    } else {
        None
    }
}

fn hash_doc(doc: &EntityDocument) -> u64 {
    let mut hasher = DefaultHasher::new();
    doc.content.hash(&mut hasher);
    hasher.finish()
}
