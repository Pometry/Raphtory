use itertools::chain;
use itertools::Itertools;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter};
use std::path::Path;

use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};
use async_openai::Client;
// use futures_util::future::join_all;
// use futures_util::{stream, StreamExt};
use tokio::task;

use async_stream::stream;

use futures_util::pin_mut;
use futures_util::stream::StreamExt;

use raphtory::db::graph::vertex::VertexView;
use raphtory::prelude::{GraphViewOps, VertexViewOps};
use serde::{Deserialize, Serialize};

use numpy::{PyArray1, PyArray2};
use pyo3::{types::IntoPyDict, PyResult, Python};

pub struct VectorStore<G> {
    graph: G,
    embeddings: Vec<(u64, Embedding)>,
}

impl<G: GraphViewOps> VectorStore<G> {
    pub fn load_graph(graph: G, cache_dir: &Path) -> Self {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        let vertices = graph.vertices().iter();
        let docs = vertices.map(|vertex| vertex.into_doc());

        // ----------------- ASYNC-VERSION -----------------
        // let mut embeddings = vec![];
        // let embedding_stream = stream! {
        //     for doc in docs {
        //         yield (doc.id, doc_to_vec(doc, cache_dir))
        //     }
        // };
        // pin_mut!(embedding_stream);
        // while let Some(embedding) = embedding_stream.next() {
        //     embeddings.push(embedding);
        // }
        // ---------------------------------------------------

        let embeddings = docs
            .map(|doc| (doc.id, doc_to_vec(doc, cache_dir)))
            .collect_vec();

        VectorStore { graph, embeddings }
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<String> {
        let query_embedding = compute_embedding(vec![query.to_owned()]).remove(0);

        let distances = self
            .embeddings
            .iter()
            .map(|(id, embedding)| (id, cosine(&query_embedding, embedding)));

        distances
            .sorted_by(|(_, d1), (_, d2)| d1.partial_cmp(d2).unwrap().reverse())
            // We use reverse because default sorting is ascending but we want it descending
            .take(limit)
            .map(|(id, _)| self.graph.vertex(*id).unwrap().into_doc().content)
            .collect_vec()
    }
}

fn cosine(vector1: &Embedding, vector2: &Embedding) -> f32 {
    assert_eq!(vector1.len(), vector2.len());

    let dot_product: f32 = vector1.iter().zip(vector2.iter()).map(|(x, y)| x * y).sum();
    // let x_length: f32 = vector1.iter().map(|x| x * x).sum();
    // let y_length: f32 = vector2.iter().map(|y| y * y).sum();
    // Vectors are already normalized, so we only need to calculate the dot product:
    // see: https://platform.openai.com/docs/guides/embeddings/which-distance-function-should-i-use

    // dot_product / (x_length.sqrt() * y_length.sqrt())
    dot_product
}

#[derive(Debug, PartialEq)]
pub struct EntityDocument {
    id: u64,
    content: String,
}

#[derive(Serialize, Deserialize)]
struct EmbeddingCache {
    doc_hash: u64,
    embedding: Embedding,
}

type Embedding = Vec<f32>;

fn doc_to_vec(doc: EntityDocument, cache_dir: &Path) -> Embedding {
    let doc_path = cache_dir.join(doc.id.to_string());
    // It'd be better if we reused the same vector if two different docs are equal,
    // but that shouldn't happen anyways

    match retrieve_embedding_from_cache(&doc, &doc_path) {
        None => {
            let doc_hash = hash_doc(&doc); // FIXME: I'm hashing twice
            let embedding = compute_embedding(vec![doc.content]).remove(0);
            let embedding_cache = EmbeddingCache {
                doc_hash,
                embedding,
            };
            let doc_file =
                File::create(doc_path).expect("Couldn't create file to store embedding cache");
            let mut doc_writer = BufWriter::new(doc_file);
            bincode::serialize_into(&mut doc_writer, &embedding_cache)
                .expect("Couldn't serialize embedding cache");
            embedding_cache.embedding
        }
        Some(embedding) => embedding,
    }
}

fn compute_embedding(texts: Vec<String>) -> Vec<Embedding> {
    Python::with_gil(|py| {
        let sentence_transformers = py.import("sentence_transformers")?;
        let locals = [("sentence_transformers", sentence_transformers)].into_py_dict(py);
        locals.set_item("texts", texts);

        let pyarray: &PyArray2<f32> = py
            .eval(
                &format!(
                    "sentence_transformers.SentenceTransformer('thenlper/gte-small').encode(texts + ['test'])"
                ),
                Some(locals),
                None,
            )?
            .extract()?;

        let readonly = pyarray.readonly();
        let chunks = readonly.as_slice().unwrap().chunks(384).into_iter();
        let embeddings = chunks.map(|chunk| chunk.iter().copied().collect_vec()).collect_vec();


        Ok::<Vec<Vec<f32>>, Box<dyn std::error::Error>>(embeddings)
    })
    .unwrap()
}

// async fn compute_embedding(texts: Vec<String>) -> Vec<Embedding> {
//     let num_texts = texts.len();
//     println!("Generating embeddings through OpenAI API for {num_texts} texts");
//     let client = Client::new();
//     let request = CreateEmbeddingRequest {
//         model: "text-embedding-ada-002".to_owned(),
//         input: EmbeddingInput::StringArray(texts),
//         user: None,
//     };
//     let mut response = client.embeddings().create(request).await.unwrap();
//     println!("> Generated embeddings successfully");
//     response.data.into_iter().map(|e| e.embedding).collect_vec()
// }

fn retrieve_embedding_from_cache(doc: &EntityDocument, doc_path: &Path) -> Option<Embedding> {
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

trait IntoDoc {
    fn into_doc(self) -> EntityDocument;
}

fn fmt_time(time: Option<i64>) -> String {
    time.map(|time| time.to_string())
        .unwrap_or_else(|| "missing".to_owned())
}

impl<G: GraphViewOps> IntoDoc for VertexView<G> {
    fn into_doc(self) -> EntityDocument {
        let id = format!("ID: {}", self.name());
        let min_time = format!("earliest activity: {}", fmt_time(self.earliest_time()));
        let max_time = format!("latest activity: {}", fmt_time(self.latest_time()));

        let prop_storage = self.properties();
        let props = prop_storage
            .iter()
            .map(|(key, prop)| (key.to_owned(), prop.to_string()))
            .filter(|(key, _)| key != "_id")
            .map(|(key, prop)| format!("{key}: {prop}"))
            .sorted_by(|a, b| a.len().cmp(&b.len()));
        // We sort by length so when cutting out the tail of the document we don't remove small properties

        let lines = chain!([id, min_time, max_time], props);
        let raw_content: String = lines.intersperse("\n".to_owned()).take(1000).collect();
        let content = match raw_content.char_indices().nth(1000) {
            Some((index, _)) => (&raw_content[..index]).to_owned(),
            None => raw_content,
        };
        // shortened to 1000 (around 250 tokens) to avoid exceeding the max number of tokens,
        // when embedding but also when inserting documents into prompts

        EntityDocument {
            id: self.id(),
            content,
        }
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;
    use dotenv::dotenv;
    use raphtory::prelude::{AdditionOps, Graph, Prop};
    use std::path::PathBuf;

    #[test]
    fn test_into_doc() {
        let g = Graph::new();
        g.add_vertex(0, "Frodo", [("kind".to_string(), Prop::str("Hobbit"))]);

        let doc = g.vertex("Frodo").unwrap().into_doc().content;
        let expected_doc = r###"ID: Frodo
earliest activity: 0
latest activity: 0
kind: Hobbit"###;
        assert_eq!(doc, expected_doc);
    }

    #[test]
    fn test_vector_store() {
        let g = Graph::new();
        g.add_vertex(0, "Gandalf", [("kind".to_string(), Prop::str("wizard"))]);
        g.add_vertex(0, "Frodo", [("kind".to_string(), Prop::str("Hobbit"))]);

        dotenv().ok();
        let vec_store =
            VectorStore::load_graph(g, &PathBuf::from("/tmp/raphtory/vector-cache-lotr-test"));

        let docs = vec_store.search("Find a wizard", 1);
        assert!(docs[0].contains("Gandalf"));

        let docs = vec_store.search("Find a magician", 1);
        assert!(docs[0].contains("Gandalf"));

        let docs = vec_store.search("Find a small person", 1);
        assert!(docs[0].contains("Frodo"));

        let docs = vec_store.search("What do you know about Gandalf?", 1);
        assert!(docs[0].contains("Gandalf"));

        println!(">>>>>>>>>>>>>>>>>>success");
    }
}
