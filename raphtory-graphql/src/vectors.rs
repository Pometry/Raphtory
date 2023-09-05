use itertools::chain;
use itertools::Itertools;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Display, Formatter};
use std::fs::{create_dir_all, File};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter};
use std::path::Path;

// use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};
// use async_openai::Client;
// use futures_util::future::join_all;
// use futures_util::{stream, StreamExt};
// use tokio::task;

// use async_stream::stream;
//
// use futures_util::pin_mut;
// use futures_util::stream::StreamExt;

use raphtory::db::graph::vertex::VertexView;
use raphtory::prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps};
use serde::{Deserialize, Serialize, Serializer};

use crate::model::graph::edge::Edge;
use numpy::PyArray2;
use pyo3::{types::IntoPyDict, Python};
use raphtory::db::graph::edge::EdgeView;

// #[derive(Clone)]
// struct EdgeId {
//     src: u64,
//     dst: u64,
// }

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum EntityId {
    Node { id: u64 },
    Edge { src: u64, dst: u64 },
}

pub struct VectorStore<G> {
    graph: G,
    embeddings: Vec<(EntityId, Embedding)>,
}

impl<G: GraphViewOps> VectorStore<G> {
    pub fn load_graph(graph: G, cache_dir: &Path) -> Self {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        let node_docs = graph.vertices().iter().map(|vertex| vertex.into_doc());
        let edge_docs = graph.edges().map(|edge| edge.into_doc());

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

        let mut embeddings = vec![];
        let mut pendings = vec![];

        for doc in chain!(node_docs, edge_docs) {
            match retrieve_embedding_from_cache(&doc, cache_dir) {
                Some(embedding) => embeddings.push((doc.id, embedding)),
                None => pendings.push(doc),
            }
        }

        let computed_embeddings = pendings
            .chunks(100)
            .map(|chunk| compute_embeddings_with_cache(chunk.to_vec(), cache_dir))
            .flatten();

        for (id, embedding) in computed_embeddings {
            embeddings.push((id, embedding));
        }

        VectorStore { graph, embeddings }
    }

    pub fn search(&self, query: &str, limit: usize, hopes: usize) -> Vec<String> {
        let query_embedding = compute_embeddings(vec![query.to_owned()]).remove(0);

        let mut entity_ids: Vec<EntityId> = vec![];

        let distances = self
            .embeddings
            .iter()
            .map(|(id, embedding)| (id, cosine(&query_embedding, embedding)));

        println!("Closest entities for query {query}:");

        let selected_distances = distances
            .sorted_by(|(_, d1), (_, d2)| d1.partial_cmp(d2).unwrap().reverse())
            // We use reverse because default sorting is ascending but we want it descending
            .take(limit);

        for (id, distance) in selected_distances {
            println!(" - At {distance}: {id}");
            entity_ids.push(id.clone())
        }

        for _ in 0..hopes {
            let prev_entity_ids = entity_ids.clone();
            for id in prev_entity_ids {
                match id {
                    EntityId::Node { id } => {
                        let edges = self.graph.vertex(id).unwrap().edges();
                        for edge in edges {
                            entity_ids.push(EntityId::Edge {
                                src: edge.src().id(),
                                dst: edge.dst().id(),
                            })
                        }
                    }
                    EntityId::Edge { src, dst } => {
                        let edge = self.graph.edge(src, dst).unwrap();
                        entity_ids.push(EntityId::Node {
                            id: edge.src().id(),
                        });
                        entity_ids.push(EntityId::Node {
                            id: edge.dst().id(),
                        })
                    }
                }
            }
        }

        entity_ids
            .iter()
            .unique()
            .map(|id| match id {
                EntityId::Node { id } => self.graph.vertex(*id).unwrap().into_doc().content,
                EntityId::Edge { src, dst } => {
                    self.graph.edge(*src, *dst).unwrap().into_doc().content
                }
            })
            .collect_vec()

        // selected_distances
        //     .iter()
        //     .map(|(id, _)| match id {
        //         EntityId::Node { id } => self.graph.vertex(*id).unwrap().into_doc().content,
        //         EntityId::Edge { src, dst } => {
        //             self.graph.edge(*src, *dst).unwrap().into_doc().content
        //         }
        //     })
        //     .collect_vec()
    }
}

fn docs_to_embeddings<I: Iterator<Item = EntityDocument>>(
    docs: I,
    cache_dir: &Path,
) -> Vec<(EntityId, Embedding)> {
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

    let mut embeddings = vec![];
    let mut pendings = vec![];

    for doc in docs {
        match retrieve_embedding_from_cache(&doc, cache_dir) {
            Some(embedding) => embeddings.push((doc.id, embedding)),
            None => pendings.push(doc),
        }
    }

    let computed_embeddings = pendings
        .chunks(100)
        .map(|chunk| compute_embeddings_with_cache(chunk.to_vec(), cache_dir))
        .flatten();

    for (id, embedding) in computed_embeddings {
        embeddings.push((id, embedding));
    }

    embeddings
}

fn cosine(vector1: &Embedding, vector2: &Embedding) -> f32 {
    assert_eq!(vector1.len(), vector2.len());

    let dot_product: f32 = vector1.iter().zip(vector2.iter()).map(|(x, y)| x * y).sum();
    let x_length: f32 = vector1.iter().map(|x| x * x).sum();
    let y_length: f32 = vector2.iter().map(|y| y * y).sum();
    // Vectors are already normalized for ada but nor for gte-small:
    // see: https://platform.openai.com/docs/guides/embeddings/which-distance-function-should-i-use

    dot_product / (x_length.sqrt() * y_length.sqrt())
    // dot_product
}

#[derive(Clone)]
pub struct EntityDocument {
    id: EntityId,
    content: String,
}

// impl Clone for EntityDocument {
//     fn clone(&self) -> Self {
//         Self {
//             id: self.id.clone(),
//             content: self.content.clone(),
//         }
//     }
// }

#[derive(Serialize, Deserialize)]
struct EmbeddingCache {
    doc_hash: u64,
    embedding: Embedding,
}

type Embedding = Vec<f32>;

fn compute_embeddings_with_cache(
    docs: Vec<EntityDocument>,
    cache_dir: &Path,
) -> Vec<(EntityId, Embedding)> {
    let texts = docs.iter().map(|doc| doc.content.clone()).collect_vec();
    let embeddings = compute_embeddings(texts);
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

fn compute_embeddings(texts: Vec<String>) -> Vec<Embedding> {
    println!("computing embeddings for {} texts", texts.len());
    Python::with_gil(|py| {
        let sentence_transformers = py.import("sentence_transformers")?;
        let locals = [("sentence_transformers", sentence_transformers)].into_py_dict(py);
        locals.set_item("texts", texts);

        let pyarray: &PyArray2<f32> = py
            .eval(
                &format!(
                    "sentence_transformers.SentenceTransformer('thenlper/gte-small').encode(texts)"
                ),
                Some(locals),
                None,
            )?
            .extract()?;

        let readonly = pyarray.readonly();
        let chunks = readonly.as_slice().unwrap().chunks(384).into_iter();
        let embeddings = chunks
            .map(|chunk| chunk.iter().copied().collect_vec())
            .collect_vec();

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

// #[derive(Clone, Debug, PartialEq)]
// enum EntityId {
//     VertexId { id: u64 },
//     EdgeId { src_id: u64, dst_id: u64 },
// }

impl Display for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityId::Node { id } => f.serialize_u64(*id),
            EntityId::Edge { src, dst } => {
                f.serialize_u64(*src)
                    .expect("src ID couldn't be serialized");
                f.write_str("-")
                    .expect("edge ID separator couldn't be serialized");
                f.serialize_u64(*dst)
            }
        }
    }
}

// impl Display for EdgeId {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.serialize_u64(self.src)
//             .expect("src ID couldn't be serialized");
//         f.write_str("-")
//             .expect("edge ID separator couldn't be serialized");
//         f.serialize_u64(self.dst)
//     }
// }

trait IntoDoc {
    // fn entity_id(&self) -> EntityId;
    fn into_doc(self) -> EntityDocument;
}

fn fmt_time(time: Option<i64>) -> String {
    time.map(|time| time.to_string())
        .unwrap_or_else(|| "missing".to_owned())
}

impl<G: GraphViewOps> IntoDoc for VertexView<G> {
    // fn entity_id(&self) -> EntityId {
    //     EntityId::VertexId { id: self.id() }
    // }
    fn into_doc(self) -> EntityDocument {
        let node_type = self
            .properties()
            .get("type")
            .map_or_else(|| " ".to_owned(), |n_type| format!(" of type {n_type} "));
        let header = format!("Properties for node{node_type}with ID {}:", self.name());
        let min_time = format!("earliest activity: {}", fmt_time(self.earliest_time()));
        let max_time = format!("latest activity: {}", fmt_time(self.latest_time()));

        let prop_storage = self.properties();
        let props = prop_storage
            .iter()
            .map(|(key, prop)| (key.to_owned(), prop.to_string()))
            .filter(|(key, _)| key != "_id" && key != "type")
            .map(|(key, prop)| format!("{key}: {prop}"))
            .sorted_by(|a, b| a.len().cmp(&b.len()));
        // We sort by length so when cutting out the tail of the document we don't remove small properties

        let lines = chain!([header, min_time, max_time], props);
        let raw_content: String = lines.intersperse("\n".to_owned()).collect();
        let content = match raw_content.char_indices().nth(1000) {
            Some((index, _)) => (&raw_content[..index]).to_owned(),
            None => raw_content,
        };
        // shortened to 1000 (around 250 tokens) to avoid exceeding the max number of tokens,
        // when embedding but also when inserting documents into prompts

        EntityDocument {
            id: EntityId::Node { id: self.id() },
            content,
        }
    }
}

impl<G: GraphViewOps> IntoDoc for EdgeView<G> {
    // fn entity_id(&self) -> EntityId {
    //     EntityId::EdgeId {
    //         src_id: self.src().id(),
    //         dst_id: self.dst().id(),
    //     }
    // }
    fn into_doc(self) -> EntityDocument {
        let header = format!("Edge from {} to {}:", self.src().name(), self.dst().name());
        let layer_names = self.layer_names();
        let layer_reprs = layer_names.iter().map(|layer| {
            let history = self.layer(layer).unwrap().history();
            format!(
                "happened in layer '{layer}' at: {}",
                history.iter().join(",")
            )
        });

        let lines = chain!([header], layer_reprs);
        let content: String = lines.intersperse("\n".to_owned()).collect();

        EntityDocument {
            id: EntityId::Edge {
                src: self.src().id(),
                dst: self.dst().id(),
            },
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

    const NO_PROPS: [(&str, Prop); 0] = [];

    #[test]
    fn test_node_into_doc() {
        let g = Graph::new();
        g.add_vertex(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("Hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
        )
        .unwrap();

        let doc = g.vertex("Frodo").unwrap().into_doc().content;
        let expected_doc = r###"Properties for node of type Hobbit with ID Frodo:
earliest activity: 0
latest activity: 0
age: 30"###;
        assert_eq!(doc, expected_doc);
    }

    #[test]
    fn test_edge_into_doc() {
        let g = Graph::new();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        let doc = g.edge("Frodo", "Gandalf").unwrap().into_doc().content;
        let expected_doc = r###"Edge from Frodo to Gandalf:
happened in layer 'talk to' at: 0"###;
        assert_eq!(doc, expected_doc);
    }

    #[test]
    fn test_vector_store() {
        let g = Graph::new();
        g.add_vertex(
            0,
            "Gandalf",
            [
                ("type".to_string(), Prop::str("wizard")),
                ("age".to_string(), Prop::str("120")),
            ],
        )
        .unwrap();
        g.add_vertex(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("Hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
        )
        .unwrap();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        dotenv().ok();
        let vec_store =
            VectorStore::load_graph(g, &PathBuf::from("/tmp/raphtory/vector-cache-lotr-test"));

        let docs = vec_store.search("Find a wizard", 1, 0);
        assert!(docs[0].contains("Gandalf"));

        let docs = vec_store.search("Find a magician", 1, 0);
        assert!(docs[0].contains("Gandalf"));

        let docs = vec_store.search("Find a small person", 1, 0);
        assert!(docs[0].contains("Frodo"));

        let docs = vec_store.search("What do you know about Gandalf?", 1, 0);
        assert!(docs[0].contains("Gandalf"));

        let docs = vec_store.search("Has anyone talked to anyone else?", 1, 0);
        assert!(docs[0].contains("Edge from Frodo to Gandalf"));
    }
}
