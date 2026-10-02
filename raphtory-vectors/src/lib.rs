use raphtory::db::{
    api::view::StaticGraphViewOps,
    graph::{edge::EdgeView, node::NodeView},
};
use arrow_array::Float32Array;
use serde::{ser::SerializeSeq, Deserialize, Serialize, Serializer};
use std::{future::Future, ops::Deref, pin::Pin};

#[cfg(feature = "python")]
use raphtory_api::python::repr::Repr;

pub mod cache;
pub mod custom;
pub mod datetimeformat;
pub mod embeddings;
pub mod errors;
mod entity_db;
mod entity_ref;
pub mod splitting;
pub mod storage; // TODO: re-export Embeddings instead of making this public
pub mod template;
mod utils;
#[doc(hidden)] // pub for raphtory-tests
pub mod vector_collection;
pub mod vector_selection;
pub mod vectorisable;
pub mod vectorised_graph;

#[cfg(feature = "python")]
pub mod python;

#[derive(Debug, Clone, PartialEq)]
pub struct Embedding(Float32Array);

impl Serialize for Embedding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for i in self.0.values().iter() {
            seq.serialize_element(i)?;
        }
        seq.end()
    }
}

impl<'a> Deserialize<'a> for Embedding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let vec: Vec<f32> = Deserialize::deserialize(deserializer)?;
        Ok(Embedding(Float32Array::from(vec)))
    }
}

impl Deref for Embedding {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        self.0.values()
    }
}

impl Embedding {
    pub fn inner(&self) -> &Float32Array {
        &self.0
    }
}

impl From<Vec<f32>> for Embedding {
    fn from(vec: Vec<f32>) -> Self {
        Embedding(Float32Array::from(vec))
    }
}

impl From<&[f32]> for Embedding {
    fn from(slice: &[f32]) -> Self {
        Embedding(Float32Array::from(slice.to_vec()))
    }
}

impl<const N: usize> From<[f32; N]> for Embedding {
    fn from(array: [f32; N]) -> Self {
        Embedding(Float32Array::from(array.to_vec()))
    }
}

impl From<Float32Array> for Embedding {
    fn from(array: Float32Array) -> Self {
        Embedding(array)
    }
}

#[cfg(feature = "python")]
impl Repr for Embedding {
    fn repr(&self) -> String {
        format!("{:?}", &self.0.values())
    }
}

impl FromIterator<f32> for Embedding {
    fn from_iter<T: IntoIterator<Item = f32>>(iter: T) -> Self {
        let vec: Vec<f32> = iter.into_iter().collect();
        Embedding(Float32Array::from(vec))
    }
}

#[derive(Debug, Clone)]
pub enum DocumentEntity<G: StaticGraphViewOps> {
    Node(NodeView<'static, G>),
    Edge(EdgeView<G>),
}

#[derive(Debug, Clone)]
pub struct Document<G: StaticGraphViewOps> {
    pub entity: DocumentEntity<G>,
    pub content: String,
    pub embedding: Embedding,
}

pub struct VectorsQuery<T> {
    future: Pin<Box<dyn Future<Output = T> + Send>>,
}

impl<T: Send> VectorsQuery<T> {
    pub fn new(future: Pin<Box<dyn Future<Output = T> + Send>>) -> Self {
        Self { future }
    }

    pub async fn execute(self) -> T {
        self.future.await
    }
}

impl<T: Send + 'static> VectorsQuery<T> {
    pub fn resolved(resolved: T) -> Self {
        Self {
            future: Box::pin(async move { resolved }),
        }
    }
}
