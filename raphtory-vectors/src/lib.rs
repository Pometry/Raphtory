//! Everything in this crate is behind the non-default `vectors` feature.
#![cfg(feature = "vectors")]

use raphtory::db::{
    api::view::StaticGraphViewOps,
    graph::{edge::EdgeView, node::NodeView},
};
use std::{future::Future, pin::Pin};

pub mod cache;
pub mod custom;
pub mod datetimeformat;
mod embedding;
pub mod embeddings;
mod entity_db;
mod entity_ref;
pub mod errors;
pub mod splitting;
pub mod storage; // TODO: re-export Embeddings instead of making this public
pub mod template;
mod utils;
pub mod vector_collection;
pub mod vector_selection;
pub mod vectorisable;
pub mod vectorised_graph;

#[cfg(feature = "python")]
pub mod python;

pub use embedding::Embedding;

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
