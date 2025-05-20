use std::ops::Deref;
use std::pin::Pin;
use std::{future::Future, sync::Arc};

use crate::{core::utils::errors::GraphResult, vectors::Embedding};
use async_openai::{
    types::{CreateEmbeddingRequest, EmbeddingInput},
    Client,
};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use tracing::info;

use super::cache::VectorCache;

const CHUNK_SIZE: usize = 1000;

pub(crate) type EmbeddingError = Box<dyn std::error::Error + Send + Sync>;
pub type EmbeddingResult<T> = Result<T, EmbeddingError>;

pub trait EmbeddingFunction: Send + Sync {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>>;
}

impl<T, F> EmbeddingFunction for T
where
    T: Fn(Vec<String>) -> F + Send + Sync,
    F: Future<Output = EmbeddingResult<Vec<Embedding>>> + Send + 'static,
{
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        Box::pin(self(texts))
    }
}

impl EmbeddingFunction for Arc<dyn EmbeddingFunction> {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        Box::pin(self.deref().call(texts))
    }
}

pub async fn openai_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
    info!("computing embeddings for {} texts", texts.len());
    let client = Client::new();
    let request = CreateEmbeddingRequest {
        model: "text-embedding-ada-002".to_owned(),
        input: EmbeddingInput::StringArray(texts),
        user: None,
        encoding_format: None,
        dimensions: None,
    };
    let response = client.embeddings().create(request).await?;
    info!("Generated embeddings successfully");
    Ok(response
        .data
        .into_iter()
        .map(|e| e.embedding.into())
        .collect())
}

// pub(super) fn compute_embeddings<'a, I>(
//     documents: I,
//     cache: &'a VectorCache,
// ) -> impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send + 'a
// where
//     I: Iterator<Item = (u32, String)> + Send + 'a,
// {
//     async_stream::stream! {
//         // tried to implement this using documents.chunks(), but the resulting type is not Send and breaks this function
//         let mut buffer = Vec::with_capacity(CHUNK_SIZE);
//         for document in documents {
//             buffer.push(document);
//             if buffer.len() >= CHUNK_SIZE {
//                 let chunk = compute_chunk(&buffer, cache).await?;
//                 for result in chunk {
//                     yield Ok(result)
//                 }
//                 buffer.clear();
//             }
//         }
//         if buffer.len() > 0 {
//             let chunk = compute_chunk(&buffer, cache).await?;
//             for result in chunk {
//                 yield Ok(result)
//             }
//         }
//     }
// }

pub(super) fn compute_embeddings<'a, I>(
    documents: I,
    cache: &'a VectorCache,
) -> impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send + 'a
where
    I: Iterator<Item = (u32, String)> + Send + 'a,
{
    async_stream::stream! {
        // tried to implement this using documents.chunks(), but the resulting type is not Send and breaks this function
        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
        for document in documents {
            buffer.push(document);
            if buffer.len() >= CHUNK_SIZE {
                let chunk = compute_chunk(&buffer, cache).await?;
                for result in chunk {
                    yield Ok(result)
                }
                buffer.clear();
            }
        }
        if buffer.len() > 0 {
            let chunk = compute_chunk(&buffer, cache).await?;
            for result in chunk {
                yield Ok(result)
            }
        }
    }
}

async fn compute_chunk(
    documents: &Vec<(u32, String)>,
    cache: &VectorCache,
) -> GraphResult<Vec<(u32, Embedding)>> {
    let texts = documents.iter().map(|(_, text)| text.clone()).collect();
    let vectors = cache.get_embeddings(texts).await;
    let embedded = documents
        .into_iter()
        .zip(vectors)
        .map(|((id, _), vector)| (*id, vector));
    Ok(embedded.collect())
}
