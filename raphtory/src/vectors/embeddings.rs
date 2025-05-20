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

// TODO: move this to common place, utils maybe?
pub(super) fn compute_embeddings<'a, I>(
    documents: I,
    cache: Arc<VectorCache>,
) -> impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send + 'a
where
    I: Iterator<Item = (u32, String)> + Send + 'a,
{
    // let cache = cache.clone();
    // // TODO: remove async_stream if Im not finally using it
    // futures_util::stream::iter(documents)
    //     .chunks(CHUNK_SIZE)
    //     .map(move |chunk| {
    //         let cache_clone = cache.clone();
    //         async move { compute_chunk(chunk, cache_clone).await }
    //     })
    //     .buffered(1)
    //     .flat_map(|result| {
    //         let stream: Pin<Box<dyn Stream<Item = GraphResult<(u32, Embedding)>> + Send + Unpin>> =
    //             match result {
    //                 Ok(result) => Box::pin(futures_util::stream::iter(result).map(Ok)),
    //                 Err(err) => Box::pin(futures_util::stream::iter([Err(err)])),
    //             };
    //         stream
    //     })
    // //////////////////////////////////////////////////////////
    // async_stream::stream! {
    //     // tried to implement this using documents.chunks(), but the resulting type is not Send and breaks this function
    //     let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    //     for document in documents {
    //         buffer.push(document);
    //         if buffer.len() >= CHUNK_SIZE {
    //             for result in compute_chunk(buffer.clone(), cache).await? {
    //                 yield Ok(result)
    //             }
    //             buffer.clear();
    //         }
    //     }
    //     if buffer.len() > 0 {
    //         for result in compute_chunk(buffer.clone(), cache).await? {
    //             yield Ok(result)
    //         }
    //     }
    // }
    // //////////////////////////////////////////////////////////////
    async_stream::stream! {
        // Process documents in chunks
        // let mut buffer = Vec::with_capacity(CHUNK_SIZE);

        // Collect all documents into an owned collection first
        let mut all_documents = Vec::new();
        for document in documents {
            all_documents.push(document);
        }

        // Now process in chunks without capturing references
        for chunk in all_documents.chunks(CHUNK_SIZE) {
            let chunk_owned = chunk.to_vec(); // Create owned data
            for result in compute_chunk(chunk_owned, Arc::clone(&cache)).await? {
                yield Ok(result);
            }
        }
    }
}

async fn compute_chunk(
    documents: Vec<(u32, String)>,
    cache: Arc<VectorCache>,
) -> GraphResult<Vec<(u32, Embedding)>> {
    // let ids: Vec<_> = documents.iter().map(|(id, _)| *id).collect();
    let texts = documents.iter().map(|(_, text)| text.clone());
    let vectors = cache.get_embeddings(texts).await;
    let embedded = documents
        .into_iter()
        .zip(vectors)
        .map(|((id, _), vector)| (id, vector));
    Ok(embedded.collect())
}
