use std::{future::Future, ops::Deref, pin::Pin, sync::Arc};

use crate::{core::utils::errors::GraphResult, vectors::Embedding};
use async_openai::{
    types::{CreateEmbeddingRequest, EmbeddingInput},
    Client,
};
use futures_util::{future::BoxFuture, Stream, StreamExt};
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

pub(super) fn compute_embeddings<'a, I>(
    documents: I,
    cache: &'a VectorCache,
) -> impl Stream<Item = GraphResult<(u32, Embedding)>> + Send + 'a
where
    I: Iterator<Item = (u32, String)> + Send + 'a,
{
    futures_util::stream::iter(documents)
        .chunks(CHUNK_SIZE)
        .then(|chunk| async {
            let texts = chunk.iter().map(|(_, text)| text.clone()).collect();
            let stream: Pin<Box<dyn Stream<Item = GraphResult<(u32, Embedding)>> + Send>> =
                match cache.get_embeddings(texts).await {
                    Ok(embeddings) => {
                        let embedded: Vec<_> = chunk
                            .into_iter()
                            .zip(embeddings)
                            .map(|((id, _), vector)| Ok((id, vector)))
                            .collect(); // TODO: do I really need this collect?
                        Box::pin(futures_util::stream::iter(embedded))
                    }
                    Err(error) => Box::pin(futures_util::stream::iter([Err(error)])),
                };
            stream
        })
        .flatten()
}
