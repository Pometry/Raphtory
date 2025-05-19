use std::{future::Future, sync::Arc};

use crate::{core::utils::errors::GraphResult, vectors::Embedding};
use async_openai::{
    types::{CreateEmbeddingRequest, EmbeddingInput},
    Client,
};
use async_stream::stream;
use futures_util::future::BoxFuture;
use tracing::info;

use super::embedding_cache::EmbeddingCache;

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
    embedding: &'a dyn EmbeddingFunction,
    cache: &'a Option<EmbeddingCache>,
) -> impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send + 'a
where
    I: Iterator<Item = (u32, String)> + Send + 'a,
{
    stream! {
        // tried to implement this using documents.chunks(), but the resulting type is not Send and breaks this function
        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
        for document in documents {
            buffer.push(document);
            if buffer.len() >= CHUNK_SIZE {
                let chunk = compute_chunk(&buffer, embedding, cache).await?;
                for result in chunk {
                    yield Ok(result)
                }
                buffer.clear();
            }
        }
        if buffer.len() > 0 {
            let chunk = compute_chunk(&buffer, embedding, cache).await?;
            for result in chunk {
                yield Ok(result)
            }
        }
    }
}

async fn compute_chunk(
    documents: &Vec<(u32, String)>,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
) -> GraphResult<Vec<(u32, Embedding)>> {
    let mut misses = vec![];
    let mut embedded = vec![];
    match cache {
        Some(cache) => {
            for (id, doc) in documents {
                let embedding = cache.get_embedding(&doc);
                match embedding {
                    Some(embedding) => embedded.push((*id, embedding)),
                    None => misses.push((*id, doc.clone())),
                }
            }
        }
        None => misses = documents.iter().cloned().collect(),
    };

    let texts: Vec<_> = misses.iter().map(|(_, doc)| doc.clone()).collect();
    let embeddings = if texts.is_empty() {
        vec![]
    } else {
        embedding.call(texts).await?
    };

    for ((id, doc), embedding) in misses.into_iter().zip(embeddings) {
        if let Some(cache) = cache {
            cache.upsert_embedding(&doc, embedding.clone())
        };
        embedded.push((id, embedding));
    }

    Ok(embedded)
}
