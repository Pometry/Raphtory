use std::{hash::Hash, pin::Pin, sync::Arc};

use async_openai::{
    types::{CreateEmbeddingRequest, EmbeddingInput},
    Client,
};
use futures_util::{future::BoxFuture, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    errors::GraphResult,
    vectors::{cache::CachedEmbeddingModel, storage::OpenAIEmbeddings, Embedding},
};

const CHUNK_SIZE: usize = 1000;

// this is an Arc to allow cloning even if the underlying type doesn't allow it
// the underlying type depends on the embedding model implementation in use
pub(crate) type EmbeddingError = Arc<dyn std::error::Error + Send + Sync>;
pub type EmbeddingResult<T> = Result<T, EmbeddingError>;

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Hash)]
pub enum ModelConfig {
    OpenAI(OpenAIEmbeddings),
}

impl From<OpenAIEmbeddings> for ModelConfig {
    fn from(embeddings: OpenAIEmbeddings) -> Self {
        ModelConfig::OpenAI(embeddings)
    }
}

impl ModelConfig {
    pub(super) async fn call(&self, texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        match self {
            ModelConfig::OpenAI(model) => model.call(texts).await,
        }
    }

    pub fn with_dimension(self, dim: usize) -> Self {
        match self {
            ModelConfig::OpenAI(mut embeddings) => {
                embeddings.dim = Some(dim);
                ModelConfig::OpenAI(embeddings)
            }
        }
    }

    pub fn dim(&self) -> Option<usize> {
        match self {
            ModelConfig::OpenAI(embeddings) => embeddings.dim,
        }
    }
}

impl OpenAIEmbeddings {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        let client = Client::with_config(self.resolve_config());
        let request = CreateEmbeddingRequest {
            model: self.model.clone(),
            input: EmbeddingInput::StringArray(texts),
            user: None,
            encoding_format: None,
            dimensions: None,
        };

        Box::pin(async move {
            let response = client
                .embeddings()
                .create(request)
                .await
                .map_err(|err| Arc::new(err) as Arc<dyn std::error::Error + Send + Sync>)?;
            Ok(response
                .data
                .into_iter()
                .map(|e| e.embedding.into())
                .collect())
        })
    }
}

pub(super) fn compute_embeddings<'a, I>(
    documents: I,
    model: &'a CachedEmbeddingModel,
) -> impl Stream<Item = GraphResult<(u64, Embedding)>> + Send + 'a
where
    I: Iterator<Item = (u64, String)> + Send + 'a,
{
    futures_util::stream::iter(documents)
        .chunks(CHUNK_SIZE)
        .then(|chunk| async {
            let texts = chunk.iter().map(|(_, text)| text.clone()).collect();
            let stream: Pin<Box<dyn Stream<Item = GraphResult<(u64, Embedding)>> + Send>> =
                match model.get_embeddings(texts).await {
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
