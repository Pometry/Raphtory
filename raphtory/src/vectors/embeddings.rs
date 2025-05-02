use crate::vectors::Embedding;
use async_openai::{
    types::{CreateEmbeddingRequest, EmbeddingInput},
    Client,
};
use tracing::info;

use super::EmbeddingResult;

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
