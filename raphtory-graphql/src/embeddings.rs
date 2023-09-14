use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};
use async_openai::Client;
use itertools::Itertools;
use raphtory::vectors::Embedding;

async fn compute_embeddings(texts: Vec<String>) -> Vec<Embedding> {
    println!("computing embeddings for {} texts", texts.len());
    let client = Client::new();
    let request = CreateEmbeddingRequest {
        model: "text-embedding-ada-002".to_owned(),
        input: EmbeddingInput::StringArray(texts),
        user: None,
    };
    let mut response = client.embeddings().create(request).await.unwrap();
    println!("Generated embeddings successfully");
    response.data.into_iter().map(|e| e.embedding).collect_vec()
}
