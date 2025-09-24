use async_openai::types::{CreateEmbeddingResponse, Embedding, EmbeddingUsage};
use axum::{
    extract::{Json, State},
    routing::post,
    Router,
};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, Debug)]
struct EmbeddingRequest {
    input: Vec<String>,
}

// #[derive(Serialize)]
// struct EmbeddingResponse {
//     object: String,
//     data: Vec<EmbeddingData>,
// }

// #[derive(Serialize)]
// struct EmbeddingData {
//     object: String,
//     embedding: Vec<f32>,
//     index: usize,
// }

async fn embeddings(
    State(function): State<Arc<dyn Fn(&str) -> Vec<f32> + Send + Sync>>,
    Json(req): Json<EmbeddingRequest>,
) -> Json<CreateEmbeddingResponse> {
    let data = req
        .input
        .iter()
        // .map(|text| function(text))
        .enumerate()
        .map(|(i, t)| Embedding {
            index: i as u32,
            object: "embedding".into(),
            embedding: function(t),
        })
        .collect();

    Json(CreateEmbeddingResponse {
        object: "list".into(),
        data,
        model: "".to_owned(),
        usage: EmbeddingUsage {
            prompt_tokens: 0,
            total_tokens: 0,
        },
    })
}

/// Runs the embedding server on the given port based on the provided function. The address can be for instance "0.0.0.0:3000"
pub async fn serve_custom_embedding<F>(address: &str, function: F)
where
    F: Fn(&str) -> Vec<f32> + Send + Sync + 'static,
{
    let state = Arc::new(function);
    let app = Router::new()
        .route("/v1/embeddings", post(embeddings))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    axum::serve(listener, app).await.unwrap()
}
