use async_openai::types::{CreateEmbeddingResponse, Embedding, EmbeddingUsage};
use axum::{
    extract::{Json, State},
    routing::post,
    Router,
};
use serde::Deserialize;
use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::mpsc;

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
    State(function): State<Arc<dyn EmbeddingFunction + Send + Sync>>,
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
            embedding: function.call(t),
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

pub struct EmbeddingServer {
    execution: Box<dyn Future<Output = ()> + Send>,
    stop_signal: tokio::sync::mpsc::Sender<()>,
}

impl EmbeddingServer {
    pub async fn start(&self) {
        self.execution.await;
    }

    pub async fn stop(&self) {
        self.stop_signal.send(()).await
    }
}

/// Runs the embedding server on the given port based on the provided function. The address can be for instance "0.0.0.0:3000"
pub async fn serve_custom_embedding(
    address: &str,
    function: impl EmbeddingFunction,
) -> EmbeddingServer {
    let state = Arc::new(function);
    let app = Router::new()
        .route("/v1/embeddings", post(embeddings))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    let shutdown: Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>> =
        Box::pin(async move {
            // TODO: add other common signals like Ctrl+C (see server_termination in raphtory-graphql/src/server.rs)
            receiver.recv().await;
        });
    let execution = axum::serve(listener, app).with_graceful_shutdown(shutdown);

    EmbeddingServer {
        execution: Box::new(async {
            execution.await.unwrap();
        }),
        stop_signal: sender,
    }
}

pub trait EmbeddingFunction: Send + Sync + 'static {
    fn call(&self, text: &str) -> Vec<f32>;
}

impl<F: Fn(&str) -> Vec<f32> + Send + Sync + 'static> EmbeddingFunction for F {
    fn call(&self, text: &str) -> Vec<f32> {
        self(text)
    }
}
