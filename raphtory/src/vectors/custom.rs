use async_openai::types::{CreateEmbeddingResponse, Embedding, EmbeddingUsage};
use axum::{
    extract::{Json, State},
    routing::post,
    Router,
};
use serde::Deserialize;
use std::{
    future::{Future, IntoFuture},
    ops::Deref,
    pin::Pin,
    sync::Arc,
};
use tokio::{sync::mpsc, task::JoinHandle};

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
    execution: JoinHandle<()>,
    stop_signal: tokio::sync::mpsc::Sender<()>,
}

impl EmbeddingServer {
    pub async fn wait(self) {
        self.execution.await.unwrap();
    }

    pub async fn stop(&self) {
        self.stop_signal.send(()).await.unwrap();
    }
}

/// Runs the embedding server on the given port based on the provided function. The address can be for instance "0.0.0.0:3000"
pub async fn serve_custom_embedding(
    address: &str,
    function: impl EmbeddingFunction,
) -> EmbeddingServer {
    let state = Arc::new(function);
    dbg!();
    let app = Router::new()
        .route("/v1/embeddings", post(embeddings))
        .with_state(state);
    dbg!();
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    dbg!();
    let (sender, mut receiver) = mpsc::channel(1);
    dbg!();
    let execution = tokio::spawn(async {
        dbg!();
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                receiver.recv().await;
            })
            .await
            .unwrap();
        dbg!();
    });
    dbg!();
    EmbeddingServer {
        execution,
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
