use async_openai::types::{CreateEmbeddingResponse, Embedding, EmbeddingUsage};
use axum::{
    extract::{Json, State},
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use serde::Deserialize;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};
use tokio::{signal, sync::mpsc, task::JoinHandle};

#[derive(Deserialize, Debug)]
struct EmbeddingRequest {
    input: Vec<String>,
}

async fn embeddings(
    State(function): State<Arc<dyn EmbeddingFunction + Send + Sync>>,
    Json(req): Json<EmbeddingRequest>,
) -> Result<Json<CreateEmbeddingResponse>, (StatusCode, String)> {
    let data = req
        .input
        .iter()
        .enumerate()
        .map(|(i, t)| {
            catch_unwind(AssertUnwindSafe(|| function.call(t)))
                .map(|embedding| Embedding {
                    index: i as u32,
                    object: "embedding".into(),
                    embedding,
                })
                .map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "embedding function panicked".to_owned(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(CreateEmbeddingResponse {
        object: "list".into(),
        data,
        model: "".to_owned(),
        usage: EmbeddingUsage {
            prompt_tokens: 0,
            total_tokens: 0,
        },
    }))
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
    let app = Router::new()
        .route("/embeddings", post(embeddings)) // TODO: this should be /v1/embeddings if we were to support multiple versions
        .with_state(state);
    // since the listener is created at this point, when this function returns the server is already available,
    // might just take some time to answer for the first time, but no requests should be rejected
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    let execution = tokio::spawn(async {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                #[cfg(unix)]
                let terminate = async {
                    signal::unix::signal(signal::unix::SignalKind::terminate())
                        .expect("failed to install signal handler")
                        .recv()
                        .await;
                };
                #[cfg(not(unix))]
                let terminate = std::future::pending::<()>();

                tokio::select! {
                    _ = terminate => {},
                    _ = signal::ctrl_c() => {},
                    _ = receiver.recv() => {},
                }
            })
            .await
            .unwrap();
    });
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
