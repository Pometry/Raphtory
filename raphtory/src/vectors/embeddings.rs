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
        // model: "text-embedding-3-large".to_owned(),
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

// this is currently commented out so we don't need to add any new dependencies
// but might be potentially useful in the future
// async fn sentence_transformers_embeddings(texts: Vec<String>) -> Vec<Embedding> {
//     info!("computing embeddings for {} texts", texts.len());
//     Python::with_gil(|py| {
//         let sentence_transformers = py.import("sentence_transformers")?;
//         let locals = [("sentence_transformers", sentence_transformers)].into_py_dict(py);
//         locals.set_item("texts", texts);
//
//         let pyarray: &PyArray2<f32> = py
//             .eval(
//                 &format!(
//                     "sentence_transformers.SentenceTransformer('thenlper/gte-small').encode(texts)"
//                 ),
//                 Some(locals),
//                 None,
//             )?
//             .extract()?;
//
//         let readonly = pyarray.readonly();
//         let chunks = readonly.as_slice().unwrap().chunks(384).into_iter();
//         let embeddings = chunks
//             .map(|chunk| chunk.iter().copied().collect_vec())
//             .collect_vec();
//
//         Ok::<Vec<Vec<f32>>, Box<dyn std::error::Error>>(embeddings)
//     })
//     .unwrap()
// }
