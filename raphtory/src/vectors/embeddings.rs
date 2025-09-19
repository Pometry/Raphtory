use crate::{
    errors::GraphResult,
    vectors::{cache::CachedEmbeddings, storage::OpenAIEmbeddings, Embedding},
};
use async_openai::{
    types::{CreateEmbeddingRequest, EmbeddingInput},
    Client,
};
use futures_util::{future::BoxFuture, Stream, StreamExt};
use serde::{de::Error as _, ser::Error as _, Deserialize, Serialize, Serializer};
use std::{future::Future, pin::Pin, sync::Arc};

const CONTENT_SAMPLE: &str = "raphtory"; // DON'T CHANGE THIS STRING BY ANY MEANS
const CHUNK_SIZE: usize = 1000;

pub(crate) type EmbeddingError = Box<dyn std::error::Error + Send + Sync>;
pub type EmbeddingResult<T> = Result<T, EmbeddingError>;

trait CustomModel: Send + Sync {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>>;
}

impl<T, F> CustomModel for T
where
    T: Fn(Vec<String>) -> F + Send + Sync,
    F: Future<Output = EmbeddingResult<Vec<Embedding>>> + Send + 'static,
{
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        Box::pin(self(texts))
    }
}

#[derive(Clone)]
pub struct CustomEmbeddingModel(Arc<dyn CustomModel>);

impl Serialize for CustomEmbeddingModel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let msg = "custom embedding model cannot be serialized";
        Err(S::Error::custom(msg))
        // serializer.serialize_none()
    }
}

impl<'de> Deserialize<'de> for CustomEmbeddingModel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let msg = "custom embedding model cannot be deserialized";
        Err(D::Error::custom(msg))
    }
}

impl std::fmt::Debug for CustomEmbeddingModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CustomEmbeddingModel")
            .field(&"<hidden>".to_owned())
            .finish()
    }
}

impl PartialEq for CustomEmbeddingModel {
    fn eq(&self, _other: &Self) -> bool {
        // two custom embedding models are always considered different for safety
        false
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum EmbeddingModel {
    Custom(CustomEmbeddingModel),
    OpenAI(OpenAIEmbeddings),
}

impl EmbeddingModel {
    pub fn custom<T, F>(function: T) -> Self
    where
        T: Fn(Vec<String>) -> F + Send + Sync + 'static,
        F: Future<Output = EmbeddingResult<Vec<Embedding>>> + Send + 'static,
    {
        Self::Custom(CustomEmbeddingModel(Arc::new(function)))
    }

    pub(super) async fn sampled(self) -> GraphResult<SampledModel> {
        let sample = self.generate_sample().await?;
        Ok(SampledModel {
            model: self,
            sample,
        })
    }

    pub(super) async fn generate_sample(&self) -> GraphResult<Embedding> {
        let mut vectors = self.call(vec![CONTENT_SAMPLE.to_owned()]).await?;
        Ok(vectors.remove(0))
    }

    // note that the EmbeddingModel is not usable until it gets sampled because this is private.
    // This is to make sure that all models are sampled before they get stored by definition
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        match self {
            EmbeddingModel::Custom(function) => function.0.call(texts),
            EmbeddingModel::OpenAI(embeddings) => embeddings.call(texts),
        }
    }
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub(super) struct SampledModel {
    pub(super) model: EmbeddingModel,
    pub(super) sample: Embedding,
}

impl SampledModel {
    pub(super) fn call(
        &self,
        texts: Vec<String>,
    ) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        self.model.call(texts)
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
            let response = client.embeddings().create(request).await?;
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
    model: &'a CachedEmbeddings,
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

#[cfg(test)]
mod embedding_tests {
    use crate::vectors::embeddings::CONTENT_SAMPLE;

    #[test]
    fn test_vector_sample_remains_unchanged() {
        assert_eq!(CONTENT_SAMPLE, "raphtory");
    }
}
