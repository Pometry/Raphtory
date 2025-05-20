use crate::{core::utils::errors::GraphResult, vectors::Embedding};
use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use futures_util::StreamExt;
use std::{collections::VecDeque, ops::Deref, path::Path};

use super::embeddings::EmbeddingFunction;

pub struct VectorCache {
    cache: HybridCache<String, Embedding>,
    function: Box<dyn EmbeddingFunction>,
}

impl VectorCache {
    pub async fn in_memory(function: Box<dyn EmbeddingFunction>) -> Self {
        let cache: HybridCache<String, Embedding> = HybridCacheBuilder::new()
            .memory(1024 * 1024) // 1MB
            .storage(Engine::Large)
            .build()
            .await
            .unwrap();
        Self { cache, function }
    }

    pub async fn from_path(path: &Path, function: Box<dyn EmbeddingFunction>) -> Self {
        let cache: HybridCache<String, Embedding> = HybridCacheBuilder::new()
            .memory(1024 * 1024) // 1MB
            .storage(Engine::Large)
            .with_device_options(DirectFsDeviceOptions::new(path).with_capacity(1024 * 1024 * 1024)) // 1GB
            .build()
            .await
            .unwrap();
        Self { cache, function }
    }
    pub(super) async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> GraphResult<impl Iterator<Item = Embedding> + '_> {
        // TODO: review, turned this into a vec only to make compute_embeddings work
        let mut results: Vec<_> = futures_util::stream::iter(texts)
            .then(|text| async move {
                match self.cache.get(&text).await.unwrap() {
                    Some(cached) => (text, Some(cached.deref().clone())),
                    None => (text, None),
                }
            })
            .collect()
            .await;
        let misses: Vec<_> = results
            .iter_mut()
            .filter_map(|(text, vector)| match vector {
                Some(_) => None,
                None => Some(text.clone()),
            })
            .collect();
        let mut fresh_vectors: VecDeque<_> = self.function.call(misses).await.unwrap().into();
        let embeddings = results.into_iter().map(move |(text, vector)| match vector {
            Some(vector) => vector,
            None => {
                let vector = fresh_vectors.pop_front().unwrap();
                self.cache.insert(text, vector.clone());
                vector
            }
        });
        Ok(embeddings)
    }

    pub(super) async fn get_single(&self, text: String) -> Embedding {
        self.get_embeddings(vec![text])
            .await
            .unwrap() // FIXME: remove this unwrap
            .next()
            .unwrap()
    }
}
