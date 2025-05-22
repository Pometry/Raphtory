use crate::{core::utils::errors::GraphResult, vectors::Embedding};
use foyer::{Cache, CacheBuilder, DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use futures_util::StreamExt;
use std::{collections::VecDeque, ops::Deref, path::Path, sync::Arc};

use super::embeddings::EmbeddingFunction;

#[derive(Clone)]
enum MemDiskCache {
    Mem(Cache<String, Embedding>),
    Disk(HybridCache<String, Embedding>),
}

impl MemDiskCache {
    async fn get(&self, key: &String) -> Option<Embedding> {
        match self {
            Self::Mem(cache) => Some(cache.get(key)?.deref().clone()),
            Self::Disk(cache) => Some(cache.get(key).await.ok().flatten()?.deref().clone()),
        }
    }

    fn insert(&self, key: String, value: Embedding) {
        match self {
            Self::Mem(cache) => {
                cache.insert(key, value);
            }
            Self::Disk(cache) => {
                cache.insert(key, value);
            }
        }
    }
}

#[derive(Clone)]
pub struct VectorCache {
    cache: MemDiskCache,
    function: Arc<dyn EmbeddingFunction>,
}

impl VectorCache {
    pub async fn in_memory(function: impl EmbeddingFunction + 'static) -> Self {
        let cache = CacheBuilder::new(100).build();
        Self {
            cache: MemDiskCache::Mem(cache),
            function: Arc::new(function),
        }
    }

    pub async fn from_path(path: &Path, function: impl EmbeddingFunction + 'static) -> Self {
        let cache: HybridCache<String, Embedding> = HybridCacheBuilder::new()
            .memory(1024 * 1024) // 1MB
            .storage(Engine::Large)
            .with_device_options(DirectFsDeviceOptions::new(path).with_capacity(1024 * 1024 * 1024)) // 1GB
            .with_flush(true)
            .build()
            .await
            .unwrap();
        Self {
            cache: MemDiskCache::Disk(cache),
            function: Arc::new(function),
        }
    }

    pub(super) async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> GraphResult<impl Iterator<Item = Embedding> + '_> {
        // TODO: review, turned this into a vec only to make compute_embeddings work
        let mut results: Vec<_> = futures_util::stream::iter(texts)
            .then(|text| async move {
                match self.cache.get(&text).await {
                    Some(cached) => (text, Some(cached)),
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

    pub(crate) async fn get_single(&self, text: String) -> GraphResult<Embedding> {
        let mut embeddings = self.get_embeddings(vec![text]).await?;
        Ok(embeddings.next().unwrap())
    }
}

#[cfg(test)]
mod cache_tests {
    use std::{ops::Deref, path::PathBuf, time::Duration};

    use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};

    use crate::vectors::{cache::MemDiskCache, embeddings::EmbeddingResult, Embedding};

    use super::VectorCache;

    async fn fake_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        Ok(texts
            .into_iter()
            .map(|_| vec![1.0, 0.0, 0.0].into())
            .collect())
    }

    // #[tokio::test]
    // async fn test_foyer() {
    //     let path = PathBuf::from("/tmp/foyer-test");
    //     {
    //         let cache = VectorCache::from_path(&path, fake_embedding).await;
    //         cache.cache.insert("hello".to_owned(), [1.0].into());
    //         if let MemDiskCache::Disk(cache) = &cache.cache {
    //             dbg!();
    //             cache.storage_writer("hello".to_owned()).force();
    //         }
    //         let vector = cache.cache.get(&"hello".to_owned()).await.unwrap();
    //         assert_eq!(vector, [1.0].into());
    //     }

    //     let cache = VectorCache::from_path(&path, fake_embedding).await;
    //     let vector = cache.cache.get(&"hello".to_owned()).await.unwrap();
    //     assert_eq!(vector, [1.0].into());
    // }

    #[tokio::test]
    async fn test_foyer() {
        let path = PathBuf::from("/tmp/foyer-test");

        let cache: HybridCache<String, Embedding> = HybridCacheBuilder::new()
            .memory(1024) // 1MB
            .storage(Engine::Small)
            .with_device_options(
                DirectFsDeviceOptions::new(&path).with_capacity(10 * 1024 * 1024 * 1024),
            ) // 1GB
            .with_flush(true)
            .build()
            .await
            .unwrap();

        for i in 0..1000 {
            cache.insert(i.to_string(), [i as f32].into());
        }

        tokio::time::sleep(Duration::from_secs(15)).await;

        for i in 0..1000 {
            dbg!(i);
            let value = cache.get(&i.to_string()).await.unwrap().unwrap();
            assert_eq!(value[0], i as f32);
        }

        let cache: HybridCache<String, Embedding> = HybridCacheBuilder::new()
            .memory(1024) // 1MB
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(&path).with_capacity(1024 * 1024 * 1024),
            ) // 1GB
            .with_flush(true)
            .build()
            .await
            .unwrap();

        for i in 0..1_000 {
            if let Some(value) = cache.get(&i.to_string()).await.unwrap() {
                panic!("{value:?}");
            }
        }
    }
}
