use crate::{core::utils::errors::GraphResult, vectors::Embedding};
use futures_util::StreamExt;
use heed::{types::SerdeBincode, Database, Env, EnvOpenOptions};
use moka::sync::Cache;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    path::Path,
    sync::Arc,
};

use super::embeddings::EmbeddingFunction;

type VectorDb = Database<SerdeBincode<u64>, SerdeBincode<Embedding>>;

enum VectorStore {
    Mem(RwLock<HashMap<u64, Embedding>>),
    Disk { env: Env, db: VectorDb },
}

impl VectorStore {
    fn in_memory() -> Self {
        Self::Mem(Default::default())
    }
    fn on_disk(path: &Path) -> Self {
        std::fs::create_dir_all(path).unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1 GB
                .open(&path)
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let db: VectorDb = env.create_database(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();
        Self::Disk { env, db }
    }

    fn get(&self, key: &u64) -> Option<Embedding> {
        match self {
            VectorStore::Mem(store) => store.read().get(key).cloned(),
            VectorStore::Disk { env, db } => {
                let rtxn = env.read_txn().unwrap();
                db.get(&rtxn, key).unwrap()
            }
        }
    }

    fn insert(&self, key: u64, value: Embedding) {
        match self {
            VectorStore::Mem(store) => {
                store.write().insert(key, value);
            }
            VectorStore::Disk { env, db } => {
                let mut wtxn = env.write_txn().unwrap();
                db.put(&mut wtxn, &key, &value).unwrap();
            }
        }
    }

    fn remove(&self, key: &u64) {
        match self {
            VectorStore::Mem(store) => {
                store.write().remove(key);
            }
            VectorStore::Disk { env, db } => {
                let mut wtxn = env.write_txn().unwrap();
                db.delete(&mut wtxn, key).unwrap();
            }
        }
    }
}

#[derive(Clone)]
pub struct VectorCache {
    store: Arc<VectorStore>,
    cache: Cache<u64, ()>,
    function: Arc<dyn EmbeddingFunction>,
}

impl VectorCache {
    pub fn in_memory(function: impl EmbeddingFunction + 'static) -> Self {
        Self {
            store: VectorStore::in_memory().into(),
            cache: Cache::new(1000),
            function: Arc::new(function),
        }
    }

    pub fn on_disk(path: &Path, function: impl EmbeddingFunction + 'static) -> Self {
        let store: Arc<_> = VectorStore::on_disk(path).into();
        let cloned = store.clone();

        let cache: Cache<u64, ()> = Cache::builder()
            .max_capacity(1_000_000)
            .eviction_listener(move |key: Arc<u64>, _value: (), _cause| cloned.remove(key.as_ref()))
            .build();

        Self {
            store,
            cache,
            function: Arc::new(function),
        }
    }

    fn get(&self, text: &str) -> Option<Embedding> {
        let hash = hash(text);
        self.cache.get(&hash)?;
        self.store.get(&hash)
    }

    fn insert(&self, text: String, vector: Embedding) {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let hash = hasher.finish();
        self.store.insert(hash, vector);
        self.cache.insert(hash, ()); // FIXME: not worth keeping the string, better have the cache being u64 -> ()
    }

    pub(super) async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> GraphResult<impl Iterator<Item = Embedding> + '_> {
        // TODO: review, turned this into a vec only to make compute_embeddings work
        let mut results: Vec<_> = futures_util::stream::iter(texts)
            .then(|text| async move {
                match self.get(&text) {
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
                self.insert(text, vector.clone());
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

fn hash(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod cache_tests {
    use std::{path::PathBuf, time::Duration};

    use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};

    use crate::vectors::{embeddings::EmbeddingResult, Embedding};

    use super::VectorCache;

    async fn fake_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        Ok(texts
            .into_iter()
            .map(|_| vec![1.0, 0.0, 0.0].into())
            .collect())
    }

    async fn test_abstract_cache(cache: VectorCache) {
        let vectors: Vec<_> = cache
            .get_embeddings(vec!["a".to_owned()])
            .await
            .unwrap()
            .collect();
        assert_eq!(vectors, vec![[1.0, 0.0, 0.0].into()]);
    }

    #[tokio::test]
    async fn test_cache() {
        test_abstract_cache(VectorCache::in_memory(fake_embedding)).await;
        let path = PathBuf::from("/tmp/vector-cache-rust-test");
        test_abstract_cache(VectorCache::on_disk(&path, fake_embedding)).await;
    }

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
