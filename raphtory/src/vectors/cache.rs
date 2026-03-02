use crate::{
    errors::GraphResult,
    vectors::{
        embeddings::{EmbeddingError, ModelConfig},
        Embedding,
    },
};
use ahash::RandomState;
use futures_util::StreamExt;
use heed::{types::SerdeBincode, Database, Env, EnvOpenOptions};
use moka::future::Cache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, Hash, Hasher},
    ops::Deref,
    path::Path,
    sync::Arc,
};

const CONTENT_SAMPLE: &str = "raphtory"; // DON'T CHANGE THIS STRING BY ANY MEANS

const MAX_DISK_ITEMS: usize = 1_000_000;
const MAX_VECTOR_DIM: usize = 8960;
const MAX_TEXT_LENGTH: usize = 200_000;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CacheEntry {
    model: ModelConfig,
    text: String,
    vector: Embedding,
}
type VectorDb = Database<SerdeBincode<u64>, SerdeBincode<CacheEntry>>;

enum VectorStore {
    Mem(RwLock<HashMap<u64, CacheEntry>>),
    Disk { env: Env, db: VectorDb },
}

impl VectorStore {
    fn in_memory() -> Self {
        Self::Mem(Default::default())
    }

    fn on_disk(path: &Path) -> GraphResult<Self> {
        let _ = std::fs::create_dir_all(path);
        let page_size = 16384;
        let max_size =
            (MAX_DISK_ITEMS * (MAX_VECTOR_DIM * 4 + MAX_TEXT_LENGTH)) / page_size * page_size;

        let env = unsafe { EnvOpenOptions::new().map_size(max_size).open(path) }?;

        let rtxn = env.read_txn()?;
        let db = env
            .open_database(&rtxn, None)
            .transpose()
            .unwrap_or_else(|| {
                let mut wtxn = env.write_txn()?;
                let db = env.create_database(&mut wtxn, None);
                wtxn.commit()?;
                db
            })?;
        drop(rtxn);

        Ok(Self::Disk { env, db })
    }

    fn get_disk_keys(&self) -> GraphResult<Vec<u64>> {
        match self {
            VectorStore::Mem(_) => Ok(vec![]),
            VectorStore::Disk { env, db } => {
                let rtxn = env.read_txn()?;
                let iter = db.iter(&rtxn)?;
                let result: Result<Vec<u64>, heed::Error> =
                    iter.map(|result| result.map(|(id, _)| id)).collect();
                Ok(result?) // TODO: simplify this?, use into inside of the map?
            }
        }
    }

    fn get(&self, key: &u64) -> Option<CacheEntry> {
        match self {
            VectorStore::Mem(store) => store.read_recursive().get(key).cloned(),
            VectorStore::Disk { env, db } => {
                let rtxn = env.read_txn().ok()?;
                db.get(&rtxn, key).ok()?
            }
        }
    }

    fn insert(&self, key: u64, value: CacheEntry) {
        match self {
            VectorStore::Mem(store) => {
                store.write().insert(key, value);
            }
            VectorStore::Disk { env, db } => {
                if let Ok(mut wtxn) = env.write_txn() {
                    let _ = db.put(&mut wtxn, &key, &value);
                    let _ = wtxn.commit();
                }
            }
        }
    }

    fn remove(&self, key: &u64) {
        match self {
            VectorStore::Mem(store) => {
                store.write().remove(key);
            }
            VectorStore::Disk { env, db } => {
                // this is a bit dangerous, because if delete ops fail and insert ops succeed,
                // the cache might explode in size, but that is very unlikely to happen
                if let Ok(mut wtxn) = env.write_txn() {
                    let _ = db.delete(&mut wtxn, key);
                    let _ = wtxn.commit();
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct VectorCache {
    store: Arc<VectorStore>,
    cache: Arc<Cache<u64, ()>>,
    models: Arc<Cache<ModelConfig, ModelConfig>>, // this always lives only in memory, precisely to force resampling from different environments
}

impl VectorCache {
    pub fn in_memory() -> Self {
        Self {
            store: VectorStore::in_memory().into(),
            cache: Cache::new(10).into(),
            models: build_model_cache(),
        }
    }

    pub async fn on_disk(path: &Path) -> GraphResult<Self> {
        let store: Arc<_> = VectorStore::on_disk(path)?.into();
        let cloned = store.clone();

        let cache: Arc<Cache<u64, ()>> = Cache::builder()
            .max_capacity(MAX_DISK_ITEMS as u64)
            .eviction_listener(move |key: Arc<u64>, _value: (), _cause| cloned.remove(key.as_ref()))
            .build()
            .into();

        for key in store.get_disk_keys()? {
            cache.insert(key, ()).await;
        }

        Ok(Self {
            store,
            cache,
            models: build_model_cache(),
        })
    }

    pub async fn openai(&self, config: ModelConfig) -> GraphResult<CachedEmbeddingModel> {
        self.validate_and_set_dim(config).await
    }

    pub(super) async fn validate_and_set_dim(
        &self,
        model: ModelConfig,
    ) -> GraphResult<CachedEmbeddingModel> {
        let expected_model = self.load_model_dim(model.clone()).await?;
        Ok(CachedEmbeddingModel {
            model: expected_model,
            cache: self.clone(),
        })
    }

    async fn load_model_dim(&self, config: ModelConfig) -> GraphResult<ModelConfig> {
        let cloned_config = config.clone();
        let model = self
            .models
            .try_get_with(config, async {
                let mut vectors = cloned_config.call(vec![CONTENT_SAMPLE.to_owned()]).await?;
                let sample = vectors.remove(0);
                Ok(cloned_config.with_dimension(sample.len()))
            })
            .await
            .map_err(|error: Arc<EmbeddingError>| {
                let inner: &EmbeddingError = error.deref();
                inner.clone()
            })?;
        Ok(model)
    }

    async fn get(&self, model: &ModelConfig, text: &str) -> Option<Embedding> {
        let hash = hash(model, text);
        self.cache.get(&hash).await?;
        let entry = self.store.get(&hash)?;
        if &entry.model == model && entry.text == text {
            Some(entry.vector)
        } else {
            None
        }
    }

    async fn insert(&self, model: ModelConfig, text: String, vector: Embedding) {
        let hash = hash(&model, &text);
        let entry = CacheEntry {
            model,
            text,
            vector,
        };
        self.store.insert(hash, entry);
        self.cache.insert(hash, ()).await;
    }
}

fn build_model_cache() -> Arc<Cache<ModelConfig, ModelConfig>> {
    Cache::new(u64::MAX).into()
}

#[derive(Clone)]
pub struct CachedEmbeddingModel {
    cache: VectorCache,
    pub(super) model: ModelConfig,
}

impl CachedEmbeddingModel {
    pub fn dim(&self) -> Option<usize> {
        self.model.dim()
    }

    pub(super) async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> GraphResult<impl Iterator<Item = Embedding> + '_> {
        // TODO: review, turned this into a vec only to make compute_embeddings work
        let results: Vec<_> = futures_util::stream::iter(texts)
            .then(|text| async move {
                match self.cache.get(&self.model, &text).await {
                    Some(cached) => (text, Some(cached)),
                    None => (text, None),
                }
            })
            .collect()
            .await;
        let misses: Vec<_> = results
            .iter()
            .filter_map(|(text, vector)| match vector {
                Some(_) => None,
                None => Some(text.clone()),
            })
            .collect();
        let mut fresh_vectors: VecDeque<_> = if !misses.is_empty() {
            self.model.call(misses.clone()).await?.into()
        } else {
            vec![].into()
        };
        futures_util::stream::iter(misses.into_iter().zip(fresh_vectors.iter().cloned()))
            .for_each(|(text, vector)| self.cache.insert(self.model.clone(), text, vector))
            .await;
        let embeddings = results.into_iter().map(move |(_, vector)| match vector {
            Some(vector) => vector,
            None => fresh_vectors.pop_front().unwrap(),
        });
        Ok(embeddings)
    }

    pub(super) async fn get_single(&self, text: String) -> GraphResult<Embedding> {
        let mut embeddings = self.get_embeddings(vec![text]).await?;
        Ok(embeddings.next().unwrap())
    }
}

fn hash(model: &ModelConfig, text: &str) -> u64 {
    let hasher = RandomState::with_seeds(
        2576675592427417589,
        14681663747860293331,
        5162080899205198708,
        4782991468701587167,
    );
    let mut state = hasher.build_hasher();
    model.hash(&mut state);
    text.hash(&mut state);
    state.finish()
}

#[cfg(test)]
mod cache_tests {
    use once_cell::sync::Lazy;
    use tempfile::tempdir;

    use crate::vectors::{
        cache::{CachedEmbeddingModel, CONTENT_SAMPLE},
        embeddings::ModelConfig,
        storage::OpenAIEmbeddings,
        Embedding,
    };

    use super::VectorCache;

    fn placeholder_config() -> OpenAIEmbeddings {
        OpenAIEmbeddings::empty("whatever")
    }

    fn other_config() -> OpenAIEmbeddings {
        OpenAIEmbeddings::empty("other")
    }

    static PLACEHOLDER_MODEL: Lazy<ModelConfig> =
        Lazy::new(|| ModelConfig::OpenAI(placeholder_config()));

    #[test]
    fn stable_hash() {
        let hash_value = super::hash(&PLACEHOLDER_MODEL, CONTENT_SAMPLE);
        assert_eq!(hash_value, 17143601129976616271);
    }

    #[test]
    fn test_vector_sample_remains_unchanged() {
        assert_eq!(CONTENT_SAMPLE, "raphtory");
    }

    #[tokio::test]
    async fn test_empty_request() {
        let model = CachedEmbeddingModel {
            cache: VectorCache::in_memory(),
            // this model will definetely error out if called, as the api base is invalid
            model: ModelConfig::OpenAI(OpenAIEmbeddings::new("whatever", "invalid-api-base")),
        };
        let result: Vec<_> = model.get_embeddings(vec![]).await.unwrap().collect();
        assert_eq!(result, vec![]);
    }

    async fn test_abstract_cache(cache: VectorCache) {
        let vector_a: Embedding = [1.0].into();
        let vector_a_alt: Embedding = [1.0, 0.0].into();
        let vector_b: Embedding = [0.5].into();

        // TOOD: try to do this using VectorCache::in_memory().openai()
        let model_a = ModelConfig::OpenAI(placeholder_config());
        let model_b = ModelConfig::OpenAI(other_config());

        assert_eq!(cache.get(&model_a, "a").await, None);
        assert_eq!(cache.get(&model_b, "a").await, None);
        assert_eq!(cache.get(&model_a, "b").await, None);

        cache
            .insert(model_a.clone(), "a".to_owned(), vector_a.clone())
            .await;
        assert_eq!(cache.get(&model_a, "a").await, Some(vector_a.clone()));
        assert_eq!(cache.get(&model_b, "a").await, None);
        assert_eq!(cache.get(&model_a, "b").await, None);

        cache
            .insert(model_b.clone(), "a".to_owned(), vector_a_alt.clone())
            .await;
        assert_eq!(cache.get(&model_a, "a").await, Some(vector_a.clone()));
        assert_eq!(cache.get(&model_b, "a").await, Some(vector_a_alt.clone()));
        assert_eq!(cache.get(&model_a, "b").await, None);

        cache
            .insert(model_a.clone(), "b".to_owned(), vector_b.clone())
            .await;
        assert_eq!(cache.get(&model_a, "a").await, Some(vector_a));
        assert_eq!(cache.get(&model_b, "a").await, Some(vector_a_alt));
        assert_eq!(cache.get(&model_a, "b").await, Some(vector_b));
    }

    #[tokio::test]
    async fn test_in_memory_cache() {
        let cache = VectorCache::in_memory();
        test_abstract_cache(cache).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache() {
        let dir = tempdir().unwrap();
        test_abstract_cache(VectorCache::on_disk(dir.path()).await.unwrap()).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache_loading() {
        let model = ModelConfig::OpenAI(placeholder_config());
        let vector: Embedding = [1.0].into();
        let dir = tempdir().unwrap();

        {
            let cache = VectorCache::on_disk(dir.path()).await.unwrap();
            cache
                .insert(model.clone(), "a".to_owned(), vector.clone())
                .await;
        } // here the heed env gets dropped, maybe we should find some key value store that doesn't need us to do this

        let loaded_from_disk = VectorCache::on_disk(dir.path()).await.unwrap();
        assert_eq!(loaded_from_disk.get(&model, "a").await, Some(vector))
    }
}
