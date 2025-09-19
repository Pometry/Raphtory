use crate::{
    errors::GraphResult,
    vectors::{embeddings::SampledModel, Embedding},
};
use futures_util::StreamExt;
use heed::{types::SerdeBincode, Database, Env, EnvOpenOptions};
use itertools::Itertools;
use moka::future::Cache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    path::Path,
    sync::Arc,
};

const MAX_DISK_ITEMS: usize = 1_000_000;
const MAX_VECTOR_DIM: usize = 8960;
const MAX_TEXT_LENGTH: usize = 200_000;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CacheEntry {
    model: usize,
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
        let mut wtxn = env.write_txn().unwrap();
        let db: VectorDb = env.create_database(&mut wtxn, None)?;
        wtxn.commit()?;
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
            VectorStore::Mem(store) => store.read().get(key).cloned(),
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

// #[derive(Clone)]
pub struct VectorCache {
    store: Arc<VectorStore>,
    cache: Cache<u64, ()>,
    models: RwLock<Vec<SampledModel>>,
}

impl VectorCache {
    // FIXME: this doesnt need to be async anymore
    pub fn in_memory() -> Arc<Self> {
        Self {
            store: VectorStore::in_memory().into(),
            cache: Cache::new(10),
            models: Default::default(),
        }
        .into()
    }

    pub async fn on_disk(path: &Path) -> GraphResult<Self> {
        let store: Arc<_> = VectorStore::on_disk(path)?.into();
        let cloned = store.clone();

        let cache: Cache<u64, ()> = Cache::builder()
            .max_capacity(MAX_DISK_ITEMS as u64)
            .eviction_listener(move |key: Arc<u64>, _value: (), _cause| cloned.remove(key.as_ref()))
            .build();

        for key in store.get_disk_keys()? {
            cache.insert(key, ()).await;
        }

        Ok(Self {
            store,
            cache,
            models: Default::default(),
        })
    }

    async fn get(&self, model: usize, text: &str) -> Option<Embedding> {
        let hash = hash(model, text);
        self.cache.get(&hash).await?;
        let entry = self.store.get(&hash)?;
        if entry.model == model && entry.text == text {
            Some(entry.vector)
        } else {
            None
        }
    }

    async fn insert(&self, model: usize, text: String, vector: Embedding) {
        let hash = hash(model, &text);
        let entry = CacheEntry {
            model,
            text,
            vector,
        };
        self.store.insert(hash, entry);
        self.cache.insert(hash, ()).await;
    }
}

pub trait EmbeddingCacher {
    async fn cache_model(&self, model: SampledModel) -> GraphResult<CachedEmbeddings>;
}

impl EmbeddingCacher for Arc<VectorCache> {
    async fn cache_model(&self, model: SampledModel) -> GraphResult<CachedEmbeddings> {
        let mut models = self.models.write();
        let maybe_id = models.iter().find_position(|f| &&model == f);
        let id = if let Some((id, _)) = maybe_id {
            id
        } else {
            models.push(model.clone());
            models.iter().find_position(|f| &&model == f).unwrap().0
        };

        let cache = self.clone();
        Ok(CachedEmbeddings { cache, id, model })
    }
}

#[derive(Clone)]
pub struct CachedEmbeddings {
    cache: Arc<VectorCache>, // TODO: review if ok using here a parking_lot::RwLock
    id: usize,
    pub(super) model: SampledModel, // this is kind of duplicated, but enables skipping the rwlock
}

impl CachedEmbeddings {
    pub(super) async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> GraphResult<impl Iterator<Item = Embedding> + '_> {
        // TODO: review, turned this into a vec only to make compute_embeddings work
        let results: Vec<_> = futures_util::stream::iter(texts)
            .then(|text| async move {
                match self.cache.get(self.id, &text).await {
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
            .for_each(|(text, vector)| self.cache.insert(self.id, text, vector))
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

    pub(super) fn get_sample(&self) -> &Embedding {
        &self.model.sample
    }
}

// const CONTENT_SAMPLE: &str = "raphtory"; // DON'T CHANGE THIS STRING BY ANY MEANS

// async fn get_vector_sample(function: &impl EmbeddingFunction) -> GraphResult<Embedding> {
//     let mut vectors = function.call(vec![CONTENT_SAMPLE.to_owned()]).await?;
//     Ok(vectors.remove(0))
// }

fn hash(model: usize, text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    model.hash(&mut hasher);
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod cache_tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::vectors::{
        cache::EmbeddingCacher,
        embeddings::{EmbeddingModel, EmbeddingResult},
        Embedding,
    };

    use super::VectorCache;

    async fn panicking_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        panic!("panicking_embedding was called");
    }

    #[tokio::test]
    async fn test_empty_request() {
        let cache = VectorCache::in_memory();
        let model = cache
            .cache_model(
                EmbeddingModel::custom(panicking_embedding)
                    .sampled()
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        let result: Vec<_> = model.get_embeddings(vec![]).await.unwrap().collect();
        assert_eq!(result, vec![]);
    }

    async fn test_abstract_cache(cache: Arc<VectorCache>) {
        let vector_a: Embedding = [1.0].into();
        let vector_a_alt: Embedding = [1.0, 0.0].into();
        let vector_b: Embedding = [0.5].into();

        assert_eq!(cache.get(0, "a").await, None);
        assert_eq!(cache.get(1, "a").await, None);
        assert_eq!(cache.get(0, "b").await, None);

        cache.insert(0, "a".to_owned(), vector_a.clone()).await;
        assert_eq!(cache.get(0, "a").await, Some(vector_a.clone()));
        assert_eq!(cache.get(1, "a").await, None);
        assert_eq!(cache.get(0, "b").await, None);

        cache.insert(1, "a".to_owned(), vector_a_alt.clone()).await;
        assert_eq!(cache.get(0, "a").await, Some(vector_a.clone()));
        assert_eq!(cache.get(1, "a").await, Some(vector_a_alt.clone()));
        assert_eq!(cache.get(0, "b").await, None);

        cache.insert(0, "b".to_owned(), vector_b.clone()).await;
        assert_eq!(cache.get(0, "a").await, Some(vector_a));
        assert_eq!(cache.get(1, "a").await, Some(vector_a_alt));
        assert_eq!(cache.get(0, "b").await, Some(vector_b));
    }

    #[tokio::test]
    async fn test_in_memory_cache() {
        let cache = VectorCache::in_memory();
        test_abstract_cache(cache).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache() {
        let dir = tempdir().unwrap();
        test_abstract_cache(VectorCache::on_disk(dir.path()).await.unwrap().into()).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache_loading() {
        let vector: Embedding = [1.0].into();
        let dir = tempdir().unwrap();

        let cache = VectorCache::on_disk(dir.path()).await.unwrap();
        cache.insert(0, "a".to_owned(), vector.clone()).await;

        let loaded_from_disk = VectorCache::on_disk(dir.path()).await.unwrap();
        assert_eq!(loaded_from_disk.get(0, "a").await, Some(vector))
    }
}
