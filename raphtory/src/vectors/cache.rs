use super::embeddings::EmbeddingFunction;
use crate::{errors::GraphResult, vectors::Embedding};
use futures_util::StreamExt;
use heed::{types::SerdeBincode, Database, Env, EnvOpenOptions};
use moka::sync::Cache;
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
    key: String,
    value: Embedding,
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

        let env = unsafe { EnvOpenOptions::new().map_size(max_size).open(&path) }?;
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
            cache: Cache::new(10),
            function: Arc::new(function),
        }
    }

    pub fn on_disk(path: &Path, function: impl EmbeddingFunction + 'static) -> GraphResult<Self> {
        let store: Arc<_> = VectorStore::on_disk(path)?.into();
        let cloned = store.clone();

        let cache: Cache<u64, ()> = Cache::builder()
            .max_capacity(MAX_DISK_ITEMS as u64)
            .eviction_listener(move |key: Arc<u64>, _value: (), _cause| cloned.remove(key.as_ref()))
            .build();

        for key in store.get_disk_keys()? {
            cache.insert(key, ());
        }

        Ok(Self {
            store,
            cache,
            function: Arc::new(function),
        })
    }

    fn get(&self, text: &str) -> Option<Embedding> {
        let hash = hash(text);
        self.cache.get(&hash)?;
        let entry = self.store.get(&hash)?;
        if &entry.key == text {
            Some(entry.value)
        } else {
            None
        }
    }

    fn insert(&self, text: String, vector: Embedding) {
        let hash = hash(&text);
        let entry = CacheEntry {
            key: text,
            value: vector,
        };
        self.store.insert(hash, entry);
        self.cache.insert(hash, ());
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
        let mut fresh_vectors: VecDeque<_> = if misses.len() > 0 {
            self.function.call(misses).await?.into()
        } else {
            vec![].into()
        };
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

    pub async fn get_single(&self, text: String) -> GraphResult<Embedding> {
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
    use tempfile::tempdir;

    use crate::vectors::{embeddings::EmbeddingResult, Embedding};

    use super::VectorCache;

    async fn placeholder_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        dbg!(texts);
        todo!()
    }

    async fn test_abstract_cache(cache: VectorCache) {
        let vector_a: Embedding = [1.0].into();
        let vector_b: Embedding = [0.5].into();

        assert_eq!(cache.get("a"), None);
        assert_eq!(cache.get("b"), None);

        cache.insert("a".to_owned(), vector_a.clone());
        assert_eq!(cache.get("a"), Some(vector_a.clone()));
        assert_eq!(cache.get("b"), None);

        cache.insert("b".to_owned(), vector_b.clone());
        assert_eq!(cache.get("a"), Some(vector_a));
        assert_eq!(cache.get("b"), Some(vector_b));
    }

    #[tokio::test]
    async fn test_empty_request() {
        let cache = VectorCache::in_memory(placeholder_embedding);
        let result: Vec<_> = cache.get_embeddings(vec![]).await.unwrap().collect();
        assert_eq!(result, vec![]);
    }

    #[tokio::test]
    async fn test_cache() {
        test_abstract_cache(VectorCache::in_memory(placeholder_embedding)).await;
        let dir = tempdir().unwrap();
        test_abstract_cache(VectorCache::on_disk(dir.path(), placeholder_embedding).unwrap()).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache() {
        let vector: Embedding = [1.0].into();
        let dir = tempdir().unwrap();

        {
            let cache = VectorCache::on_disk(dir.path(), placeholder_embedding).unwrap();
            cache.insert("a".to_owned(), vector.clone());
        } // here the heed env gets closed

        let loaded_from_disk = VectorCache::on_disk(dir.path(), placeholder_embedding).unwrap();
        assert_eq!(loaded_from_disk.get("a"), Some(vector))
    }
}
