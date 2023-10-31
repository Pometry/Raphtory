use crate::vectors::{entity_id::EntityId, Embedding};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fs::{create_dir_all, File},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

type CacheKey = (EntityId, u64);
type CacheStore = HashMap<CacheKey, Vec<Embedding>>;
pub(crate) struct EmbeddingCache {
    cache: RwLock<CacheStore>,
    path: PathBuf,
}

impl EmbeddingCache {
    pub(crate) fn from_path(path: PathBuf) -> Self {
        let inner_cache = Self::try_reading_from_disk(&path).unwrap_or(HashMap::new());
        let cache = RwLock::new(inner_cache);
        Self { cache, path }
    }

    fn try_reading_from_disk(path: &PathBuf) -> Option<CacheStore> {
        let file = File::open(&path).ok()?;
        let mut reader = BufReader::new(file);
        bincode::deserialize_from(&mut reader).ok()
    }

    pub(crate) fn get_embeddings(&self, id: EntityId, hash: u64) -> Option<Vec<Embedding>> {
        let cache_key = (id, hash);
        self.cache.read().get(&cache_key).cloned()
    }

    pub(crate) fn upsert_embeddings(&self, id: EntityId, hash: u64, embeddings: Vec<Embedding>) {
        let cache_key = (id, hash);
        self.cache.write().insert(cache_key, embeddings);
    }

    // TODO: remove entries that weren't read in the last usage
    pub(crate) fn dump_to_disk(&self) {
        self.path.parent().iter().for_each(|parent_path| {
            create_dir_all(parent_path).expect("Impossible to use cache dir");
        });
        // TODO: print helpful error if the path is a directory, maybe when creating the cache
        // instead of here to save the embedding model to be called
        let file = File::create(&self.path).expect("Couldn't create file to store embedding cache");
        let mut writer = BufWriter::new(file);
        bincode::serialize_into::<_, CacheStore>(&mut writer, &self.cache.read())
            .expect("Couldn't serialize embedding cache");
    }
}
