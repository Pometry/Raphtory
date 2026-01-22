use crate::error::StorageError;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 32 * 1024 * 1024;

pub trait ConfigOps: Serialize + Deserialize<'static> {
    fn persistence(&self) -> &PersistenceConfig;

    fn load_from_dir(dir: impl AsRef<Path>) -> Result<Self, StorageError>;
    fn save_to_dir(&self, dir: impl AsRef<Path>) -> Result<(), StorageError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    max_node_page_len: u32,
    max_edge_page_len: u32,
    max_memory_bytes: usize,
    bg_flush_enabled: bool,
    node_types: Vec<String>,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            max_node_page_len: DEFAULT_MAX_PAGE_LEN_NODES,
            max_edge_page_len: DEFAULT_MAX_PAGE_LEN_EDGES,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            bg_flush_enabled: true,
            node_types: Vec::new(),
        }
    }
}

impl PersistenceConfig {
    const CONFIG_FILE: &str = "persistence_config.json";

    pub fn new_with_memory(max_memory_bytes: usize) -> Self {
        Self {
            max_memory_bytes,
            ..Default::default()
        }
    }

    pub fn new_with_page_lens(
        max_memory_bytes: usize,
        max_node_page_len: u32,
        max_edge_page_len: u32,
    ) -> Self {
        Self {
            max_memory_bytes,
            max_node_page_len,
            max_edge_page_len,
            ..Default::default()
        }
    }

    pub fn load_from_dir(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let config_file = dir.as_ref().join(Self::CONFIG_FILE);
        let config_file = std::fs::File::open(config_file)?;
        let config = serde_json::from_reader(config_file)?;
        Ok(config)
    }

    pub fn save_to_dir(&self, dir: impl AsRef<Path>) -> Result<(), StorageError> {
        let config_file = dir.as_ref().join(Self::CONFIG_FILE);
        let config_file = std::fs::File::create(&config_file)?;
        serde_json::to_writer_pretty(config_file, self)?;
        Ok(())
    }

    pub fn with_bg_flush(mut self) -> Self {
        self.bg_flush_enabled = true;
        self
    }

    pub fn with_node_types(&self, types: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let node_types = types.into_iter().map(|s| s.as_ref().to_string()).collect();

        Self {
            node_types,
            ..*self
        }
    }

    pub fn max_node_page_len(&self) -> u32 {
        self.max_node_page_len
    }

    pub fn max_edge_page_len(&self) -> u32 {
        self.max_edge_page_len
    }

    pub fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }

    pub fn bg_flush_enabled(&self) -> bool {
        self.bg_flush_enabled
    }

    pub fn node_types(&self) -> &[String] {
        &self.node_types
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoOpConfig {
    persistence: PersistenceConfig,
}

impl NoOpConfig {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        let persistence = PersistenceConfig {
            max_node_page_len,
            max_edge_page_len,
            max_memory_bytes: usize::MAX,
            bg_flush_enabled: false,
            node_types: Vec::new(),
        };

        Self { persistence }
    }
}

impl Default for NoOpConfig {
    fn default() -> Self {
        let persistence = PersistenceConfig {
            max_node_page_len: DEFAULT_MAX_PAGE_LEN_NODES,
            max_edge_page_len: DEFAULT_MAX_PAGE_LEN_EDGES,
            max_memory_bytes: usize::MAX,
            bg_flush_enabled: false,
            node_types: Vec::new(),
        };

        Self { persistence }
    }
}

impl ConfigOps for NoOpConfig {
    fn persistence(&self) -> &PersistenceConfig {
        &self.persistence
    }

    fn load_from_dir(_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self::default())
    }

    fn save_to_dir(&self, _dir: impl AsRef<Path>) -> Result<(), StorageError> {
        Ok(())
    }
}
