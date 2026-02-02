use crate::error::StorageError;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::Path;

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20
pub const CONFIG_FILE: &str = "config.json";

pub trait ConfigOps: Serialize + DeserializeOwned {
    fn max_node_page_len(&self) -> u32;

    fn max_edge_page_len(&self) -> u32;

    fn node_types(&self) -> &[String];

    fn with_node_types(&self, node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self;

    fn load_from_dir(dir: &Path) -> Result<Self, StorageError> {
        let config_file = dir.join(CONFIG_FILE);
        let config_file = std::fs::File::open(config_file)?;
        let config = serde_json::from_reader(config_file)?;
        Ok(config)
    }

    fn save_to_dir(&self, dir: &Path) -> Result<(), StorageError> {
        let config_file = dir.join(CONFIG_FILE);
        let config_file = std::fs::File::create(&config_file)?;
        serde_json::to_writer_pretty(config_file, self)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BaseConfig {
    max_node_page_len: u32,
    max_edge_page_len: u32,
}

impl BaseConfig {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        Self {
            max_node_page_len,
            max_edge_page_len,
        }
    }
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self {
            max_node_page_len: DEFAULT_MAX_PAGE_LEN_NODES,
            max_edge_page_len: DEFAULT_MAX_PAGE_LEN_EDGES,
        }
    }
}

impl ConfigOps for BaseConfig {
    fn max_node_page_len(&self) -> u32 {
        self.max_node_page_len
    }

    fn max_edge_page_len(&self) -> u32 {
        self.max_edge_page_len
    }

    fn node_types(&self) -> &[String] {
        &[]
    }

    fn with_node_types(&self, _node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        *self
    }

    fn load_from_dir(_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self::default())
    }

    fn save_to_dir(&self, _dir: &Path) -> Result<(), StorageError> {
        Ok(())
    }
}
