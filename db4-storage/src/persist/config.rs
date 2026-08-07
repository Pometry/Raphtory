pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 600_000; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 6_000_000; // 2^20
pub const CONFIG_FILE_NAME: &str = "config.json";

use crate::{
    error::StorageError,
    persist::args::{ArgsOps, BaseArgs},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::Path;
use tempfile::NamedTempFile;

/// Trait for graph storage configuration.
///
/// `Config` is the resolved configuration used internally and persisted to
/// `config.json`. User-facing overrides are supplied via [`ArgsOps`].
pub trait ConfigOps: Serialize + DeserializeOwned + Sized + Clone {
    type Args: ArgsOps<Config = Self> + From<Self>;

    fn load_from_dir(dir: &Path) -> Result<Self, StorageError> {
        let config_file = dir.join(CONFIG_FILE_NAME);
        let config_file = std::fs::File::open(config_file)?;
        let config = serde_json::from_reader(config_file)?;

        Ok(config)
    }

    fn save_to_dir(&self, dir: &Path) -> Result<(), StorageError> {
        let config_path = dir.join(CONFIG_FILE_NAME);
        let mut tmp_file = NamedTempFile::new_in(dir)?;

        serde_json::to_writer_pretty(&mut tmp_file, self)?;
        tmp_file.as_file().sync_all()?;
        tmp_file
            .persist(&config_path)
            .map_err(std::io::Error::from)?;

        Ok(())
    }

    fn max_node_page_len(&self) -> u32;

    fn max_edge_page_len(&self) -> u32;

    fn node_types(&self) -> &[String];

    fn with_max_node_page_len(self, page_len: u32) -> Self;

    fn with_max_edge_page_len(self, page_len: u32) -> Self;

    fn with_node_types(&self, node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BaseConfig {
    max_node_page_len: u32,
    max_edge_page_len: u32,
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PAGE_LEN_NODES, DEFAULT_MAX_PAGE_LEN_EDGES)
    }
}

impl BaseConfig {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        Self {
            max_node_page_len,
            max_edge_page_len,
        }
    }
}

impl ConfigOps for BaseConfig {
    type Args = BaseArgs;

    fn max_node_page_len(&self) -> u32 {
        self.max_node_page_len
    }

    fn max_edge_page_len(&self) -> u32 {
        self.max_edge_page_len
    }

    fn with_max_node_page_len(mut self, page_len: u32) -> Self {
        self.max_node_page_len = page_len;
        self
    }

    fn with_max_edge_page_len(mut self, page_len: u32) -> Self {
        self.max_edge_page_len = page_len;
        self
    }

    fn node_types(&self) -> &[String] {
        &[]
    }

    fn with_node_types(&self, _node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use crate::persist::config::{
        BaseConfig, DEFAULT_MAX_PAGE_LEN_EDGES, DEFAULT_MAX_PAGE_LEN_NODES,
    };

    #[test_log::test]
    fn test_default() {
        let default = BaseConfig::default();
        assert_eq!(default.max_edge_page_len, DEFAULT_MAX_PAGE_LEN_EDGES);
        assert_eq!(default.max_node_page_len, DEFAULT_MAX_PAGE_LEN_NODES);
    }
}
