pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 600_000; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 6_000_000; // 2^20
pub const CONFIG_FILE_NAME: &str = "config.json";

use crate::persist::args::{ArgsOps, BaseArgs};

/// Trait for graph storage configuration.
pub trait ConfigOps: Sized {
    type Args: ArgsOps<Config = Self> + From<Self>;

    fn max_node_page_len(&self) -> u32;

    fn max_edge_page_len(&self) -> u32;

    fn node_types(&self) -> &[String];

    fn with_max_node_page_len(self, page_len: u32) -> Self;

    fn with_max_edge_page_len(self, page_len: u32) -> Self;

    fn with_node_types(&self, node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self;
}

#[derive(Debug, Copy, Clone)]
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
