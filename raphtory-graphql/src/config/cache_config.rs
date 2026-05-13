use serde::Deserialize;

pub const DEFAULT_CAPACITY: u64 = 30;

#[derive(Debug, Deserialize, PartialEq, Clone, serde::Serialize)]
pub struct CacheConfig {
    pub capacity: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CAPACITY,
        }
    }
}
