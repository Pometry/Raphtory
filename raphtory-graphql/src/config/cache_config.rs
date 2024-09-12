use serde::Deserialize;
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct CacheConfig {
    pub capacity: u64,
    pub tti_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 30,
            tti_seconds: 900,
        }
    }
}
