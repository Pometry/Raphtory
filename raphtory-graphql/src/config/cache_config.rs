use field_types::FieldName;
use serde::Deserialize;

pub const DEFAULT_CACHE_CAPACITY: u64 = 30;

#[derive(Debug, Deserialize, PartialEq, Clone, serde::Serialize, FieldName)]
pub struct CacheConfig {
    pub capacity: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CACHE_CAPACITY,
        }
    }
}
