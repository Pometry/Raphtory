use serde::Deserialize;

pub const DEFAULT_CAPACITY: u64 = 30;
pub const DEFAULT_TTI_SECONDS: u64 = 900;

#[derive(Debug, Deserialize, Clone, serde::Serialize)]
pub struct CacheConfig {
    pub capacity: u64,
    pub tti_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CAPACITY,
            tti_seconds: DEFAULT_TTI_SECONDS,
        }
    }
}
