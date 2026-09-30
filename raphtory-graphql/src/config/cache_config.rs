use field_types::FieldName;
use serde::Deserialize;

pub const DEFAULT_CACHE_CAPACITY: u64 = 30;

#[derive(Debug, Deserialize, PartialEq, Clone, serde::Serialize, FieldName)]
pub struct CacheConfig {
    pub capacity: u64,
    /// Serve every graph read-only: reads skip per-access segment locking entirely and
    /// graph mutations fail. For deployments whose workload has no updates.
    pub read_only: bool,
    /// Serve only the graphs at these paths (relative to the work dir, e.g. `ns/graph`)
    /// read-only, leaving every other graph writable. When set, supersedes `read_only`.
    pub read_only_graphs: Option<Vec<String>>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CACHE_CAPACITY,
            read_only: false,
            read_only_graphs: None,
        }
    }
}
