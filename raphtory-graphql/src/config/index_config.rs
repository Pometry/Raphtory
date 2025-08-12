use serde::Deserialize;

pub const DEFAULT_CREATE_INDEX: bool = false;

#[derive(Debug, Deserialize, PartialEq, Clone, serde::Serialize)]
pub struct IndexConfig {
    pub create_index: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            create_index: DEFAULT_CREATE_INDEX,
        }
    }
}
