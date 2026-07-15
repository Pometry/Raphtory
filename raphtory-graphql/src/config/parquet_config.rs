use field_types::FieldName;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration controlling which paths parquet files may be loaded from.
/// An empty `allowed_paths` list permits loading from any path.
#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize, FieldName)]
pub struct ParquetConfig {
    pub allowed_paths: Vec<PathBuf>,
}
