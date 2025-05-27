use crate::{core::utils::errors::GraphError, search::property_index::PropertyIndex};
use raphtory_api::core::{entities::properties::props::PropMapper, PropType};
use std::{fs::create_dir_all, path::PathBuf};
use tantivy::{
    schema::Schema,
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Index, IndexReader, IndexSettings,
};

pub mod graph_index;
pub mod searcher;

mod collectors;
mod edge_filter_executor;
pub mod edge_index;
pub mod entity_index;
mod node_filter_executor;
pub mod node_index;
pub mod property_index;
mod query_builder;

pub(in crate::search) mod fields {
    pub const TIME: &str = "time";
    pub const SECONDARY_TIME: &str = "secondary_time";
    pub const NODE_ID: &str = "node_id";
    pub const NODE_NAME: &str = "node_name";
    pub const NODE_NAME_TOKENIZED: &str = "node_name_tokenized";
    pub const NODE_TYPE: &str = "node_type";
    pub const NODE_TYPE_TOKENIZED: &str = "node_type_tokenized";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "src";
    pub const SOURCE_TOKENIZED: &str = "src_tokenized";
    pub const DESTINATION: &str = "dst";
    pub const DESTINATION_TOKENIZED: &str = "dst_tokenized";
    pub const LAYER_ID: &str = "layer_id";
}

pub(crate) const TOKENIZER: &str = "custom_default";

pub fn register_default_tokenizers(index: &Index) {
    let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TOKENIZER, tokenizer);
}

pub(crate) fn new_index(
    schema: Schema,
    path: &Option<PathBuf>,
) -> Result<(Index, IndexReader), GraphError> {
    let index_builder = Index::builder()
        .settings(IndexSettings::default())
        .schema(schema);

    let index = if let Some(path) = path {
        create_dir_all(path).map_err(|e| {
            GraphError::IOErrorMsg(format!(
                "Failed to create index directory {}: {}",
                path.display(),
                e
            ))
        })?;

        index_builder.create_in_dir(path).map_err(|e| {
            GraphError::IndexErrorMsg(format!("Failed to create index in directory: {}", e))
        })?
    } else {
        index_builder.create_in_ram().map_err(|e| {
            GraphError::IndexErrorMsg(format!("Failed to create in-memory index: {}", e))
        })?
    };

    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()?;

    register_default_tokenizers(&index);

    Ok((index, reader))
}

fn resolve_props(
    meta: &PropMapper,
    props: &Vec<Option<PropertyIndex>>,
) -> Vec<(String, usize, PropType)> {
    props
        .iter()
        .enumerate()
        .filter_map(|(idx, opt)| {
            opt.as_ref().map(|_| {
                let name = meta.get_name(idx).to_string();
                let d_type = meta.get_dtype(idx).unwrap();
                (name, idx, d_type)
            })
        })
        .collect()
}
