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

pub(crate) fn new_index(schema: Schema, path: &Option<PathBuf>) -> (Index, IndexReader) {
    let index_builder = Index::builder()
        .settings(IndexSettings::default())
        .schema(schema);

    let index = if let Some(path) = path {
        create_dir_all(path).expect(&format!(
            "Failed to create index directory {}",
            path.display()
        ));
        index_builder
            .create_in_dir(path)
            .expect("Failed to create index")
    } else {
        index_builder
            .create_in_ram()
            .expect("Failed to create index in ram")
    };

    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()
        .unwrap();

    register_default_tokenizers(&index);

    (index, reader)
}
