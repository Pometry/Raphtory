use crate::{
    db::api::{
        properties::internal::{ConstPropertiesOps, PropertiesOps},
        view::{
            internal::{CoreGraphOps, InternalLayerOps, TimeSemantics},
            StaticGraphViewOps,
        },
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use itertools::Itertools;
use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
};
use tantivy::{
    query::Query,
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
    pub const NODE_TYPE: &str = "node_type";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "from";
    pub const DESTINATION: &str = "to";
    pub const LAYER_ID: &str = "layer_id";
}

pub(crate) const TOKENIZER: &str = "custom_default";

pub(crate) fn new_index(schema: Schema) -> (Index, IndexReader) {
    let index = Index::builder()
        .settings(IndexSettings::default())
        .schema(schema)
        .create_in_ram()
        .expect("Failed to create index");

    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()
        .unwrap();

    let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TOKENIZER, tokenizer);

    (index, reader)
}
