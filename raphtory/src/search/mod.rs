use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{
        api::{
            properties::internal::{ConstPropertiesOps, PropertiesOps},
            view::StaticGraphViewOps,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{GraphViewOps, NodeViewOps, PropertyFilter},
    search::{property_index::PropertyIndex, query_builder::QueryBuilder},
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tantivy::{
    query::Query,
    schema::Schema,
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Index, IndexReader, IndexSettings,
};

pub mod graph_index;
pub mod searcher;

pub mod edge_index;
pub mod latest_value_collector;
pub mod node_filter_collector;
pub mod node_index;
pub mod property_index;
mod query_builder;
mod node_query_executor;
mod edge_query_executor;

pub(in crate::search) mod fields {
    pub const TIME: &str = "time";
    pub const NODE_ID: &str = "node_id";
    pub const NODE_NAME: &str = "node_name";
    pub const NODE_TYPE: &str = "node_type";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "from";
    pub const DESTINATION: &str = "to";
    pub const EDGE_TYPE: &str = "edge_type";
    pub const PROPERTIES: &str = "properties";
}

pub(crate) const TOKENIZER: &str = "custom_default";

pub(crate) fn new_index(schema: Schema, index_settings: IndexSettings) -> (Index, IndexReader) {
    let index = Index::builder()
        .settings(index_settings)
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

fn index_properties<I, PI: DerefMut<Target = Vec<Option<PropertyIndex>>>>(
    properties: I,
    mut property_indexes: PI,
    time: i64,
    field: &str,
    id: u64,
) -> tantivy::Result<()>
where
    I: Iterator<Item = (ArcStr, usize, Prop)>,
{
    for (prop_name, prop_id, prop_value) in properties {
        // Resize the vector if needed
        if prop_id >= property_indexes.len() {
            property_indexes.resize(prop_id + 1, None);
        }

        // Create a new PropertyIndex if it doesn't exist
        if property_indexes[prop_id].is_none() {
            let d_type = prop_value.dtype();
            property_indexes[prop_id] = Some(PropertyIndex::new(prop_name.clone(), d_type));
        }

        // Add the property value to the existing PropertyIndex
        if let Some(property_index) = &mut property_indexes[prop_id] {
            let prop_doc = property_index.create_document(
                time,
                field,
                id,
                prop_name.to_string(),
                prop_value,
            )?;

            let mut prop_writer = property_index.index.writer(50_000_000)?;
            prop_writer.add_document(prop_doc)?;
            prop_writer.commit()?;
            property_index.reader.reload()?;
        }
    }

    Ok(())
}
