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
use parking_lot::Mutex;
use raphtory_api::core::{
    entities::properties::props::PropMapper, storage::arc_str::ArcStr, PropType,
};
use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};
use tantivy::{
    query::Query,
    schema::Schema,
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Index, IndexReader, IndexSettings, IndexWriter,
};

pub mod graph_index;
pub mod searcher;

mod collectors;
mod edge_filter_executor;
pub mod edge_index;
mod node_filter_executor;
pub mod node_index;
pub mod property_index;
mod query_builder;

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

pub fn initialize_property_indexes(
    property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_meta: &PropMapper,
) -> tantivy::Result<Vec<Option<IndexWriter>>> {
    let properties = prop_meta
        .get_keys()
        .into_iter()
        .filter_map(|k| {
            prop_meta.get_id(&*k).and_then(|prop_id| {
                prop_meta
                    .get_dtype(prop_id)
                    .map(|prop_type| (k.to_string(), prop_id, prop_type))
            })
        })
        .collect_vec();

    let mut prop_index_guard = property_indexes.write()?;
    let mut writers: Vec<Option<IndexWriter>> = Vec::new();

    for (prop_name, prop_id, prop_type) in properties {
        // Resize the vector if needed
        if prop_id >= prop_index_guard.len() {
            prop_index_guard.resize(prop_id + 1, None);
        }

        // Create a new PropertyIndex if it doesn't exist
        if prop_index_guard[prop_id].is_none() {
            let property_index = PropertyIndex::new(ArcStr::from(prop_name), prop_type);
            let writer = property_index.index.writer(50_000_000)?;
            writers.push(Some(writer));
            prop_index_guard[prop_id] = Some(property_index);
        }
    }

    Ok(writers)
}

fn index_properties<I, PI: DerefMut<Target = Vec<Option<PropertyIndex>>>>(
    properties: I,
    mut property_indexes: PI,
    time: i64,
    field: &str,
    id: u64,
    writers: &[Option<IndexWriter>],
) -> tantivy::Result<()>
where
    I: Iterator<Item = (ArcStr, usize, Prop)>,
{
    for (prop_name, prop_id, prop_value) in properties {
        if let Some(Some(prop_writer)) = writers.get(prop_id) {
            if let Some(property_index) = &mut property_indexes[prop_id] {
                let prop_doc = property_index.create_document(
                    time,
                    field,
                    id,
                    prop_name.to_string(),
                    prop_value,
                )?;
                prop_writer.add_document(prop_doc)?;
            }
        }
    }

    Ok(())
}
