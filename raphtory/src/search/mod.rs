use crate::{
    core::{utils::errors::GraphError, Prop},
    db::api::{
        properties::internal::{ConstPropertiesOps, PropertiesOps},
        storage::graph::storage_ops::GraphStorage,
        view::{
            internal::{CoreGraphOps, InternalLayerOps, TimeSemantics},
            StaticGraphViewOps,
        },
    },
    prelude::{GraphViewOps, NodeViewOps},
    search::property_index::PropertyIndex,
};
use itertools::Itertools;
use parking_lot::RwLock;
use raphtory_api::core::{
    entities::properties::props::{Meta, PropMapper},
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    PropType,
};
use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tantivy::{
    query::Query,
    schema::{Schema, SchemaBuilder, FAST, INDEXED, STORED},
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

pub fn create_property_index(
    const_property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    temporal_property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    prop_id: MaybeNew<usize>,
    prop_name: &str,
    prop_type: &PropType,
    is_static: bool,
    add_const_schema_fields: fn(&mut SchemaBuilder),
    add_temporal_schema_fields: fn(&mut SchemaBuilder),
    new_property: fn(ArcStr, Schema) -> PropertyIndex,
) -> Result<(), GraphError> {
    prop_id
        .if_new(|prop_id| {
            let mut prop_index_guard = if is_static {
                const_property_indexes.write()
            } else {
                temporal_property_indexes.write()
            };

            // Resize the vector if needed
            if prop_id >= prop_index_guard.len() {
                prop_index_guard.resize(prop_id + 1, None);
            }

            let mut schema_builder = PropertyIndex::schema_builder(&*prop_name, prop_type.clone());
            if is_static {
                add_const_schema_fields(&mut schema_builder);
            } else {
                add_temporal_schema_fields(&mut schema_builder);
            }
            let schema = schema_builder.build();
            let property_index = new_property(ArcStr::from(prop_name), schema);
            prop_index_guard[prop_id] = Some(property_index);
            Ok::<_, GraphError>(())
        })
        .transpose()?;
    Ok(())
}

fn get_property_writers(
    mut prop_ids: impl Iterator<Item = usize>,
    property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
) -> Result<Vec<Option<IndexWriter>>, GraphError> {
    let mut prop_index_guard = property_indexes.read();

    let mut writers = Vec::new();
    writers.resize_with(prop_index_guard.len(), || None);
    for id in prop_ids {
        let writer = prop_index_guard[id]
            .as_ref()
            .map(|index| index.index.writer(50_000_000))
            .transpose()?;
        writers[id] = writer;
    }

    Ok(writers)
}

// We initialize the property indexes per property as and when we discover a new property while processing each node and edge update.
// While when creating indexes for a graph already built, all nodes/edges properties are already known in advance,
// which is why create all the property indexes upfront.
fn initialize_property_indexes(
    graph: &GraphStorage,
    property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
    get_property_meta: fn(&GraphStorage) -> &PropMapper,
    add_schema_fields: fn(&mut SchemaBuilder),
    new_property: fn(ArcStr, Schema) -> PropertyIndex,
) -> Result<Vec<Option<IndexWriter>>, GraphError> {
    let prop_meta = get_property_meta(graph);
    let properties = prop_keys
        .filter_map(|k| {
            prop_meta.get_id(&*k).and_then(|prop_id| {
                prop_meta
                    .get_dtype(prop_id)
                    .map(|prop_type| (k.to_string(), prop_id, prop_type))
            })
        })
        .collect_vec();

    let mut prop_index_guard = property_indexes.write();
    let mut writers: Vec<Option<IndexWriter>> = Vec::new();

    for (prop_name, prop_id, prop_type) in properties {
        // Resize the vector if needed
        if prop_id >= prop_index_guard.len() {
            prop_index_guard.resize(prop_id + 1, None);
        }

        // Create a new PropertyIndex if it doesn't exist
        if prop_index_guard[prop_id].is_none() {
            let mut schema_builder = PropertyIndex::schema_builder(&*prop_name, prop_type);
            add_schema_fields(&mut schema_builder);
            let schema = schema_builder.build();
            let property_index = new_property(ArcStr::from(prop_name), schema);
            let writer = property_index.index.writer(50_000_000)?;

            writers.push(Some(writer));
            prop_index_guard[prop_id] = Some(property_index);
        }
    }

    Ok(writers)
}

fn initialize_node_const_property_indexes(
    graph: &GraphStorage,
    property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> Result<Vec<Option<IndexWriter>>, GraphError> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.node_meta().const_prop_meta(),
        |schema| {
            schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        },
        PropertyIndex::new_node_property,
    )
}

fn initialize_node_temporal_property_indexes(
    graph: &GraphStorage,
    property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> Result<Vec<Option<IndexWriter>>, GraphError> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.node_meta().temporal_prop_meta(),
        |schema| {
            schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        },
        PropertyIndex::new_node_property,
    )
}

fn initialize_edge_const_property_indexes(
    graph: &GraphStorage,
    property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> Result<Vec<Option<IndexWriter>>, GraphError> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.edge_meta().const_prop_meta(),
        |schema| {
            schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
        },
        PropertyIndex::new_edge_property,
    )
}

fn initialize_edge_temporal_property_indexes(
    graph: &GraphStorage,
    property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> Result<Vec<Option<IndexWriter>>, GraphError> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.edge_meta().temporal_prop_meta(),
        |schema| {
            schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
        },
        PropertyIndex::new_edge_property,
    )
}

fn index_node_const_properties(
    property_indexes: impl Deref<Target = Vec<Option<PropertyIndex>>>,
    node_id: u64,
    writers: &[Option<IndexWriter>],
    props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
) -> Result<(), GraphError> {
    for (prop_id, prop_value) in props {
        if let Some(Some(prop_writer)) = writers.get(prop_id) {
            if let Some(property_index) = &property_indexes[prop_id] {
                let prop_doc = property_index
                    .create_node_const_property_document(node_id, prop_value.borrow())?;
                prop_writer.add_document(prop_doc)?;
            }
        }
    }

    Ok(())
}

fn index_node_temporal_properties(
    time: TimeIndexEntry,
    property_indexes: impl Deref<Target = Vec<Option<PropertyIndex>>>,
    node_id: u64,
    writers: &[Option<IndexWriter>],
    props: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
) -> Result<(), GraphError> {
    for (prop_id, prop) in props {
        if let Some(Some(prop_writer)) = writers.get(prop_id) {
            if let Some(property_index) = &property_indexes[prop_id] {
                let prop_doc = property_index.create_node_temporal_property_document(
                    time,
                    node_id,
                    prop.borrow(),
                )?;
                prop_writer.add_document(prop_doc)?;
            }
        }
    }

    Ok(())
}

fn index_edge_const_properties(
    property_indexes: impl Deref<Target = Vec<Option<PropertyIndex>>>,
    edge_id: u64,
    layer_id: usize,
    writers: &[Option<IndexWriter>],
    props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
) -> Result<(), GraphError> {
    for (prop_id, prop_value) in props {
        if let Some(Some(prop_writer)) = writers.get(prop_id) {
            if let Some(property_index) = &property_indexes[prop_id] {
                let prop_doc = property_index.create_edge_const_property_document(
                    edge_id,
                    layer_id,
                    prop_value.borrow(),
                )?;
                prop_writer.add_document(prop_doc)?;
            }
        }
    }

    Ok(())
}

fn index_edge_temporal_properties(
    time: TimeIndexEntry,
    property_indexes: impl Deref<Target = Vec<Option<PropertyIndex>>>,
    edge_id: u64,
    layer_id: usize,
    writers: &[Option<IndexWriter>],
    props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
) -> Result<(), GraphError> {
    for (prop_id, prop) in props {
        if let Some(Some(prop_writer)) = writers.get(prop_id) {
            if let Some(property_index) = &property_indexes[prop_id] {
                let prop_doc = property_index.create_edge_temporal_property_document(
                    time,
                    edge_id,
                    layer_id,
                    prop.borrow(),
                )?;
                prop_writer.add_document(prop_doc)?;
            }
        }
    }

    Ok(())
}

fn fetch_property_index(
    indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_id: Option<usize>,
) -> Option<(Arc<PropertyIndex>, usize)> {
    prop_id.and_then(|id| {
        indexes
            .read()
            .get(id)
            .and_then(|opt| opt.as_ref())
            .cloned()
            .map(Arc::from)
            .map(|index| (index, id))
    })
}

fn get_const_property_index(
    constant_property_indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    meta: &Meta,
    prop_name: &str,
) -> Result<Option<(Arc<PropertyIndex>, usize)>, GraphError> {
    Ok(fetch_property_index(
        constant_property_indexes,
        meta.const_prop_meta().get_id(prop_name),
    ))
}

fn get_temporal_property_index(
    temporal_property_indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    meta: &Meta,
    prop_name: &str,
) -> Result<Option<(Arc<PropertyIndex>, usize)>, GraphError> {
    Ok(fetch_property_index(
        temporal_property_indexes,
        meta.temporal_prop_meta().get_id(prop_name),
    ))
}
