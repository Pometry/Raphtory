use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{
        api::{
            properties::internal::{ConstPropertiesOps, PropertiesOps},
            storage::graph::storage_ops::GraphStorage,
            view::{
                internal::{CoreGraphOps, InternalLayerOps, TimeSemantics},
                StaticGraphViewOps,
            },
        },
        graph::edge::EdgeView,
    },
    prelude::{GraphViewOps, NodeViewOps},
    search::property_index::PropertyIndex,
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::properties::props::{Meta, PropMapper},
    storage::arc_str::ArcStr,
};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
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
    pub const NODE_ID: &str = "node_id";
    pub const NODE_NAME: &str = "node_name";
    pub const NODE_TYPE: &str = "node_type";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "from";
    pub const DESTINATION: &str = "to";
    pub const LAYER_ID: &str = "layer_id";
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

// We initialize the property indexes per property as and when we discover a new property while processing each node and edge update.
// While when creating indexes for a graph already built, all nodes/edges properties are already known in advance,
// which is why create all the property indexes upfront.
fn initialize_property_indexes(
    graph: &GraphStorage,
    property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
    get_property_meta: fn(&GraphStorage) -> &PropMapper,
    add_schema_fields: fn(&mut SchemaBuilder),
) -> tantivy::Result<Vec<Option<IndexWriter>>> {
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

    let mut prop_index_guard = property_indexes.write()?;
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
            let property_index = PropertyIndex::new(ArcStr::from(prop_name), schema);
            let writer = property_index.index.writer(50_000_000)?;

            writers.push(Some(writer));
            prop_index_guard[prop_id] = Some(property_index);
        }
    }

    Ok(writers)
}

fn initialize_node_const_property_indexes(
    graph: &GraphStorage,
    property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> tantivy::Result<Vec<Option<IndexWriter>>> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.node_meta().const_prop_meta(),
        |schema| {
            schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        },
    )
}

fn initialize_node_temporal_property_indexes(
    graph: &GraphStorage,
    property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> tantivy::Result<Vec<Option<IndexWriter>>> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.node_meta().temporal_prop_meta(),
        |schema| {
            schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        },
    )
}

fn initialize_edge_const_property_indexes(
    graph: &GraphStorage,
    property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> tantivy::Result<Vec<Option<IndexWriter>>> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.edge_meta().const_prop_meta(),
        |schema| {
            schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
        },
    )
}

fn initialize_edge_temporal_property_indexes(
    graph: &GraphStorage,
    property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    prop_keys: impl Iterator<Item = ArcStr>,
) -> tantivy::Result<Vec<Option<IndexWriter>>> {
    initialize_property_indexes(
        graph,
        property_indexes,
        prop_keys,
        |g| g.edge_meta().temporal_prop_meta(),
        |schema| {
            schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
            schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
        },
    )
}

fn index_properties<I, PI: DerefMut<Target = Vec<Option<PropertyIndex>>>>(
    properties: I,
    mut property_indexes: PI,
    time: i64,
    field: &str,
    id: u64,
    layer_id: Option<usize>,
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
                    layer_id,
                    prop_name.to_string(),
                    prop_value,
                )?;
                prop_writer.add_document(prop_doc)?;
            }
        }
    }

    Ok(())
}

// Property Semantics:
// There is a possibility that a const and temporal property share same name. This means that if a node
// or an edge doesn't have a value for that temporal property, we fall back to its const property value.
// Otherwise, the temporal property takes precedence.
//
// Search semantics:
// This means that a property filter criteria, say p == 1, is looked for in both the const and temporal
// property indexes for the given property name (if shared by both const and temporal properties). Now,
// if the filter matches to docs in const property index but there already is a temporal property with a
// different value, the doc is rejected i.e., fails the property filter criteria because temporal property
// takes precedence.
//          Search p == 1
//      t_prop      c_prop
//        T           T
//        T           F
//  (p=2) F     (p=1) T
//        F           F

fn get_property_indexes(
    constant_property_indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    temporal_property_indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    meta: &Meta,
    prop_name: &str,
) -> Result<
    (
        Option<(Arc<PropertyIndex>, usize)>,
        Option<(Arc<PropertyIndex>, usize)>,
    ),
    GraphError,
> {
    fn fetch_property_index(
        indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
        prop_id: Option<usize>,
    ) -> Option<(Arc<PropertyIndex>, usize)> {
        prop_id.and_then(|id| {
            indexes
                .read()
                .ok()?
                .get(id)
                .and_then(|opt| opt.as_ref())
                .cloned()
                .map(Arc::from)
                .map(|index| (index, id))
        })
    }

    let constant_index = fetch_property_index(
        constant_property_indexes,
        meta.const_prop_meta().get_id(prop_name),
    );
    let temporal_index = fetch_property_index(
        temporal_property_indexes,
        meta.temporal_prop_meta().get_id(prop_name),
    );

    match (constant_index.clone(), temporal_index.clone()) {
        (None, None) => Err(GraphError::PropertyNotFound(prop_name.to_string())),
        _ => Ok((constant_index, temporal_index)),
    }
}
