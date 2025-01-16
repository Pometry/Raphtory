use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::internal::ConstPropertiesOps, storage::graph::storage_ops::GraphStorage,
            view::internal::InternalIndexSearch,
        },
        graph::node::NodeView,
    },
    prelude::*,
    search::{fields, index_properties, new_index, property_index::PropertyIndex, TOKENIZER},
};
use raphtory_api::core::{entities::properties::props::Meta, storage::arc_str::ArcStr};
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Formatter},
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, BooleanQuery, TermQuery},
    schema::{
        Field, FieldType, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
        Type, Value, FAST, INDEXED, STORED,
    },
    Document, Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, TantivyError, Term,
};

#[derive(Clone)]
pub struct NodeIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
    pub(crate) property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
}

impl Debug for NodeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIndex")
            .field("index", &self.index)
            .finish()
    }
}

impl NodeIndex {
    pub(crate) fn new() -> Self {
        let schema = Self::schema_builder().build();
        let (index, reader) = new_index(schema, IndexSettings::default());
        Self {
            index: Arc::new(index),
            reader,
            property_indexes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(100))?;

        println!("Total node doc count: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Node doc: {:?}", doc.to_json(searcher.schema()));
        }

        let property_indexes = self
            .property_indexes
            .read()
            .map_err(|_| GraphError::LockError)?;

        for property_index in property_indexes.iter().flatten() {
            property_index.print()?;
        }

        Ok(())
    }

    fn schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        schema.add_text_field(
            fields::NODE_NAME,
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(TOKENIZER)
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        schema.add_text_field(
            fields::NODE_TYPE,
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(TOKENIZER)
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        schema
    }

    pub fn get_property_index(
        &self,
        meta: &Meta,
        prop_name: &str,
    ) -> Result<Arc<PropertyIndex>, GraphError> {
        fn get_prop_id(meta: &Meta, prop_name: &str) -> Result<usize, GraphError> {
            meta.temporal_prop_meta()
                .get_id(prop_name)
                .or_else(|| meta.const_prop_meta().get_id(prop_name))
                .ok_or_else(|| GraphError::PropertyNotFound(prop_name.to_string()))
        }

        let property_indexes = self
            .property_indexes
            .read()
            .map_err(|_| GraphError::LockError)?;

        let prop_id = get_prop_id(meta, prop_name)?;

        if let Some(Some(property_index)) = property_indexes.get(prop_id) {
            Ok(Arc::from(property_index.clone()))
        } else {
            Err(GraphError::PropertyIndexNotFound(prop_name.to_string()))
        }
    }

    pub fn get_node_field(&self, field_name: &str) -> tantivy::Result<Field> {
        self.index.schema().get_field(field_name)
    }

    fn create_document<'a>(
        &self,
        node_id: u64,
        node_name: String,
        node_type: ArcStr,
    ) -> tantivy::Result<TantivyDocument> {
        let schema = self.index.schema();

        let doc = json!({
            "node_id": node_id,
            "node_name": node_name,
            "node_type": node_type,
        });

        let document = TantivyDocument::parse_json(&schema, &doc.to_string())?;
        println!("Added node doc: {}", &document.to_json(&schema));

        Ok(document)
    }

    fn collect_constant_properties<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        node: &NodeView<G, GH>,
    ) -> Vec<(ArcStr, usize, Prop)> {
        node.properties()
            .constant()
            .iter()
            .filter_map(|(k, p)| node.get_const_prop_id(k.as_ref()).map(|id| (k, id, p)))
            .collect()
    }

    fn collect_temporal_properties<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        node: &NodeView<G, GH>,
    ) -> BTreeMap<i64, Vec<(ArcStr, usize, Prop)>> {
        node.properties()
            .temporal()
            .iter()
            .flat_map(|(key, values)| {
                let pid = values.id;
                values
                    .into_iter()
                    .map(move |(t, v)| (t, key.clone(), pid, v))
            })
            .fold(BTreeMap::new(), |mut map, (t, k, pid, v)| {
                map.entry(t).or_insert_with(Vec::new).push((k, pid, v));
                map
            })
    }

    fn index_node<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        W: Deref<Target = IndexWriter>,
    >(
        &self,
        node: NodeView<G, GH>,
        writer: &W,
    ) -> tantivy::Result<()> {
        let node_id: u64 = usize::from(node.node) as u64;
        let node_name = node.name();
        let node_type = node.node_type().unwrap_or_else(|| ArcStr::from(""));

        let constant_properties: Vec<(ArcStr, usize, Prop)> =
            self.collect_constant_properties(&node);

        let temporal_properties: BTreeMap<i64, Vec<(ArcStr, usize, Prop)>> =
            self.collect_temporal_properties(&node);

        for (time, temp_props) in &temporal_properties {
            let properties = constant_properties
                .iter()
                .cloned()
                .chain(temp_props.iter().cloned());

            index_properties(
                properties,
                self.property_indexes.write()?,
                *time,
                fields::NODE_ID,
                node_id,
            )?;
        }

        // Check if the node document is already in the index,
        // if it does skip adding a new doc for same node
        let schema = self.index.schema();
        let node_id_field = schema.get_field(fields::NODE_ID)?;

        let query = TermQuery::new(
            Term::from_field_u64(node_id_field, node_id),
            IndexRecordOption::Basic,
        );

        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
        if !top_docs.is_empty() {
            return Ok(());
        }

        let node_doc = self.create_document(node_id, node_name.clone(), node_type.clone())?;
        writer.add_document(node_doc)?;

        Ok(())
    }

    pub(crate) fn index_nodes(g: &GraphStorage) -> tantivy::Result<NodeIndex> {
        let node_index = NodeIndex::new();

        let writer = Arc::new(parking_lot::RwLock::new(
            node_index.index.writer(100_000_000)?,
        ));
        let v_ids = (0..g.count_nodes()).collect::<Vec<_>>();
        v_ids.par_chunks(128).try_for_each(|v_ids| {
            let writer_lock = writer.clone();
            {
                let writer_guard = writer_lock.read();
                for v_id in v_ids {
                    if let Some(node) = g.node(NodeRef::new((*v_id).into())) {
                        node_index.index_node(node, &writer_guard)?;
                    }
                }
            }

            Ok::<(), TantivyError>(())
        })?;

        let mut writer_guard = writer.write();
        writer_guard.commit()?;

        node_index.reader.reload()?;

        Ok(node_index)
    }

    pub(crate) fn add_node_update(
        &self,
        graph: &GraphStorage,
        t: TimeIndexEntry,
        v: VID,
    ) -> Result<(), GraphError> {
        let node_id = v.as_u64();
        let node = graph
            .node(VID(node_id as usize))
            .expect("Node for internal id should exist.")
            .at(t.t());

        let writer = Arc::new(parking_lot::RwLock::new(self.index.writer(100_000_000)?));
        let mut writer_guard = writer.write();
        self.index_node(node, &writer_guard)?;
        writer_guard.commit()?;
        self.reader.reload()?;

        Ok(())
    }
}
