use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::internal::{ConstPropertiesOps, TemporalPropertiesOps},
            storage::graph::storage_ops::GraphStorage,
            view::internal::{CoreGraphOps, InternalIndexSearch},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
    search::{
        fields, index_properties, initialize_node_const_property_indexes,
        initialize_node_temporal_property_indexes, new_index, property_index::PropertyIndex,
        TOKENIZER,
    },
};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::properties::props::Meta,
        storage::{arc_str::ArcStr, locked_vec::ArcReadLockedVec},
    },
    iter::BoxedLIter,
};
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Formatter},
    sync::{Arc, RwLock},
};
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, TermQuery},
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, Value,
        FAST, INDEXED, STORED,
    },
    Document, HasLen, Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument,
    TantivyError, Term,
};

#[derive(Clone)]
pub struct NodeIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
    pub(crate) constant_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    pub(crate) temporal_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
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
            constant_property_indexes: Arc::new(RwLock::new(Vec::new())),
            temporal_property_indexes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(1000))?;

        println!("Total node doc count: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Node doc: {:?}", doc.to_json(searcher.schema()));
        }

        let constant_property_indexes = self
            .constant_property_indexes
            .read()
            .map_err(|_| GraphError::LockError)?;

        for property_index in constant_property_indexes.iter().flatten() {
            property_index.print()?;
        }

        let temporal_property_indexes = self
            .temporal_property_indexes
            .read()
            .map_err(|_| GraphError::LockError)?;

        for property_index in temporal_property_indexes.iter().flatten() {
            property_index.print()?;
        }

        Ok(())
    }

    fn schema_builder() -> SchemaBuilder {
        let mut schema_builder: SchemaBuilder = Schema::builder();
        schema_builder.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        schema_builder.add_text_field(
            fields::NODE_NAME,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema_builder.add_text_field(
            fields::NODE_TYPE,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema_builder
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
        // println!("Added node doc: {}", &document.to_json(&schema));

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

    fn index_node<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        node: NodeView<G, GH>,
        writer: &IndexWriter,
        const_writers: &[Option<IndexWriter>],
        temporal_writers: &[Option<IndexWriter>],
    ) -> tantivy::Result<()> {
        let node_id: u64 = usize::from(node.node) as u64;
        let node_name = node.name();
        let node_type = node.node_type().unwrap_or_else(|| ArcStr::from(""));

        let constant_properties: &Vec<(ArcStr, usize, Prop)> =
            &self.collect_constant_properties(&node);
        let temporal_properties: BTreeMap<i64, Vec<(ArcStr, usize, Prop)>> =
            self.collect_temporal_properties(&node);

        for (time, temp_props) in &temporal_properties {
            index_properties(
                constant_properties.iter().cloned(),
                self.constant_property_indexes.write()?,
                *time,
                fields::NODE_ID,
                node_id,
                None,
                const_writers,
            )?;
            index_properties(
                temp_props.iter().cloned(),
                self.temporal_property_indexes.write()?,
                *time,
                fields::NODE_ID,
                node_id,
                None,
                temporal_writers,
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

    pub(crate) fn index_nodes(graph: &GraphStorage) -> tantivy::Result<NodeIndex> {
        let node_index = NodeIndex::new();

        // Initialize property indexes and get their writers
        let const_property_keys = graph
            .node_meta()
            .const_prop_meta()
            .get_keys()
            .into_iter()
            .cloned();
        let mut const_writers = initialize_node_const_property_indexes(
            graph,
            node_index.constant_property_indexes.clone(),
            const_property_keys,
        )?;

        let temporal_property_keys = graph
            .node_meta()
            .temporal_prop_meta()
            .get_keys()
            .into_iter()
            .cloned();
        let mut temporal_writers = initialize_node_temporal_property_indexes(
            graph,
            node_index.temporal_property_indexes.clone(),
            temporal_property_keys,
        )?;

        // Index nodes in parallel
        let mut writer = node_index.index.writer(100_000_000)?;
        let v_ids = (0..graph.count_nodes()).collect::<Vec<_>>();
        v_ids.par_chunks(128).try_for_each(|v_ids| {
            for v_id in v_ids {
                if let Some(node) = graph.node(NodeRef::new((*v_id).into())) {
                    node_index.index_node(node, &writer, &const_writers, &temporal_writers)?;
                }
            }
            Ok::<(), TantivyError>(())
        })?;

        // Commit writers
        for writer_option in &mut const_writers {
            if let Some(const_writer) = writer_option {
                const_writer.commit()?;
            }
        }
        for writer_option in &mut temporal_writers {
            if let Some(temporal_writer) = writer_option {
                temporal_writer.commit()?;
            }
        }
        writer.commit()?;

        // Reload readers
        {
            let const_indexes = node_index.constant_property_indexes.read()?;
            for property_index_option in const_indexes.iter().flatten() {
                property_index_option.reader.reload()?;
            }
        }
        {
            let temporal_indexes = node_index.temporal_property_indexes.read()?;
            for property_index_option in temporal_indexes.iter().flatten() {
                property_index_option.reader.reload()?;
            }
        }
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

        let const_property_keys = node.const_prop_keys();
        let const_writers = initialize_node_const_property_indexes(
            graph,
            self.constant_property_indexes.clone(),
            const_property_keys,
        )?;

        let temporal_property_keys = node.temporal_prop_keys();
        let temporal_writers = initialize_node_temporal_property_indexes(
            graph,
            self.temporal_property_indexes.clone(),
            temporal_property_keys,
        )?;

        let writer = Arc::new(parking_lot::RwLock::new(self.index.writer(100_000_000)?));
        let mut writer_guard = writer.write();
        self.index_node(node, &writer_guard, &const_writers, &temporal_writers)?;
        writer_guard.commit()?;
        self.reader.reload()?;

        Ok(())
    }
}
