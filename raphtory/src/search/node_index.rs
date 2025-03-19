use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::internal::{
                ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertiesRowView,
            },
            storage::graph::storage_ops::GraphStorage,
            view::internal::{CoreGraphOps, InternalIndexSearch},
        },
        graph::node::NodeView,
    },
    prelude::*,
    search::{
        fields::{NODE_ID, NODE_NAME, NODE_TYPE},
        get_property_writers, index_node_const_properties, index_node_temporal_properties,
        initialize_node_const_property_indexes, initialize_node_temporal_property_indexes,
        new_index,
        property_index::PropertyIndex,
        TOKENIZER,
    },
};
use parking_lot::RwLock;
use raphtory_api::core::storage::{arc_str::ArcStr, dict_mapper::MaybeNew};
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
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
    pub(crate) node_id_field: Field,
    pub(crate) node_name_field: Field,
    pub(crate) node_type_field: Field,
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
        let node_id_field = schema.get_field(NODE_ID).ok().expect("Node id absent");
        let node_name_field = schema.get_field(NODE_NAME).expect("Node name absent");
        let node_type_field = schema.get_field(NODE_TYPE).expect("Node type absent");
        let (index, reader) = new_index(schema, IndexSettings::default());
        Self {
            index: Arc::new(index),
            reader,
            constant_property_indexes: Arc::new(RwLock::new(Vec::new())),
            temporal_property_indexes: Arc::new(RwLock::new(Vec::new())),
            node_id_field,
            node_name_field,
            node_type_field,
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

        let constant_property_indexes = self.constant_property_indexes.read();

        for property_index in constant_property_indexes.iter().flatten() {
            property_index.print()?;
        }

        let temporal_property_indexes = self.temporal_property_indexes.read();

        for property_index in temporal_property_indexes.iter().flatten() {
            property_index.print()?;
        }

        Ok(())
    }

    fn schema_builder() -> SchemaBuilder {
        let mut schema_builder: SchemaBuilder = Schema::builder();
        schema_builder.add_u64_field(NODE_ID, INDEXED | FAST | STORED);
        schema_builder.add_text_field(
            NODE_NAME,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema_builder.add_text_field(
            NODE_TYPE,
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
        node_type: Option<ArcStr>,
    ) -> TantivyDocument {
        let mut document = TantivyDocument::new();
        document.add_u64(self.node_id_field, node_id);
        document.add_text(self.node_name_field, node_name);
        if let Some(node_type) = node_type {
            document.add_text(self.node_type_field, node_type);
        }
        document
    }

    fn index_node_c(
        &self,
        node_id: VID,
        const_writers: &[Option<IndexWriter>],
        const_props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node_id = node_id.as_u64();
        index_node_const_properties(
            self.constant_property_indexes.read(),
            node_id,
            const_writers,
            const_props.iter().map(|(id, prop)| (*id, prop)),
        )
    }

    fn index_node_t(
        &self,
        time: TimeIndexEntry,
        node_id: MaybeNew<VID>,
        node_name: String,
        node_type: Option<ArcStr>,
        writer: &IndexWriter,
        temporal_writers: &[Option<IndexWriter>],
        temporal_props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let vid_u64 = node_id.inner().as_u64();
        index_node_temporal_properties(
            time,
            self.temporal_property_indexes.read(),
            vid_u64,
            temporal_writers,
            temporal_props.iter().map(|(id, prop)| (*id, prop)),
        )?;

        // Check if the node document is already in the index,
        // if it does skip adding a new doc for same node
        node_id
            .if_new(|vid| {
                let node_doc = self.create_document(vid_u64, node_name, node_type);
                writer.add_document(node_doc)?;
                Ok::<(), GraphError>(())
            })
            .transpose()?;

        Ok(())
    }

    fn index_node<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        node: NodeView<G, GH>,
        writer: &IndexWriter,
        const_writers: &[Option<IndexWriter>],
        temporal_writers: &[Option<IndexWriter>],
    ) -> Result<(), GraphError> {
        let node_id: u64 = usize::from(node.node) as u64;
        let node_name = node.name();
        let node_type = node.node_type();

        index_node_const_properties(
            self.constant_property_indexes.read(),
            node_id,
            const_writers,
            node.properties().constant().iter_id(),
        )?;

        for (t, temporal_properties) in node.rows() {
            index_node_temporal_properties(
                t,
                self.temporal_property_indexes.read(),
                node_id,
                temporal_writers,
                temporal_properties,
            )?;
        }

        let node_doc = self.create_document(node_id, node_name.clone(), node_type.clone());
        writer.add_document(node_doc)?;

        Ok(())
    }

    pub(crate) fn index_nodes(graph: &GraphStorage) -> Result<NodeIndex, GraphError> {
        let node_index = NodeIndex::new();

        // Initialize property indexes and get their writers
        let const_property_keys = graph.node_meta().const_prop_meta().get_keys().into_iter();
        let mut const_writers = initialize_node_const_property_indexes(
            graph,
            &node_index.constant_property_indexes,
            const_property_keys,
        )?;

        let temporal_property_keys = graph
            .node_meta()
            .temporal_prop_meta()
            .get_keys()
            .into_iter();
        let mut temporal_writers = initialize_node_temporal_property_indexes(
            graph,
            &node_index.temporal_property_indexes,
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
            Ok::<(), GraphError>(())
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
            let const_indexes = node_index.constant_property_indexes.read();
            for property_index_option in const_indexes.iter().flatten() {
                property_index_option.reader.reload()?;
            }
        }
        {
            let temporal_indexes = node_index.temporal_property_indexes.read();
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
        node_id: MaybeNew<VID>,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node = graph
            .node(VID(node_id.inner().as_u64() as usize))
            .expect("Node for internal id should exist.")
            .at(t.t());

        let temporal_property_ids = node.temporal_prop_ids();
        let temporal_writers =
            get_property_writers(temporal_property_ids, &self.temporal_property_indexes)?;

        let mut writer = self.index.writer(100_000_000)?;
        self.index_node_t(
            t,
            node_id,
            node.name(),
            node.node_type(),
            &writer,
            &temporal_writers,
            props,
        )?;
        writer.commit()?;
        self.reader.reload()?;

        Ok(())
    }

    pub(crate) fn add_node_constant_properties(
        &self,
        graph: &GraphStorage,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node = graph
            .node(VID(node_id.as_u64() as usize))
            .expect("Node for internal id should exist.");

        let const_property_ids = node.const_prop_ids();
        let const_writers =
            get_property_writers(const_property_ids, &self.constant_property_indexes)?;

        self.index_node_c(node_id, &const_writers, props)?;
        self.reader.reload()?;

        Ok(())
    }

    // TODO: Update the constant property by deleting and recreating tantivy doc?
    pub(crate) fn update_node_constant_properties(
        &self,
        graph: &GraphStorage,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node = graph
            .node(VID(node_id.as_u64() as usize))
            .expect("Node for internal id should exist.");

        let const_property_ids = node.const_prop_ids();
        let const_writers =
            get_property_writers(const_property_ids, &self.constant_property_indexes)?;

        self.index_node_c(node_id, &const_writers, props)?;
        self.reader.reload()?;

        Ok(())
    }
}
