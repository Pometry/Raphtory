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
        },
        graph::node::NodeView,
    },
    prelude::*,
    search::{
        entity_index::EntityIndex,
        fields::{NODE_ID, NODE_NAME, NODE_TYPE},
        TOKENIZER,
    },
};
use raphtory_api::core::storage::{arc_str::ArcStr, dict_mapper::MaybeNew};
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use std::fmt::{Debug, Formatter};
use tantivy::{
    collector::TopDocs,
    query::AllQuery,
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST,
        INDEXED, STORED,
    },
    Document, IndexWriter, TantivyDocument,
};

#[derive(Clone)]
pub struct NodeIndex {
    pub(crate) entity_index: EntityIndex,
    pub(crate) node_id_field: Field,
    pub(crate) node_name_field: Field,
    pub(crate) node_type_field: Field,
}

impl Debug for NodeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIndex")
            .field("index", &self.entity_index.index)
            .finish()
    }
}

impl NodeIndex {
    pub(crate) fn new() -> Self {
        let schema = Self::schema_builder().build();
        let node_id_field = schema.get_field(NODE_ID).ok().expect("Node id absent");
        let node_name_field = schema.get_field(NODE_NAME).expect("Node name absent");
        let node_type_field = schema.get_field(NODE_TYPE).expect("Node type absent");
        let entity_index = EntityIndex::new(schema);
        Self {
            entity_index,
            node_id_field,
            node_name_field,
            node_type_field,
        }
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.entity_index.reader.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(1000))?;

        println!("Total node doc count: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Node doc: {:?}", doc.to_json(searcher.schema()));
        }

        let constant_property_indexes = self.entity_index.const_property_indexes.read();

        for property_index in constant_property_indexes.iter().flatten() {
            property_index.print()?;
        }

        let temporal_property_indexes = self.entity_index.temporal_property_indexes.read();

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
        self.entity_index.index.schema().get_field(field_name)
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
        const_writers: &mut [Option<IndexWriter>],
        const_props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node_id = node_id.as_u64();
        self.entity_index.index_node_const_properties(
            node_id,
            const_writers,
            const_props.iter().map(|(id, prop)| (*id, prop)),
        )?;

        self.entity_index.commit_writers(const_writers)
    }

    fn index_node_t(
        &self,
        time: TimeIndexEntry,
        node_id: MaybeNew<VID>,
        node_name: String,
        node_type: Option<ArcStr>,
        writer: &mut IndexWriter,
        temporal_writers: &mut [Option<IndexWriter>],
        temporal_props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let vid_u64 = node_id.inner().as_u64();
        self.entity_index.index_node_temporal_properties(
            time,
            vid_u64,
            temporal_writers,
            temporal_props.iter().map(|(id, prop)| (*id, prop)),
        )?;

        // Check if the node document is already in the index,
        // if it does skip adding a new doc for same node
        node_id
            .if_new(|_| {
                let node_doc = self.create_document(vid_u64, node_name, node_type);
                writer.add_document(node_doc)?;
                Ok::<(), GraphError>(())
            })
            .transpose()?;

        self.entity_index.commit_writers(temporal_writers)?;
        writer.commit()?;

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

        self.entity_index.index_node_const_properties(
            node_id,
            const_writers,
            node.properties().constant().iter_id(),
        )?;

        for (t, temporal_properties) in node.rows() {
            self.entity_index.index_node_temporal_properties(
                t,
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
        let mut const_writers = node_index
            .entity_index
            .initialize_node_const_property_indexes(graph, const_property_keys)?;

        let temporal_property_keys = graph
            .node_meta()
            .temporal_prop_meta()
            .get_keys()
            .into_iter();
        let mut temporal_writers = node_index
            .entity_index
            .initialize_node_temporal_property_indexes(graph, temporal_property_keys)?;

        // Index nodes in parallel
        let mut writer = node_index.entity_index.index.writer(100_000_000)?;
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
        node_index.entity_index.commit_writers(&mut const_writers)?;
        node_index
            .entity_index
            .commit_writers(&mut temporal_writers)?;
        writer.commit()?;

        // Reload readers
        node_index.entity_index.reload_const_property_indexes()?;
        node_index.entity_index.reload_temporal_property_indexes()?;
        node_index.entity_index.reader.reload()?;

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
        let mut temporal_writers = self
            .entity_index
            .get_temporal_property_writers(temporal_property_ids)?;

        let mut writer = self.entity_index.index.writer(100_000_000)?;
        self.index_node_t(
            t,
            node_id,
            node.name(),
            node.node_type(),
            &mut writer,
            &mut temporal_writers,
            props,
        )?;

        self.entity_index.reader.reload()?;

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
        let mut const_writers = self
            .entity_index
            .get_const_property_writers(const_property_ids)?;

        self.index_node_c(node_id, &mut const_writers, props)?;

        self.entity_index.reload_const_property_indexes()?;

        Ok(())
    }

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
        let mut const_writers = self
            .entity_index
            .get_const_property_writers(const_property_ids)?;

        // Delete existing constant property document
        self.entity_index.delete_const_properties_index_docs(
            node_id.as_u64(),
            &mut const_writers,
            props.iter().map(|(id, prop)| (*id, prop)),
        )?;

        // Reindex the node's constant properties
        self.index_node_c(node_id, &mut const_writers, props)?;

        self.entity_index.reload_const_property_indexes()?;

        Ok(())
    }
}
