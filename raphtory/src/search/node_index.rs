use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
    },
    db::{api::view::IndexSpec, graph::node::NodeView},
    errors::GraphError,
    prelude::*,
    search::{
        entity_index::EntityIndex,
        fields::{NODE_ID, NODE_NAME, NODE_NAME_TOKENIZED, NODE_TYPE, NODE_TYPE_TOKENIZED},
        get_reader, indexed_props, resolve_props, TOKENIZER,
    },
};
use ahash::HashSet;
use raphtory_api::core::storage::{arc_str::ArcStr, dict_mapper::MaybeNew};
use raphtory_storage::graph::graph::GraphStorage;
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use std::{
    fmt::{Debug, Formatter},
    path::PathBuf,
};
use tantivy::{
    collector::TopDocs,
    query::AllQuery,
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST,
        INDEXED, STORED, STRING,
    },
    Document, IndexWriter, TantivyDocument, Term,
};

#[derive(Clone)]
pub struct NodeIndex {
    pub(crate) entity_index: EntityIndex,
    pub(crate) node_id_field: Field,
    pub(crate) node_name_field: Field,
    pub(crate) node_name_tokenized_field: Field,
    pub(crate) node_type_field: Field,
    pub(crate) node_type_tokenized_field: Field,
}

impl Debug for NodeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIndex")
            .field("index", &self.entity_index.index)
            .finish()
    }
}

impl NodeIndex {
    fn fetch_fields(schema: &Schema) -> Result<(Field, Field, Field, Field, Field), GraphError> {
        let node_id_field = schema
            .get_field(NODE_ID)
            .map_err(|_| GraphError::IndexErrorMsg("Node ID field missing in schema.".into()))?;

        let node_name_field = schema
            .get_field(NODE_NAME)
            .map_err(|_| GraphError::IndexErrorMsg("Node name field missing in schema.".into()))?;

        let node_name_tokenized_field = schema.get_field(NODE_NAME_TOKENIZED).map_err(|_| {
            GraphError::IndexErrorMsg("Tokenized node name field missing in schema.".into())
        })?;

        let node_type_field = schema
            .get_field(NODE_TYPE)
            .map_err(|_| GraphError::IndexErrorMsg("Node type field missing in schema.".into()))?;

        let node_type_tokenized_field = schema.get_field(NODE_TYPE_TOKENIZED).map_err(|_| {
            GraphError::IndexErrorMsg("Tokenized node type field missing in schema.".into())
        })?;

        Ok((
            node_id_field,
            node_name_field,
            node_name_tokenized_field,
            node_type_field,
            node_type_tokenized_field,
        ))
    }

    pub(crate) fn new(path: &Option<PathBuf>) -> Result<Self, GraphError> {
        let schema = Self::schema_builder().build();
        let (
            node_id_field,
            node_name_field,
            node_name_tokenized_field,
            node_type_field,
            node_type_tokenized_field,
        ) = Self::fetch_fields(&schema)?;

        let entity_index = EntityIndex::new(schema, path)?;

        Ok(Self {
            entity_index,
            node_id_field,
            node_name_field,
            node_name_tokenized_field,
            node_type_field,
            node_type_tokenized_field,
        })
    }

    pub(crate) fn load_from_path(path: &PathBuf) -> Result<Self, GraphError> {
        let entity_index = EntityIndex::load_nodes_index_from_path(path)?;
        let schema = entity_index.index.schema();
        let (
            node_id_field,
            node_name_field,
            node_name_tokenized_field,
            node_type_field,
            node_type_tokenized_field,
        ) = Self::fetch_fields(&schema)?;

        Ok(Self {
            entity_index,
            node_id_field,
            node_name_field,
            node_name_tokenized_field,
            node_type_field,
            node_type_tokenized_field,
        })
    }

    pub(crate) fn resolve_const_props(&self) -> HashSet<usize> {
        let props = self.entity_index.const_property_indexes.read();
        resolve_props(&props)
    }

    pub(crate) fn resolve_temp_props(&self) -> HashSet<usize> {
        let props = self.entity_index.temporal_property_indexes.read();
        resolve_props(&props)
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = get_reader(&self.entity_index.index)?.searcher();
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
        schema_builder.add_text_field(NODE_NAME, STRING);
        schema_builder.add_text_field(
            NODE_NAME_TOKENIZED,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema_builder.add_text_field(NODE_TYPE, STRING);
        schema_builder.add_text_field(
            NODE_TYPE_TOKENIZED,
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

    pub fn get_tokenized_node_field(&self, field_name: &str) -> tantivy::Result<Field> {
        self.entity_index
            .index
            .schema()
            .get_field(format!("{field_name}_tokenized").as_ref())
    }

    fn create_document(
        &self,
        node_id: u64,
        node_name: String,
        node_type: Option<ArcStr>,
    ) -> TantivyDocument {
        let mut document = TantivyDocument::new();
        document.add_u64(self.node_id_field, node_id);
        document.add_text(self.node_name_field, node_name.clone());
        document.add_text(self.node_name_tokenized_field, node_name);
        if let Some(node_type) = node_type {
            document.add_text(self.node_type_field, node_type.clone());
            document.add_text(self.node_type_tokenized_field, node_type);
        }
        document
    }

    fn index_node<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        node: NodeView<'graph, G, GH>,
        writer: &IndexWriter,
    ) -> Result<(), GraphError> {
        let node_id: u64 = usize::from(node.node) as u64;
        let node_name = node.name();
        let node_type = node.node_type();

        let node_doc = self.create_document(node_id, node_name.clone(), node_type.clone());
        writer.add_document(node_doc)?;

        Ok(())
    }

    pub(crate) fn index_nodes(
        &self,
        graph: &GraphStorage,
        path: Option<PathBuf>,
        index_spec: &IndexSpec,
    ) -> Result<(), GraphError> {
        // Index nodes fields
        let mut writer = self.entity_index.index.writer(100_000_000)?;
        let v_ids = (0..graph.count_nodes()).collect::<Vec<_>>();
        v_ids.par_chunks(128).try_for_each(|v_ids| {
            for v_id in v_ids {
                if let Some(node) = graph.node(NodeRef::new((*v_id).into())) {
                    self.index_node(node, &writer)?;
                }
            }
            Ok::<(), GraphError>(())
        })?;

        writer.commit()?;
        drop(writer);

        // Index const properties
        self.entity_index
            .index_node_const_props(graph, index_spec, &path)?;

        // Index temporal properties
        self.entity_index
            .index_node_temporal_props(graph, index_spec, &path)?;

        Ok(())
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
        let vid_u64 = node_id.inner().as_u64();

        // Check if the node document is already in the index,
        // if it does skip adding a new doc for same node
        node_id
            .if_new(|_| {
                let mut writer = self.entity_index.index.writer(100_000_000)?;
                let node_doc = self.create_document(vid_u64, node.name(), node.node_type());
                writer.add_document(node_doc)?;
                writer.commit()?;
                Ok::<(), GraphError>(())
            })
            .transpose()?;

        let indexes = self.entity_index.temporal_property_indexes.read();
        for (prop_id, prop_value) in indexed_props(props, &indexes) {
            if let Some(index) = &indexes[prop_id] {
                let mut writer = index.index.writer(50_000_000)?;
                let prop_doc =
                    index.create_node_temporal_property_document(t, vid_u64, &prop_value)?;
                writer.add_document(prop_doc)?;
                writer.commit()?;
            }
        }

        Ok(())
    }

    pub(crate) fn add_node_constant_properties(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let indexes = self.entity_index.const_property_indexes.read();
        for (prop_id, prop_value) in indexed_props(props, &indexes) {
            if let Some(index) = &indexes[prop_id] {
                let prop_doc =
                    index.create_node_const_property_document(node_id.as_u64(), &prop_value)?;
                let mut writer = index.index.writer(50_000_000)?;
                writer.add_document(prop_doc)?;
                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn update_node_constant_properties(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let indexes = self.entity_index.const_property_indexes.read();
        for (prop_id, prop_value) in indexed_props(props, &indexes) {
            if let Some(index) = &indexes[prop_id] {
                let mut writer = index.index.writer(50_000_000)?;
                // Delete existing constant property document
                let term = Term::from_field_u64(index.entity_id_field, node_id.as_u64());
                writer.delete_term(term);
                // Reindex constant properties
                let prop_doc =
                    index.create_node_const_property_document(node_id.as_u64(), &prop_value)?;
                writer.add_document(prop_doc)?;
                writer.commit()?;
            }
        }
        Ok(())
    }
}
