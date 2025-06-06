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
        resolve_props, TOKENIZER,
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
    Document, IndexWriter, TantivyDocument,
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

    fn index_node_c(
        &self,
        node_id: VID,
        writers: &mut [Option<IndexWriter>],
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node_id = node_id.as_u64();
        self.entity_index
            .index_node_const_properties(node_id, writers, props)?;
        self.entity_index.commit_writers(writers)
    }

    fn index_node_t(
        &self,
        time: TimeIndexEntry,
        node_id: MaybeNew<VID>,
        node_name: String,
        node_type: Option<ArcStr>,
        writer: &mut IndexWriter,
        temporal_writers: &mut [Option<IndexWriter>],
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let vid_u64 = node_id.inner().as_u64();
        self.entity_index
            .index_node_temporal_properties(time, vid_u64, temporal_writers, props)?;

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
        node: NodeView<'graph, G, GH>,
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
            &*node
                .properties()
                .constant()
                .iter_id()
                .collect::<Vec<(usize, Prop)>>(),
        )?;

        for (t, temporal_properties) in node.rows() {
            self.entity_index.index_node_temporal_properties(
                t,
                node_id,
                temporal_writers,
                &*temporal_properties,
            )?;
        }

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
        // Initialize property indexes and get their writers
        let const_properties_index_path = path.as_deref().map(|p| p.join("const_properties"));
        let mut const_writers = self.entity_index.initialize_node_const_property_indexes(
            graph.node_meta().const_prop_meta(),
            &const_properties_index_path,
            &index_spec.node_const_props,
        )?;

        let temporal_properties_index_path = path.as_deref().map(|p| p.join("temporal_properties"));
        let mut temporal_writers = self
            .entity_index
            .initialize_node_temporal_property_indexes(
                graph.node_meta().temporal_prop_meta(),
                &temporal_properties_index_path,
                &index_spec.node_temp_props,
            )?;

        // Index nodes in parallel
        let mut writer = self.entity_index.index.writer(100_000_000)?;
        let v_ids = (0..graph.count_nodes()).collect::<Vec<_>>();
        v_ids.par_chunks(128).try_for_each(|v_ids| {
            for v_id in v_ids {
                if let Some(node) = graph.node(NodeRef::new((*v_id).into())) {
                    self.index_node(node, &writer, &const_writers, &temporal_writers)?;
                }
            }
            Ok::<(), GraphError>(())
        })?;

        // Commit writers
        self.entity_index.commit_writers(&mut const_writers)?;
        self.entity_index.commit_writers(&mut temporal_writers)?;
        writer.commit()?;

        // Reload readers
        self.entity_index.reload_const_property_indexes()?;
        self.entity_index.reload_temporal_property_indexes()?;
        self.entity_index.reader.reload()?;

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

        let mut temporal_writers = self.entity_index.get_temporal_property_writers(props)?;

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

        self.entity_index.reload_temporal_property_indexes()?;
        self.entity_index.reader.reload()?;

        Ok(())
    }

    pub(crate) fn add_node_constant_properties(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut const_writers = self.entity_index.get_const_property_writers(props)?;

        self.index_node_c(node_id, &mut const_writers, props)?;

        self.entity_index.reload_const_property_indexes()?;

        Ok(())
    }

    pub(crate) fn update_node_constant_properties(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut const_writers = self.entity_index.get_const_property_writers(props)?;

        // Delete existing constant property document
        self.entity_index.delete_const_properties_index_docs(
            node_id.as_u64(),
            &mut const_writers,
            props,
        )?;

        // Reindex the node's constant properties
        self.index_node_c(node_id, &mut const_writers, props)?;

        self.entity_index.reload_const_property_indexes()?;

        Ok(())
    }
}
