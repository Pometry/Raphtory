use crate::{
    core::{entities::EID, storage::timeindex::TimeIndexEntry},
    db::{api::view::IndexSpec, graph::edge::EdgeView},
    errors::GraphError,
    prelude::*,
    search::{
        entity_index::EntityIndex,
        fields::{DESTINATION, DESTINATION_TOKENIZED, EDGE_ID, SOURCE, SOURCE_TOKENIZED},
        get_reader, indexed_props, resolve_props, TOKENIZER,
    },
};
use ahash::HashSet;
use raphtory_api::core::storage::dict_mapper::MaybeNew;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage},
};
use rayon::{iter::IntoParallelIterator, prelude::ParallelIterator};
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
pub struct EdgeIndex {
    pub(crate) entity_index: EntityIndex,
    pub(crate) edge_id_field: Field,
    pub(crate) src_field: Field,
    pub(crate) src_tokenized_field: Field,
    pub(crate) dst_field: Field,
    pub(crate) dst_tokenized_field: Field,
}

impl Debug for EdgeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeIndex")
            .field("index", &self.entity_index.index)
            .finish()
    }
}

impl EdgeIndex {
    fn fetch_fields(schema: &Schema) -> Result<(Field, Field, Field, Field, Field), GraphError> {
        let edge_id_field = schema
            .get_field(EDGE_ID)
            .map_err(|_| GraphError::IndexErrorMsg("Edge ID field missing in schema.".into()))?;

        let src_field = schema
            .get_field(SOURCE)
            .map_err(|_| GraphError::IndexErrorMsg("Source field missing in schema.".into()))?;

        let src_tokenized_field = schema.get_field(SOURCE_TOKENIZED).map_err(|_| {
            GraphError::IndexErrorMsg("Tokenized source field missing in schema.".into())
        })?;

        let dst_field = schema.get_field(DESTINATION).map_err(|_| {
            GraphError::IndexErrorMsg("Destination field missing in schema.".into())
        })?;

        let dst_tokenized_field = schema.get_field(DESTINATION_TOKENIZED).map_err(|_| {
            GraphError::IndexErrorMsg("Tokenized destination field missing in schema.".into())
        })?;

        Ok((
            edge_id_field,
            src_field,
            src_tokenized_field,
            dst_field,
            dst_tokenized_field,
        ))
    }

    pub(crate) fn new(path: &Option<PathBuf>) -> Result<Self, GraphError> {
        let schema = Self::schema_builder().build();
        let (edge_id_field, src_field, src_tokenized_field, dst_field, dst_tokenized_field) =
            Self::fetch_fields(&schema)?;

        let entity_index = EntityIndex::new(schema, path)?;

        Ok(Self {
            entity_index,
            edge_id_field,
            src_field,
            src_tokenized_field,
            dst_field,
            dst_tokenized_field,
        })
    }

    pub(crate) fn load_from_path(path: &PathBuf) -> Result<Self, GraphError> {
        let entity_index = EntityIndex::load_edges_index_from_path(path)?;
        let schema = entity_index.index.schema();
        let (edge_id_field, src_field, src_tokenized_field, dst_field, dst_tokenized_field) =
            Self::fetch_fields(&schema)?;

        Ok(Self {
            entity_index,
            edge_id_field,
            src_field,
            src_tokenized_field,
            dst_field,
            dst_tokenized_field,
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

        println!("Total edge doc count: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Edge doc: {:?}", doc.to_json(searcher.schema()));
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
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field(EDGE_ID, INDEXED | FAST | STORED);
        schema_builder.add_text_field(SOURCE, STRING);
        schema_builder.add_text_field(
            SOURCE_TOKENIZED,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema_builder.add_text_field(DESTINATION, STRING);
        schema_builder.add_text_field(
            DESTINATION_TOKENIZED,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema_builder
    }

    pub fn get_edge_field(&self, field_name: &str) -> tantivy::Result<Field> {
        self.entity_index.index.schema().get_field(field_name)
    }

    pub fn get_tokenized_edge_field(&self, field_name: &str) -> tantivy::Result<Field> {
        self.entity_index
            .index
            .schema()
            .get_field(format!("{field_name}_tokenized").as_ref())
    }

    fn create_document(&self, edge_id: u64, src: String, dst: String) -> TantivyDocument {
        let mut document = TantivyDocument::new();
        document.add_u64(self.edge_id_field, edge_id);
        document.add_text(self.src_field, src.clone());
        document.add_text(self.src_tokenized_field, src);
        document.add_text(self.dst_field, dst.clone());
        document.add_text(self.dst_tokenized_field, dst);
        document
    }

    fn index_edge<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        edge: EdgeView<G, GH>,
        writer: &IndexWriter,
    ) -> Result<(), GraphError> {
        let edge_id = edge.edge.pid().as_u64();
        let src = edge.src().name();
        let dst = edge.dst().name();

        let edge_doc = self.create_document(edge_id, src, dst);
        writer.add_document(edge_doc)?;

        Ok(())
    }

    pub(crate) fn index_edges(
        &self,
        graph: &GraphStorage,
        path: Option<PathBuf>,
        index_spec: &IndexSpec,
    ) -> Result<(), GraphError> {
        let mut writer = self.entity_index.index.writer(100_000_000)?;
        (0..graph.count_edges())
            .into_par_iter()
            .try_for_each(|e_id| {
                let edge = graph.core_edge(EID(e_id));
                let e_view = EdgeView::new(graph, edge.out_ref());
                self.index_edge(e_view, &writer)?;
                Ok::<(), GraphError>(())
            })?;
        writer.commit()?;
        drop(writer);

        // Index const properties
        self.entity_index
            .index_edge_const_props(graph, index_spec, &path)?;

        // Index temporal properties
        self.entity_index
            .index_edge_temporal_props(graph, index_spec, &path)?;

        Ok(())
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: MaybeNew<EID>,
        t: TimeIndexEntry,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let eid_u64 = edge_id.inner().as_u64();

        // Check if the edge document is already in the index,
        // if it does skip adding a new doc for same edge
        edge_id
            .if_new(|eid| {
                let mut writer = self.entity_index.index.writer(100_000_000)?;
                let ese = graph.core_edge(eid);
                let src = graph.node_name(ese.src());
                let dst = graph.node_name(ese.dst());
                let edge_doc = self.create_document(eid_u64, src, dst);
                writer.add_document(edge_doc)?;
                writer.commit()?;
                Ok::<(), GraphError>(())
            })
            .transpose()?;

        let indexes = self.entity_index.temporal_property_indexes.read();
        for (prop_id, prop_value) in indexed_props(props, &indexes) {
            if let Some(index) = &indexes[prop_id] {
                let mut writer = index.index.writer(50_000_000)?;
                let prop_doc = index.create_edge_temporal_property_document(
                    t,
                    eid_u64,
                    layer_id,
                    &prop_value,
                )?;
                writer.add_document(prop_doc)?;
                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn add_edge_constant_properties(
        &self,
        edge_id: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let indexes = self.entity_index.const_property_indexes.read();
        for (prop_id, prop_value) in indexed_props(props, &indexes) {
            if let Some(index) = &indexes[prop_id] {
                let prop_doc = index.create_edge_const_property_document(
                    edge_id.as_u64(),
                    layer_id,
                    &prop_value,
                )?;
                let mut writer = index.index.writer(50_000_000)?;
                writer.add_document(prop_doc)?;
                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn update_edge_constant_properties(
        &self,
        edge_id: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let indexes = self.entity_index.const_property_indexes.read();
        for (prop_id, prop_value) in indexed_props(props, &indexes) {
            if let Some(index) = &indexes[prop_id] {
                let mut writer = index.index.writer(50_000_000)?;
                // Delete existing constant property document
                let term = Term::from_field_u64(index.entity_id_field, edge_id.as_u64());
                writer.delete_term(term);
                // Reindex constant properties
                let prop_doc = index.create_edge_const_property_document(
                    edge_id.as_u64(),
                    layer_id,
                    &prop_value,
                )?;
                writer.add_document(prop_doc)?;
                writer.commit()?;
            }
        }
        Ok(())
    }
}
