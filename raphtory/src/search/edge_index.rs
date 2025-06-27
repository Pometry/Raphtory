use crate::{
    core::{entities::EID, storage::timeindex::TimeIndexEntry},
    db::{api::view::IndexSpec, graph::edge::EdgeView},
    errors::GraphError,
    prelude::*,
    search::{
        entity_index::EntityIndex,
        fields::{DESTINATION, DESTINATION_TOKENIZED, EDGE_ID, SOURCE, SOURCE_TOKENIZED},
        resolve_props, TOKENIZER,
    },
};
use ahash::HashSet;
use raphtory_api::core::storage::dict_mapper::MaybeNew;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage},
    layer_ops::InternalLayerOps,
};
use rayon::prelude::ParallelIterator;
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
    Document, IndexWriter, TantivyDocument, TantivyError,
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
        let searcher = self.entity_index.get_reader()?.searcher();
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

    fn collect_temporal_properties<
        'a,
        'graph,
        G: GraphViewOps<'graph> + 'a,
        GH: GraphViewOps<'graph> + 'a,
    >(
        &'a self,
        edge: &EdgeView<G, GH>,
    ) -> impl Iterator<Item = (usize, Prop)> + 'a {
        edge.properties()
            .temporal()
            .into_iter()
            .flat_map(|(_, values)| {
                let pid = values.id;
                values.into_iter().map(move |(_, v)| (pid, v))
            })
    }

    fn index_edge_c(
        &self,
        edge_id: EID,
        layer_id: usize,
        writers: &mut [Option<IndexWriter>],
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let edge_id = edge_id.as_u64();
        self.entity_index
            .index_edge_const_properties(edge_id, layer_id, writers, props)?;
        self.entity_index.commit_writers(writers)
    }

    fn index_edge_t(
        &self,
        graph: &GraphStorage,
        time: TimeIndexEntry,
        edge_id: MaybeNew<EID>,
        layer_id: usize,
        writer: &mut IndexWriter,
        temporal_writers: &mut [Option<IndexWriter>],
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let eid_u64 = edge_id.inner().as_u64();
        self.entity_index.index_edge_temporal_properties(
            time,
            eid_u64,
            layer_id,
            temporal_writers,
            props,
        )?;

        // Check if the edge document is already in the index,
        // if it does skip adding a new doc for same edge
        edge_id
            .if_new(|eid| {
                let ese = graph.core_edge(eid);
                let src = graph.node_name(ese.src());
                let dst = graph.node_name(ese.dst());
                let edge_doc = self.create_document(eid_u64, src, dst);
                writer.add_document(edge_doc)?;
                Ok::<(), GraphError>(())
            })
            .transpose()?;

        self.entity_index.commit_writers(temporal_writers)?;
        writer.commit()?;

        Ok(())
    }

    fn index_edge<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        graph: &GraphStorage,
        edge: EdgeView<G, GH>,
        writer: &IndexWriter,
        const_writers: &[Option<IndexWriter>],
        temporal_writers: &[Option<IndexWriter>],
    ) -> Result<(), GraphError> {
        let edge_id = edge.edge.pid().as_u64();
        let src = edge.src().name();
        let dst = edge.dst().name();

        for edge in edge.explode_layers() {
            let layer_name = edge
                .layer_name()
                .map_err(|e| TantivyError::InternalError(e.to_string()))?
                .to_string();

            if let Some(layer_id) = graph.get_layer_id(&layer_name) {
                self.entity_index.index_edge_const_properties(
                    edge_id,
                    layer_id,
                    const_writers,
                    &*edge
                        .properties()
                        .constant()
                        .iter_id()
                        .collect::<Vec<(usize, Prop)>>(),
                )?;

                for edge in edge.explode() {
                    if let Ok(time) = edge.time_and_index() {
                        let temporal_properties = self.collect_temporal_properties(&edge);
                        self.entity_index.index_edge_temporal_properties(
                            time,
                            edge_id,
                            layer_id,
                            temporal_writers,
                            &*temporal_properties.collect::<Vec<(usize, Prop)>>(),
                        )?;
                    }
                }
            };
        }

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
        // Initialize property indexes and get their writers
        let const_properties_index_path = path.as_deref().map(|p| p.join("const_properties"));
        let mut const_writers = self.entity_index.initialize_edge_const_property_indexes(
            graph.edge_meta().const_prop_meta(),
            &const_properties_index_path,
            &index_spec.edge_const_props,
        )?;

        let temporal_properties_index_path = path.as_deref().map(|p| p.join("temporal_properties"));
        let mut temporal_writers = self
            .entity_index
            .initialize_edge_temporal_property_indexes(
                graph.edge_meta().temporal_prop_meta(),
                &temporal_properties_index_path,
                &index_spec.edge_temp_props,
            )?;

        let mut writer = self.entity_index.index.writer(100_000_000)?;
        let locked_edges = graph.core_edges();
        locked_edges
            .par_iter(&graph.layer_ids())
            .try_for_each(|e| {
                let e_ref = e.out_ref();
                {
                    let e_view = EdgeView::new(graph.clone(), e_ref);
                    self.index_edge(graph, e_view, &writer, &const_writers, &temporal_writers)?;
                }
                Ok::<(), GraphError>(())
            })?;

        // Commit writers
        self.entity_index.commit_writers(&mut const_writers)?;
        self.entity_index.commit_writers(&mut temporal_writers)?;
        writer.commit()?;

        // Drop writers
        drop(const_writers);
        drop(temporal_writers);
        drop(writer);

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
        let mut temporal_writers = self.entity_index.get_temporal_property_writers(props)?;

        let mut writer = self.entity_index.index.writer(100_000_000)?;
        self.index_edge_t(
            graph,
            t,
            edge_id,
            layer_id,
            &mut writer,
            &mut temporal_writers,
            props,
        )?;

        Ok(())
    }

    pub(crate) fn add_edge_constant_properties(
        &self,
        edge_id: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut const_writers = self.entity_index.get_const_property_writers(props)?;

        self.index_edge_c(edge_id, layer_id, &mut const_writers, props)?;

        Ok(())
    }

    pub(crate) fn update_edge_constant_properties(
        &self,
        edge_id: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut const_writers = self.entity_index.get_const_property_writers(props)?;

        // Delete existing constant property document
        self.entity_index.delete_const_properties_index_docs(
            edge_id.as_u64(),
            &mut const_writers,
            props,
        )?;

        // Reindex the edge's constant properties
        self.index_edge_c(edge_id, layer_id, &mut const_writers, props)?;

        Ok(())
    }
}
