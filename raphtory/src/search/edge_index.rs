use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::internal::ConstPropertiesOps,
            storage::graph::storage_ops::GraphStorage,
            view::internal::{core_ops::CoreGraphOps, InternalLayerOps},
        },
        graph::{edge::EdgeView, edges::Edges},
    },
    prelude::*,
    search::{
        fields, index_properties, initialize_property_indexes, new_index,
        property_index::PropertyIndex, TOKENIZER,
    },
};
use raphtory_api::core::storage::arc_str::ArcStr;
use rayon::prelude::ParallelIterator;
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
    Document, Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, TantivyError, Term,
};

#[derive(Clone)]
pub struct EdgeIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
    pub(crate) constant_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    pub(crate) temporal_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
}

impl Debug for EdgeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeIndex")
            .field("index", &self.index)
            .finish()
    }
}

impl EdgeIndex {
    pub(crate) fn new() -> Self {
        let schema = Self::schema_builder().build();
        let (index, reader) = new_index(schema, IndexSettings::default());
        EdgeIndex {
            index: Arc::new(index),
            reader,
            constant_property_indexes: Arc::new(RwLock::new(Vec::new())),
            temporal_property_indexes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(1000))?;

        println!("Total edge doc count: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Edge doc: {:?}", doc.to_json(searcher.schema()));
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
        let mut schema = Schema::builder();
        schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
        schema.add_text_field(
            fields::SOURCE,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema.add_text_field(
            fields::DESTINATION,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema.add_text_field(
            fields::EDGE_TYPE,
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema
    }

    pub fn get_edge_field(&self, field_name: &str) -> tantivy::Result<Field> {
        self.index.schema().get_field(field_name)
    }

    fn create_document<'a>(
        &self,
        edge_id: u64,
        src: String,
        dst: String,
    ) -> tantivy::Result<TantivyDocument> {
        let schema = self.index.schema();

        let doc = json!({
            "edge_id": edge_id,
            "from": src,
            "to": dst,
        });

        let document = TantivyDocument::parse_json(&schema, &doc.to_string())?;
        // println!("Edge doc as json = {}", &document.to_json(schema));

        Ok(document)
    }

    fn collect_constant_properties<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        edge: &EdgeView<G, GH>,
    ) -> Vec<(ArcStr, usize, Prop)> {
        edge.properties()
            .constant()
            .iter()
            .filter_map(|(k, p)| edge.get_const_prop_id(k.as_ref()).map(|id| (k, id, p)))
            .collect()
    }

    fn collect_temporal_properties<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        edge: &EdgeView<G, GH>,
    ) -> BTreeMap<i64, Vec<(ArcStr, usize, Prop)>> {
        edge.properties()
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

    fn index_edge<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        &self,
        graph: &GraphStorage,
        edge: EdgeView<G, GH>,
        writer: &IndexWriter,
        const_writers: &[Option<IndexWriter>],
        temporal_writers: &[Option<IndexWriter>],
    ) -> tantivy::Result<()> {
        let edge_id = edge.edge.pid().as_u64();
        let src = edge.src().name();
        let dst = edge.dst().name();

        for edge in edge.explode_layers() {
            let layer_name = edge
                .layer_name()
                .map_err(|e| TantivyError::InternalError(e.to_string()))?
                .to_string();

            let layer_id = graph.get_layer_id(&layer_name);

            let constant_properties: Vec<(ArcStr, usize, Prop)> =
                self.collect_constant_properties(&edge);

            let temporal_properties: BTreeMap<i64, Vec<(ArcStr, usize, Prop)>> =
                self.collect_temporal_properties(&edge);

            for (time, temp_props) in &temporal_properties {
                index_properties(
                    constant_properties.iter().cloned(),
                    self.constant_property_indexes.write()?,
                    *time,
                    fields::EDGE_ID,
                    edge_id,
                    layer_id,
                    const_writers,
                )?;
                index_properties(
                    temp_props.iter().cloned(),
                    self.temporal_property_indexes.write()?,
                    *time,
                    fields::EDGE_ID,
                    edge_id,
                    layer_id,
                    temporal_writers,
                )?;
            }
        }

        // Check if the edge document is already in the index,
        // if it does skip adding a new doc for same edge
        let schema = self.index.schema();
        let field_id = schema.get_field(fields::EDGE_ID)?;

        let query = TermQuery::new(
            Term::from_field_u64(field_id, edge_id),
            IndexRecordOption::Basic,
        );

        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
        if !top_docs.is_empty() {
            return Ok(());
        }

        let edge_doc = self.create_document(edge_id, src, dst)?;
        writer.add_document(edge_doc)?;

        Ok(())
    }

    pub(crate) fn index_edges(graph: &GraphStorage) -> tantivy::Result<EdgeIndex> {
        let edge_index = EdgeIndex::new();

        // Initialize property indexes and get their writers
        let mut const_writers = initialize_property_indexes(
            edge_index.constant_property_indexes.clone(),
            graph.edge_meta().const_prop_meta(),
        )?;
        let mut temporal_writers = initialize_property_indexes(
            edge_index.temporal_property_indexes.clone(),
            graph.edge_meta().temporal_prop_meta(),
        )?;

        let mut writer = edge_index.index.writer(100_000_000)?;

        let locked_g = graph.core_graph();
        locked_g.edges_par(&graph).try_for_each(|e_ref| {
            {
                let e_view = EdgeView::new(graph.clone(), e_ref);
                edge_index.index_edge(graph, e_view, &writer, &const_writers, &temporal_writers)?;
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
            let const_indexes = edge_index.constant_property_indexes.read()?;
            for property_index_option in const_indexes.iter().flatten() {
                property_index_option.reader.reload()?;
            }
        }
        {
            let temporal_indexes = edge_index.temporal_property_indexes.read()?;
            for property_index_option in temporal_indexes.iter().flatten() {
                property_index_option.reader.reload()?;
            }
        }
        edge_index.reader.reload()?;

        Ok(edge_index)
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        let edge = graph
            .edge(src, dst)
            .expect("Edge for internal id should exist.")
            .at(t.t());

        let const_writers = initialize_property_indexes(
            self.constant_property_indexes.clone(),
            graph.node_meta().const_prop_meta(),
        )?;
        let temporal_writers = initialize_property_indexes(
            self.temporal_property_indexes.clone(),
            graph.node_meta().temporal_prop_meta(),
        )?;

        let writer = Arc::new(parking_lot::RwLock::new(self.index.writer(100_000_000)?));
        let mut writer_guard = writer.write();
        self.index_edge(
            graph,
            edge,
            &writer_guard,
            &const_writers,
            &temporal_writers,
        )?;
        writer_guard.commit()?;
        self.reader.reload()?;

        Ok(())
    }
}
