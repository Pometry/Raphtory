use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::internal::ConstPropertiesOps, storage::graph::storage_ops::GraphStorage,
            view::internal::core_ops::CoreGraphOps,
        },
        graph::{edge::EdgeView},
    },
    prelude::*,
    search::{fields, index_properties, new_index, property_index::PropertyIndex, TOKENIZER},
};
use raphtory_api::core::storage::arc_str::ArcStr;
use rayon::prelude::ParallelIterator;
use serde_json::json;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Formatter},
    ops::Deref,
    sync::{Arc, RwLock},
};
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, TermQuery},
    schema::{
        IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, Value, FAST,
        INDEXED, STORED,
    },
    Document, Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, TantivyError, Term,
};

#[derive(Clone)]
pub struct EdgeIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
    pub(crate) property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
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
            property_indexes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(100))?;

        println!("Total edge doc count: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Edge doc: {:?}", doc.to_json(searcher.schema()));
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

    fn index_edge<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        W: Deref<Target = IndexWriter>,
    >(
        &self,
        edge: EdgeView<G, GH>,
        writer: &W,
    ) -> tantivy::Result<()> {
        let edge_id = edge.edge.pid().as_u64();
        let src = edge.src().name();
        let dst = edge.dst().name();

        let constant_properties: Vec<(ArcStr, usize, Prop)> =
            self.collect_constant_properties(&edge);

        let temporal_properties: BTreeMap<i64, Vec<(ArcStr, usize, Prop)>> =
            self.collect_temporal_properties(&edge);

        for (time, temp_props) in &temporal_properties {
            let properties = constant_properties
                .iter()
                .cloned()
                .chain(temp_props.iter().cloned());

            index_properties(
                properties,
                self.property_indexes.write()?,
                *time,
                fields::EDGE_ID,
                edge_id
            )?;
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

    pub(crate) fn index_edges(g: &GraphStorage) -> tantivy::Result<EdgeIndex> {
        let edge_index = EdgeIndex::new();

        let writer = Arc::new(parking_lot::RwLock::new(
            edge_index.index.writer(100_000_000)?,
        ));

        let locked_g = g.core_graph();

        locked_g.edges_par(&g).try_for_each(|e_ref| {
            let writer_lock = writer.clone();
            {
                let writer_guard = writer_lock.read();
                let e_view = EdgeView::new(g.clone(), e_ref);
                edge_index.index_edge(e_view, &writer_guard)?;
            }
            Ok::<(), TantivyError>(())
        })?;

        let mut writer_guard = writer.write();
        writer_guard.commit()?;

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

        let writer = Arc::new(parking_lot::RwLock::new(self.index.writer(100_000_000)?));
        let mut writer_guard = writer.write();
        self.index_edge(edge, &writer_guard)?;
        writer_guard.commit()?;
        self.reader.reload()?;

        Ok(())
    }
}
