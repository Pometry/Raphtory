// search goes here

pub mod into_indexed;
#[cfg(feature = "proto")]
mod serialise;

use crate::{
    core::{
        entities::{
            nodes::node_ref::{AsNodeRef, NodeRef},
            EID, VID,
        },
        storage::{
            raw_edges::WriteLockedEdges,
            timeindex::{AsTime, TimeIndexEntry},
            WriteLockedNodes,
        },
        utils::errors::GraphError,
        PropType,
    },
    db::{
        api::{
            mutation::internal::{
                InheritPropertyAdditionOps, InternalAdditionOps, InternalDeletionOps,
            },
            storage::graph::{edges::edge_storage_ops::EdgeStorageOps, locked::WriteLockedGraph},
            view::{
                internal::{DynamicGraph, InheritViewOps, IntoDynamic, Static},
                Base, StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::GidType,
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew},
};
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use serde_json::json;
use std::{collections::HashMap, iter, ops::Deref, sync::Arc};
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{
        Field, JsonObjectOptions, Schema, SchemaBuilder, TextFieldIndexing, Value, FAST, INDEXED,
        STORED, TEXT,
    },
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Document, Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, TantivyError,
};

#[derive(Clone)]
pub struct IndexedGraph<G> {
    pub graph: G,
    pub(crate) node_index: Arc<Index>,
    pub(crate) edge_index: Arc<Index>,
    pub(crate) node_reader: IndexReader,
    pub(crate) edge_reader: IndexReader,
}

impl<G> Base for IndexedGraph<G> {
    type Base = G;

    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: StaticGraphViewOps> Static for IndexedGraph<G> {}

impl<G: StaticGraphViewOps> InheritViewOps for IndexedGraph<G> {}

//FIXME: should index constant properties on updates
impl<G: StaticGraphViewOps> InheritPropertyAdditionOps for IndexedGraph<G> {}

pub(in crate::search) mod fields {
    pub const TIME: &str = "time";
    pub const VERTEX_ID: &str = "node_id";
    pub const VERTEX_ID_REV: &str = "node_id_rev";
    pub const NAME: &str = "name";
    pub const NODE_TYPE: &str = "node_type";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "from";
    pub const DESTINATION: &str = "to";
    pub const CONSTANT_PROPERTIES: &str = "constant_properties";
    pub const TEMPORAL_PROPERTIES: &str = "temporal_properties";
}

impl<'graph, G: GraphViewOps<'graph>> From<G> for IndexedGraph<G> {
    fn from(graph: G) -> Self {
        Self::from_graph(&graph).expect("failed to generate index from graph")
    }
}

impl<G: GraphViewOps<'static> + IntoDynamic> IndexedGraph<G> {
    pub fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        IndexedGraph {
            graph: self.graph.into_dynamic(),
            node_index: self.node_index,
            edge_index: self.edge_index,
            node_reader: self.node_reader,
            edge_reader: self.edge_reader,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> IndexedGraph<G> {
    pub fn graph(&self) -> &G {
        &self.graph
    }

    fn node_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        schema.add_u64_field(fields::VERTEX_ID, FAST | STORED);
        schema.add_u64_field(fields::VERTEX_ID_REV, FAST | STORED);
        schema.add_text_field(fields::NAME, TEXT);
        schema.add_text_field(fields::NODE_TYPE, TEXT);
        schema.add_json_field(
            fields::CONSTANT_PROPERTIES,
            JsonObjectOptions::default()
                .set_stored()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                        ),
                ),
        );
        schema.add_json_field(
            fields::TEMPORAL_PROPERTIES,
            JsonObjectOptions::default()
                .set_stored()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                        ),
                ),
        );

        schema
    }

    fn edge_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        schema.add_u64_field(fields::EDGE_ID, FAST | STORED);
        schema.add_text_field(fields::SOURCE, TEXT);
        schema.add_text_field(fields::DESTINATION, TEXT);
        schema.add_json_field(
            fields::CONSTANT_PROPERTIES,
            JsonObjectOptions::default()
                .set_stored()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                        ),
                ),
        );
        schema.add_json_field(
            fields::TEMPORAL_PROPERTIES,
            JsonObjectOptions::default()
                .set_stored()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                        ),
                ),
        );

        schema
    }

    fn index_node_view<W: Deref<Target = IndexWriter>>(
        node: NodeView<G>,
        schema: &Schema,
        writer: &W,
    ) -> tantivy::Result<()> {
        let node_id: u64 = usize::from(node.node) as u64;
        let node_id_rev = u64::MAX - node_id;
        let node_name = node.name();
        let node_type = node.node_type().unwrap_or_else(|| ArcStr::from(""));

        let binding = node.properties().constant();
        let constant_properties = binding.iter();

        let binding = node.properties().temporal();
        let temporal_properties =
            Box::new(binding.iter().flat_map(|(key, values)| {
                values.into_iter().map(move |(t, v)| (t, key.clone(), v))
            }));

        let document = create_node_document(
            node_id,
            node_id_rev,
            node_name,
            node_type,
            constant_properties,
            temporal_properties,
            schema,
        )?;

        writer.add_document(document)?;
        Ok(())
    }

    fn index_nodes(g: &G) -> tantivy::Result<(Index, IndexReader)> {
        let schema = Self::node_schema_builder().build();

        let (index, reader) = Self::new_index(schema.clone(), Self::default_node_index_settings());

        let writer = Arc::new(parking_lot::RwLock::new(index.writer(100_000_000)?));

        let v_ids = (0..g.count_nodes()).collect::<Vec<_>>();

        v_ids.par_chunks(128).try_for_each(|v_ids| {
            let writer_lock = writer.clone();
            {
                let writer_guard = writer_lock.read();
                for v_id in v_ids {
                    if let Some(node) = g.node(NodeRef::new((*v_id).into())) {
                        Self::index_node_view(node, &schema, &writer_guard)?;
                    }
                }
            }

            Ok::<(), TantivyError>(())
        })?;

        let mut writer_guard = writer.write();
        writer_guard.commit()?;
        reader.reload()?;
        Ok((index, reader))
    }

    fn index_edge_view<W: Deref<Target = IndexWriter>>(
        e_ref: EdgeView<G, G>,
        schema: &Schema,
        writer: &W,
    ) -> tantivy::Result<()> {
        let edge_id = e_ref.edge.pid().as_u64();
        let src = e_ref.src().name();
        let dst = e_ref.dst().name();

        let binding = e_ref.properties().constant();
        let constant_properties = binding.iter();

        let binding = e_ref.properties().temporal();
        let temporal_properties =
            Box::new(binding.iter().flat_map(|(key, values)| {
                values.into_iter().map(move |(t, v)| (t, key.clone(), v))
            }));

        let document = create_edge_document(
            edge_id,
            src,
            dst,
            constant_properties,
            temporal_properties,
            schema,
        )?;

        writer.add_document(document)?;
        Ok(())
    }

    pub fn index_edges(g: &G) -> tantivy::Result<(Index, IndexReader)> {
        let schema = Self::edge_schema_builder().build();

        let (index, reader) = Self::new_index(schema.clone(), Self::default_edge_index_settings());

        let writer = Arc::new(parking_lot::RwLock::new(index.writer(100_000_000)?));

        let locked_g = g.core_graph();

        locked_g.edges_par(&g).try_for_each(|e_ref| {
            let writer_lock = writer.clone();
            {
                let writer_guard = writer_lock.read();
                let e_view = EdgeView::new(g.clone(), e_ref);
                Self::index_edge_view(e_view, &schema, &writer_guard)?;
            }
            Ok::<(), TantivyError>(())
        })?;

        let mut writer_guard = writer.write();
        writer_guard.commit()?;
        reader.reload()?;
        Ok((index, reader))
    }

    pub fn from_graph(g: &G) -> tantivy::Result<Self> {
        let (node_index, node_reader) = Self::index_nodes(g)?;
        let (edge_index, edge_reader) = Self::index_edges(g)?;

        Ok(IndexedGraph {
            graph: g.clone(),
            node_index: Arc::new(node_index),
            edge_index: Arc::new(edge_index),
            node_reader,
            edge_reader,
        })
    }

    fn default_node_index_settings() -> IndexSettings {
        IndexSettings::default()
    }

    fn default_edge_index_settings() -> IndexSettings {
        IndexSettings::default()
    }

    fn new_index(schema: Schema, index_settings: IndexSettings) -> (Index, IndexReader) {
        let index = Index::builder()
            .settings(index_settings)
            .schema(schema)
            .create_in_ram()
            .expect("failed to create index");

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .unwrap();

        let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .build();
        index.tokenizers().register("custom_default", tokenizer);
        (index, reader)
    }

    pub fn new(graph: G) -> Self {
        let schema = Self::node_schema_builder().build();
        let (node_index, node_reader) =
            Self::new_index(schema, Self::default_node_index_settings());

        let schema = Self::edge_schema_builder().build();
        let (edge_index, edge_reader) =
            Self::new_index(schema, Self::default_edge_index_settings());

        IndexedGraph {
            graph,
            node_index: Arc::new(node_index),
            edge_index: Arc::new(edge_index),
            node_reader,
            edge_reader,
        }
    }

    pub fn reload(&self) -> Result<(), GraphError> {
        self.node_reader.reload()?;
        self.edge_reader.reload()?;
        Ok(())
    }

    fn resolve_node_from_search_result(
        &self,
        node_id: Field,
        doc: TantivyDocument,
    ) -> Option<NodeView<G>> {
        let node_id: usize = doc
            .get_first(node_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let node_id = NodeRef::Internal(node_id.into());
        self.graph.node(node_id)
    }

    fn resolve_edge_from_search_result(
        &self,
        edge_id: Field,
        doc: TantivyDocument,
    ) -> Option<EdgeView<G, G>> {
        let edge_id: usize = doc
            .get_first(edge_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let core_edge = self.graph.core_edge(EID(edge_id));
        let layer_ids = self.graph.layer_ids();
        if !self.graph.filter_edge(core_edge.as_ref(), layer_ids) {
            return None;
        }
        if self.graph.nodes_filtered() {
            if !self.graph.filter_node(
                self.graph.core_node_entry(core_edge.src()).as_ref(),
                layer_ids,
            ) || !self.graph.filter_node(
                self.graph.core_node_entry(core_edge.dst()).as_ref(),
                layer_ids,
            ) {
                return None;
            }
        }
        let e_view = EdgeView::new(self.graph.clone(), core_edge.out_ref());
        Some(e_view)
    }

    fn node_parser(&self) -> Result<QueryParser, GraphError> {
        let temporal_properties = self
            .node_index
            .schema()
            .get_field(fields::TEMPORAL_PROPERTIES)?;
        let const_properties = self
            .node_index
            .schema()
            .get_field(fields::CONSTANT_PROPERTIES)?;
        Ok(QueryParser::for_index(
            &self.node_index,
            vec![temporal_properties, const_properties],
        ))
    }

    fn edge_parser(&self) -> Result<QueryParser, GraphError> {
        let temporal_properties = self
            .edge_index
            .schema()
            .get_field(fields::TEMPORAL_PROPERTIES)?;
        let const_properties = self
            .edge_index
            .schema()
            .get_field(fields::CONSTANT_PROPERTIES)?;
        Ok(QueryParser::for_index(
            &self.edge_index,
            vec![temporal_properties, const_properties],
        ))
    }

    pub fn search_nodes(
        &self,
        q: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let searcher = self.node_reader.searcher();
        let query_parser = self.node_parser()?;
        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let node_id = self.node_index.schema().get_field(fields::VERTEX_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn search_node_count(&self, q: &str) -> Result<usize, GraphError> {
        let searcher = self.node_reader.searcher();
        let query_parser = self.node_parser()?;
        let query = query_parser.parse_query(q)?;

        let count = searcher.search(&query, &tantivy::collector::Count)?;

        Ok(count)
    }

    pub fn search_edge_count(&self, q: &str) -> Result<usize, GraphError> {
        let searcher = self.edge_reader.searcher();
        let query_parser = self.edge_parser()?;
        let query = query_parser.parse_query(q)?;

        let count = searcher.search(&query, &tantivy::collector::Count)?;

        Ok(count)
    }

    pub fn search_edges(
        &self,
        q: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G, G>>, GraphError> {
        let searcher = self.edge_reader.searcher();
        let query_parser = self.edge_parser()?;
        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let edge_id = self.edge_index.schema().get_field(fields::EDGE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_edge_from_search_result(edge_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn fuzzy_search_nodes(
        &self,
        q: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let searcher = self.node_reader.searcher();
        let mut query_parser = self.node_parser()?;

        self.node_index
            .schema()
            .fields()
            .for_each(|(f, _)| query_parser.set_field_fuzzy(f, prefix, levenshtein_distance, true));

        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let node_id = self.node_index.schema().get_field(fields::VERTEX_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn fuzzy_search_edges(
        &self,
        q: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let searcher = self.edge_reader.searcher();
        let mut query_parser = self.edge_parser()?;
        self.edge_index
            .schema()
            .fields()
            .for_each(|(f, _)| query_parser.set_field_fuzzy(f, prefix, levenshtein_distance, true));

        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let edge_id = self.edge_index.schema().get_field(fields::EDGE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_edge_from_search_result(edge_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }
}

impl<G: StaticGraphViewOps + InternalAdditionOps> InternalAdditionOps for IndexedGraph<G> {
    #[inline]
    fn id_type(&self) -> Option<GidType> {
        self.graph.id_type()
    }

    #[inline]
    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError> {
        self.graph.write_lock()
    }

    #[inline]
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, GraphError> {
        self.graph.write_lock_nodes()
    }

    #[inline]
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, GraphError> {
        self.graph.write_lock_edges()
    }

    #[inline]
    fn num_shards(&self) -> Result<usize, GraphError> {
        self.graph.num_shards()
    }

    #[inline]
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph.next_event_id()
    }

    #[inline]
    fn read_event_id(&self) -> usize {
        self.graph.read_event_id()
    }

    #[inline]
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, GraphError> {
        self.graph.reserve_event_ids(num_ids)
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        self.graph.resolve_layer(layer)
    }

    #[inline]
    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<MaybeNew<VID>, GraphError> {
        self.graph.resolve_node(n)
    }

    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        self.graph.resolve_node_and_type(id, node_type)
    }

    #[inline]
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph.resolve_graph_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph.resolve_node_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph.resolve_edge_property(prop, dtype, is_static)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node_id = v.as_u64();
        let node_id_rev = u64::MAX - node_id;
        let node_name = self.graph.node_name(v);
        let node_type = self.graph.node_type(v).unwrap_or_else(|| ArcStr::from(""));
        let constant_properties = iter::empty::<(ArcStr, Prop)>();
        let temporal_properties: Box<dyn Iterator<Item = (i64, ArcStr, Prop)> + '_> =
            Box::new(props.iter().filter_map(move |(prop_id, prop)| {
                let prop_name = self.graph.node_meta().get_prop_name(*prop_id, false);
                Some((t.t(), prop_name, prop.clone()))
            }));

        let schema = self.node_index.schema();
        let mut writer = self.node_index.writer(50_000_000)?;

        let document = create_node_document(
            node_id,
            node_id_rev,
            node_name,
            node_type,
            Box::new(constant_properties),
            Box::new(temporal_properties),
            &schema,
        )?;
        writer.add_document(document)?;
        writer.commit()?;

        self.graph.internal_add_node(t, v, props)
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        let res = self.graph.internal_add_edge(t, src, dst, props, layer)?;

        let edge_id = self
            .edge(src, dst)
            .ok_or(GraphError::EdgeMissingError {
                src: self.graph.node_id(src),
                dst: self.graph.node_id(dst),
            })?
            .edge
            .pid()
            .as_u64();
        let src_name = self.graph.node_name(src);
        let dst_name = self.graph.node_name(dst);

        let constant_properties = iter::empty::<(ArcStr, Prop)>();
        let temporal_properties: Box<dyn Iterator<Item = (i64, ArcStr, Prop)> + '_> =
            Box::new(props.iter().filter_map(move |(prop_id, prop)| {
                let prop_name = self.graph.edge_meta().get_prop_name(*prop_id, false);
                Some((t.t(), prop_name, prop.clone()))
            }));

        let schema = self.edge_index.schema();
        let mut writer = self.edge_index.writer(50_000_000)?;

        let document = create_edge_document(
            edge_id,
            src_name,
            dst_name,
            Box::new(constant_properties),
            Box::new(temporal_properties),
            &schema,
        )?;
        writer.add_document(document)?;
        writer.commit()?;

        Ok(res)
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph.internal_add_edge_update(t, edge, props, layer)
    }
}

fn collect_constant_properties(
    properties: Box<dyn Iterator<Item = (ArcStr, Prop)> + '_>,
) -> serde_json::Map<String, serde_json::Value> {
    properties.map(|(k, v)| (k.to_string(), v.into())).collect()
}

fn collect_temporal_properties(
    properties: Box<dyn Iterator<Item = (i64, ArcStr, Prop)> + '_>,
) -> (Vec<i64>, serde_json::Value) {
    let mut temporal_properties_map: HashMap<i64, serde_json::Map<String, serde_json::Value>> =
        HashMap::new();
    let mut time: Vec<i64> = vec![];

    properties.for_each(|(t, k, v)| {
        temporal_properties_map
            .entry(t)
            .or_insert_with(serde_json::Map::new)
            .insert(k.to_string(), v.into());
    });

    let temporal_properties = temporal_properties_map
        .into_iter()
        .map(|(t, mut props)| {
            props.insert("time".to_string(), json!(t));
            time.push(t);
            serde_json::Value::Object(props)
        })
        .collect::<Vec<_>>();

    (time, serde_json::Value::Array(temporal_properties))
}

fn create_node_document(
    node_id: u64,
    node_id_rev: u64,
    node_name: String,
    node_type: ArcStr,
    constant_properties: Box<dyn Iterator<Item = (ArcStr, Prop)> + '_>,
    temporal_properties: Box<dyn Iterator<Item = (i64, ArcStr, Prop)> + '_>,
    schema: &Schema,
) -> tantivy::Result<TantivyDocument> {
    let constant_properties = collect_constant_properties(constant_properties);
    let (time, temporal_properties) = collect_temporal_properties(temporal_properties);

    let doc = json!({
        "time": time,
        "node_id": node_id,
        "node_id_rev": node_id_rev,
        "name": node_name,
        "node_type": node_type,
        "constant_properties": constant_properties,
        "temporal_properties": temporal_properties
    });

    let document = TantivyDocument::parse_json(schema, &doc.to_string())?;
    // println!("doc as json = {}", &document.to_json(schema));

    Ok(document)
}

fn create_edge_document(
    edge_id: u64,
    src: String,
    dst: String,
    constant_properties: Box<dyn Iterator<Item = (ArcStr, Prop)> + '_>,
    temporal_properties: Box<dyn Iterator<Item = (i64, ArcStr, Prop)> + '_>,
    schema: &Schema,
) -> tantivy::Result<TantivyDocument> {
    let constant_properties = collect_constant_properties(constant_properties);
    let (time, temporal_properties) = collect_temporal_properties(temporal_properties);

    let doc = json!({
        "time": time,
        "edge_id": edge_id,
        "from": src,
        "to": dst,
        "constant_properties": constant_properties,
        "temporal_properties": temporal_properties
    });

    let document = TantivyDocument::parse_json(schema, &doc.to_string())?;
    // println!("doc as json = {}", &document.to_json(schema));
    Ok(document)
}

impl<G: InternalDeletionOps> InternalDeletionOps for IndexedGraph<G> {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        self.graph.internal_delete_edge(t, src, dst, layer)
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph.internal_delete_existing_edge(t, eid, layer)
    }
}

impl<G: DeletionOps> DeletionOps for IndexedGraph<G> {}

#[cfg(test)]
mod search_tests {
    use super::*;
    use raphtory_api::core::utils::logging::global_info_logger;
    use std::time::SystemTime;
    use tantivy::{doc, DocAddress, Order};
    use tracing::info;

    #[test]
    fn test_custom_tokenizer() {
        let graph = Graph::new();
        graph
            .add_node(
                0,
                "0x0a5e1db3671faccd146404925bda5c59929f66c3",
                [
                    ("balance", Prop::F32(0.0011540000414242968)),
                    (
                        "cluster_id",
                        Prop::Str(ArcStr::from("0x0a5e1db3671faccd146404925bda5c59929f66c3")),
                    ),
                ],
                Some("center"),
            )
            .unwrap();
        graph
            .add_node(
                1,
                "0x0a5e1db3671faccd146404925bda5c59929f66c3",
                [
                    ("balance", Prop::F32(0.9)),
                    (
                        "cluster_id",
                        Prop::Str(ArcStr::from("0x0a5e1db3671faccd146404925bda5c59929f66c3")),
                    ),
                ],
                Some("center"),
            )
            .unwrap();
        graph
            .add_node(
                1,
                "0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c",
                [
                    ("balance", Prop::F32(0.0)),
                    (
                        "cluster_id",
                        Prop::Str(ArcStr::from("0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c")),
                    ),
                ],
                Some("collapsed"),
            )
            .unwrap();
        graph
            .add_node(
                2,
                "0x941900204497226bede1324742eb83af6b0b5eec",
                [
                    ("balance", Prop::F32(0.0)),
                    (
                        "cluster_id",
                        Prop::Str(ArcStr::from("0x941900204497226bede1324742eb83af6b0b5eec")),
                    ),
                ],
                Some("collapsed"),
            )
            .unwrap();

        graph
            .node("0x0a5e1db3671faccd146404925bda5c59929f66c3")
            .unwrap()
            .add_constant_properties([("firenation", Prop::Bool(true))])
            .unwrap();
        graph
            .node("0x0a5e1db3671faccd146404925bda5c59929f66c3")
            .unwrap()
            .add_constant_properties([("watertribe", Prop::Bool(false))])
            .unwrap();
        graph
            .node("0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c")
            .unwrap()
            .add_constant_properties([("firenation", Prop::Bool(false))])
            .unwrap();
        graph
            .node("0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c")
            .unwrap()
            .add_constant_properties([("watertribe", Prop::Bool(false))])
            .unwrap();
        graph
            .node("0x941900204497226bede1324742eb83af6b0b5eec")
            .unwrap()
            .add_constant_properties([("firenation", Prop::Bool(false))])
            .unwrap();
        graph
            .node("0x941900204497226bede1324742eb83af6b0b5eec")
            .unwrap()
            .add_constant_properties([("watertribe", Prop::Bool(true))])
            .unwrap();

        let ig: IndexedGraph<Graph> = graph.into();

        let mut results = ig
            .search_nodes("node_type:collapsed", 5, 0)
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        assert_eq!(
            results,
            vec![
                "0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c",
                "0x941900204497226bede1324742eb83af6b0b5eec",
            ]
        );

        let mut results = ig
            .search_nodes("temporal_properties.balance:0.0", 5, 0)
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        assert_eq!(
            results,
            vec![
                "0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c",
                "0x941900204497226bede1324742eb83af6b0b5eec",
            ]
        );

        let mut results = ig
            .search_nodes(
                "temporal_properties.cluster_id:\"0x941900204497226bede1324742eb83af6b0b5eec\"",
                5,
                0,
            )
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        assert_eq!(results, vec!["0x941900204497226bede1324742eb83af6b0b5eec"]);

        let results = ig
            .search_nodes(
                "node_type:collapsed AND temporal_properties.cluster_id:\"0x941900204497226bede1324742eb83af6b0b5eec\"",
                5,
                0,
            )
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        assert_eq!(results, vec!["0x941900204497226bede1324742eb83af6b0b5eec"]);

        let mut results = ig
            .search_nodes(
                "node_type:collapsed OR temporal_properties.cluster_id:\"0x941900204497226bede1324742eb83af6b0b5eec\"",
                5,
                0,
            )
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        assert_eq!(
            results,
            vec![
                "0x1c5e2c8e97f34a5ca18dc7370e2bfc0da3baed5c",
                "0x941900204497226bede1324742eb83af6b0b5eec",
            ]
        );
    }

    #[test]
    fn index_numeric_props() {
        let graph = Graph::new();
        graph
            .add_node(
                1,
                "Blerg",
                [
                    ("age".to_string(), Prop::U64(42)),
                    ("balance".to_string(), Prop::I64(-1234)),
                ],
                None,
            )
            .expect("failed to add node");

        let ig: IndexedGraph<Graph> = graph.into();

        let results = ig
            .search_nodes("temporal_properties.age:42", 5, 0)
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();

        assert_eq!(results, vec!["Blerg"]);
    }

    #[test]
    #[cfg(feature = "proto")]
    #[ignore = "this test is for experiments with the jira graph"]
    fn load_jira_graph() -> Result<(), GraphError> {
        global_info_logger();
        let graph = Graph::decode("/tmp/graphs/jira").expect("failed to load graph");
        assert!(graph.count_nodes() > 0);

        let now = SystemTime::now();

        let index_graph: IndexedGraph<Graph> = graph.into();
        let elapsed = now.elapsed().unwrap().as_secs();
        info!("indexing took: {:?}", elapsed);

        let issues = index_graph.search_nodes("name:'DEV-1690'", 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        info!("names: {:?}", names);

        Ok(())
    }

    #[test]
    fn create_indexed_graph_from_existing_graph() {
        let graph = Graph::new();
        graph
            .add_node(
                1,
                "Gandalf",
                [("kind".to_string(), Prop::str("Wizard"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                2,
                "Frodo",
                [
                    ("kind".to_string(), Prop::str("Hobbit")),
                    ("has_ring".to_string(), Prop::str("yes")),
                ],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                2,
                "Merry",
                [("kind".to_string(), Prop::str("Hobbit"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                4,
                "Gollum",
                [("kind".to_string(), Prop::str("Creature"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                9,
                "Gollum",
                [("has_ring".to_string(), Prop::str("yes"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                9,
                "Frodo",
                [("has_ring".to_string(), Prop::str("no"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                10,
                "Frodo",
                [("has_ring".to_string(), Prop::str("yes"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                10,
                "Gollum",
                [("has_ring".to_string(), Prop::str("no"))],
                None,
            )
            .expect("add node failed");

        let indexed_graph: IndexedGraph<Graph> =
            IndexedGraph::from_graph(&graph).expect("failed to generate index from graph");
        indexed_graph.reload().expect("failed to reload index");

        let results = indexed_graph
            .search_nodes("temporal_properties.kind:Hobbit", 10, 0)
            .expect("search failed");
        let mut actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let mut expected = vec!["Frodo", "Merry"];
        // FIXME: this is not deterministic
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);

        let results = indexed_graph
            .search_nodes("temporal_properties.kind:Wizard", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        let results = indexed_graph
            .search_nodes("temporal_properties.kind:Creature", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);

        // search by name
        let results = indexed_graph
            .search_nodes("name:Gollum", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_name() {
        let graph = IndexedGraph::new(Graph::new());
        graph
            .add_node(1, "Gandalf", NO_PROPS, None)
            .expect("add node failed");

        graph.reload().expect("reload failed");

        let nodes = graph
            .search_nodes(r#"name:Gandalf"#, 10, 0)
            .expect("search failed");

        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_description() {
        let graph = IndexedGraph::new(Graph::new());
        graph
            .add_node(
                1,
                "Bilbo",
                [("description".to_string(), Prop::str("A hobbit"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                2,
                "Gandalf",
                [("description".to_string(), Prop::str("A wizard"))],
                None,
            )
            .expect("add node failed");

        graph.reload().expect("reload failed");

        // Find the Wizard
        let nodes = graph
            .search_nodes(r#"temporal_properties.description:"A wizard""#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        // Find the Hobbit
        let nodes = graph
            .search_nodes(r#"temporal_properties.description:'hobbit'"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Bilbo"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_node_type() {
        let graph = IndexedGraph::new(Graph::new());

        graph
            .add_node(1, "Gandalf", NO_PROPS, Some("wizard"))
            .expect("add node failed");

        graph
            .add_node(2, "Bilbo", NO_PROPS, None)
            .expect("add node failed");

        graph.reload().expect("reload failed");

        let nodes = graph
            .search_nodes(r#"node_type:wizard"#, 10, 0)
            .expect("search failed");

        let actual = nodes
            .into_iter()
            .map(|v| v.node_type().unwrap().to_string())
            .collect::<Vec<_>>();
        let expected = vec!["wizard"];

        assert_eq!(actual, expected);

        let nodes = graph
            .search_nodes(r#"node_type:''"#, 10, 0)
            .expect("search failed");

        let actual = nodes
            .into_iter()
            .map(|v| v.node_type().unwrap().to_string())
            .collect::<Vec<_>>();
        let expected: Vec<String> = vec![];

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_description_and_time() {
        let graph = IndexedGraph::new(Graph::new());
        graph
            .add_node(
                1,
                "Gandalf",
                [("description".to_string(), Prop::str("The wizard"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                2,
                "Saruman",
                [("description".to_string(), Prop::str("Another wizard"))],
                None,
            )
            .expect("add node failed");

        graph.reload().expect("reload failed");

        // Find Saruman
        let nodes = graph
            .search_nodes(
                r#"temporal_properties.description:wizard AND time:[2 TO 5]"#,
                10,
                0,
            )
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Saruman"];
        assert_eq!(actual, expected);

        // Find Gandalf
        let nodes = graph
            .search_nodes(
                r#"temporal_properties.description:'wizard' AND time:[1 TO 2}"#,
                10,
                0,
            )
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        // Find both wizards
        let nodes = graph
            .search_nodes(
                r#"temporal_properties.description:'wizard' AND time:[1 TO 100]"#,
                10,
                0,
            )
            .expect("search failed");
        let mut actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let mut expected = vec!["Gandalf", "Saruman"];

        // FIXME: this is not deterministic
        actual.sort();
        expected.sort();

        assert_eq!(actual, expected);
    }

    #[test]
    fn search_by_edge_props_indexed_graph() {
        let g = IndexedGraph::new(Graph::new());
        g.add_edge(
            1,
            "Frodo",
            "Gandalf",
            [("type".to_string(), Prop::str("friends"))],
            None,
        )
        .expect("add edge failed");
        g.add_edge(
            1,
            "Frodo",
            "Gollum",
            [("type".to_string(), Prop::str("enemies"))],
            None,
        )
        .expect("add edge failed");

        g.reload().unwrap();

        let results = g.search_edges("from:Frodo", 5, 0).expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();

        let results = g
            .search_edges(r#"temporal_properties.type:'friends'"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gandalf".to_string())];

        assert_eq!(actual, expected);

        let results = g
            .search_edges(r#"temporal_properties.type:'enemies'"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gollum".to_string())];

        assert_eq!(actual, expected);
    }

    #[test]
    fn search_by_edge_props_graph_indexed() {
        let g = Graph::new();
        g.add_edge(
            1,
            "Frodo",
            "Gandalf",
            [("type".to_string(), Prop::str("friends"))],
            None,
        )
        .expect("add edge failed");
        g.add_edge(
            1,
            "Frodo",
            "Gollum",
            [("type".to_string(), Prop::str("enemies"))],
            None,
        )
        .expect("add edge failed");

        let g: IndexedGraph<Graph> = g.into();

        let results = g.search_edges("from:Frodo", 5, 0).expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();

        let results = g
            .search_edges(r#"temporal_properties.type:'friends'"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gandalf".to_string())];

        assert_eq!(actual, expected);

        let results = g
            .search_edges(r#"temporal_properties.type:'enemies'"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gollum".to_string())];

        assert_eq!(actual, expected);
    }

    #[test]
    fn search_by_edge_src_dst() {
        let g = Graph::new();
        g.add_edge(1, "Frodo", "Gandalf", NO_PROPS, None)
            .expect("add edge failed");
        g.add_edge(1, "Frodo", "Gollum", NO_PROPS, None)
            .expect("add edge failed");

        let ig: IndexedGraph<Graph> = g.into();

        let results = ig
            .search_edges(r#"from:Frodo"#, 10, 0)
            .expect("search failed");
        let mut actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let mut expected = vec![
            ("Frodo".to_string(), "Gandalf".to_string()),
            ("Frodo".to_string(), "Gollum".to_string()),
        ];

        actual.sort();
        expected.sort();

        assert_eq!(actual, expected);

        // search by destination
        let results = ig.search_edges("to:gollum", 10, 0).expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gollum".to_string())];

        assert_eq!(actual, expected);
    }

    #[test]
    fn tantivy_101() {
        let node_index_props = vec!["name"];

        let mut schema = Schema::builder();

        for prop in node_index_props {
            schema.add_text_field(prop.as_ref(), TEXT);
        }

        // ensure time is part of the index
        schema.add_u64_field("time", INDEXED | STORED);
        // ensure we add node_id as stored to get back the node id after the search
        schema.add_u64_field("node_id", FAST | STORED);

        let index = Index::create_in_ram(schema.build());

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .unwrap();

        {
            let mut writer = index.writer(50_000_000).unwrap();

            let name = index.schema().get_field("name").unwrap();
            let time = index.schema().get_field("time").unwrap();
            let node_id = index.schema().get_field("node_id").unwrap();

            writer
                .add_document(doc!(name => "Gandalf", time => 1u64, node_id => 0u64))
                .expect("add document failed");

            writer.commit().expect("commit failed");
        }

        reader.reload().unwrap();

        let searcher = reader.searcher();

        let query_parser = tantivy::query::QueryParser::for_index(&index, vec![]);
        let query = query_parser.parse_query(r#"name:"gandalf""#).unwrap();

        let ranking =
            TopDocs::with_limit(10).order_by_fast_field(fields::VERTEX_ID.to_string(), Order::Asc);
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &ranking).unwrap();

        assert!(!top_docs.is_empty());
    }

    #[test]
    fn property_name_on_node_does_not_crash() {
        let g = Graph::new();
        g.add_node(0, "test", [("name", "test")], None).unwrap();
        let _gi: IndexedGraph<_> = g.into();
    }
}
