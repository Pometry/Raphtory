use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, EID, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            storage::graph::{edges::edge_storage_ops::EdgeStorageOps, storage_ops::GraphStorage},
            view::internal::core_ops::CoreGraphOps,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use serde_json::json;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    iter,
    ops::Deref,
    sync::Arc,
};
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{
        Field, IndexRecordOption, JsonObjectOptions, Schema, SchemaBuilder, TextFieldIndexing,
        TextOptions, Value, FAST, INDEXED, STORED,
    },
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, TantivyError,
};

#[derive(Copy, Clone)]
pub struct Searcher<'a> {
    pub(crate) index: &'a GraphIndex,
}

impl<'a> Searcher<'a> {
    fn resolve_node_from_search_result<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        node_id: Field,
        doc: TantivyDocument,
    ) -> Option<NodeView<G>> {
        let node_id: usize = doc
            .get_first(node_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let node_id = NodeRef::Internal(node_id.into());
        graph.node(node_id)
    }

    fn resolve_edge_from_search_result<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        edge_id: Field,
        doc: TantivyDocument,
    ) -> Option<EdgeView<G>> {
        let edge_id: usize = doc
            .get_first(edge_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let core_edge = graph.core_edge(EID(edge_id));
        let layer_ids = graph.layer_ids();
        if !graph.filter_edge(core_edge.as_ref(), layer_ids) {
            return None;
        }
        if graph.nodes_filtered() {
            if !graph.filter_node(graph.core_node_entry(core_edge.src()).as_ref(), layer_ids)
                || !graph.filter_node(graph.core_node_entry(core_edge.dst()).as_ref(), layer_ids)
            {
                return None;
            }
        }
        let e_view = EdgeView::new(graph.clone(), core_edge.out_ref());
        Some(e_view)
    }

    // TODO: impl missing queries
    // TODO: Fix filtering

    // TODO: Fix time filtering
    pub fn search_nodes<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        q: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let searcher = self.index.node_reader.searcher();
        let query_parser = self.index.node_parser()?;
        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let node_id = self
            .index
            .node_index
            .schema()
            .get_field(fields::VERTEX_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(graph, node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn search_node_count(&self, q: &str) -> Result<usize, GraphError> {
        let searcher = self.index.node_reader.searcher();
        let query_parser = self.index.node_parser()?;
        let query = query_parser.parse_query(q)?;

        let count = searcher.search(&query, &tantivy::collector::Count)?;

        Ok(count)
    }

    pub fn search_edges<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        q: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let searcher = self.index.edge_reader.searcher();
        let query_parser = self.index.edge_parser()?;
        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let edge_id = self.index.edge_index.schema().get_field(fields::EDGE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_edge_from_search_result(graph, edge_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn search_edge_count(&self, q: &str) -> Result<usize, GraphError> {
        let searcher = self.index.edge_reader.searcher();
        let query_parser = self.index.edge_parser()?;
        let query = query_parser.parse_query(q)?;

        let count = searcher.search(&query, &tantivy::collector::Count)?;

        Ok(count)
    }

    pub fn fuzzy_search_nodes<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        q: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let searcher = self.index.node_reader.searcher();
        let mut query_parser = self.index.node_parser()?;

        self.index
            .node_index
            .schema()
            .fields()
            .for_each(|(f, _)| query_parser.set_field_fuzzy(f, prefix, levenshtein_distance, true));

        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let node_id = self
            .index
            .node_index
            .schema()
            .get_field(fields::VERTEX_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(graph, node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn fuzzy_search_edges<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        q: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let searcher = self.index.edge_reader.searcher();
        let mut query_parser = self.index.edge_parser()?;
        self.index
            .edge_index
            .schema()
            .fields()
            .for_each(|(f, _)| query_parser.set_field_fuzzy(f, prefix, levenshtein_distance, true));

        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit).and_offset(offset);

        let top_docs = searcher.search(&query, &ranking)?;

        let edge_id = self.index.edge_index.schema().get_field(fields::EDGE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_edge_from_search_result(graph, edge_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }
}

#[derive(Clone)]
pub struct GraphIndex {
    pub(crate) node_index: Arc<Index>,
    pub(crate) edge_index: Arc<Index>,
    pub(crate) node_reader: IndexReader,
    pub(crate) edge_reader: IndexReader,
}

impl Debug for GraphIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphIndex")
            .field("node_index", &self.node_index)
            .field("edge_index", &self.edge_index)
            .finish()
    }
}

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

impl<'a> TryFrom<&'a GraphStorage> for GraphIndex {
    type Error = GraphError;

    fn try_from(graph: &GraphStorage) -> Result<Self, Self::Error> {
        let (node_index, node_reader) = Self::index_nodes(graph)?;
        let (edge_index, edge_reader) = Self::index_edges(graph)?;

        Ok(GraphIndex {
            node_index: Arc::new(node_index),
            edge_index: Arc::new(edge_index),
            node_reader,
            edge_reader,
        })
    }
}

impl GraphIndex {
    fn node_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        schema.add_u64_field(fields::VERTEX_ID, FAST | STORED);
        schema.add_u64_field(fields::VERTEX_ID_REV, FAST | STORED);
        schema.add_text_field(
            fields::NAME,
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        schema.add_text_field(
            fields::NODE_TYPE,
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        schema.add_json_field(
            fields::CONSTANT_PROPERTIES,
            JsonObjectOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("custom_default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema.add_json_field(
            fields::TEMPORAL_PROPERTIES,
            JsonObjectOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("custom_default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );

        schema
    }

    fn edge_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        schema.add_u64_field(fields::EDGE_ID, FAST | STORED);
        schema.add_text_field(
            fields::SOURCE,
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        schema.add_text_field(
            fields::DESTINATION,
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("custom_default")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        schema.add_json_field(
            fields::CONSTANT_PROPERTIES,
            JsonObjectOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("custom_default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        schema.add_json_field(
            fields::TEMPORAL_PROPERTIES,
            JsonObjectOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("custom_default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );

        schema
    }

    fn index_node_view<W: Deref<Target = IndexWriter>>(
        node: NodeView<GraphStorage>,
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
        let temporal_properties = binding
            .iter()
            .flat_map(|(key, values)| values.into_iter().map(move |(t, v)| (t, key.clone(), v)));

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

    fn index_nodes(g: &GraphStorage) -> tantivy::Result<(Index, IndexReader)> {
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
        e_ref: EdgeView<GraphStorage>,
        schema: &Schema,
        writer: &W,
    ) -> tantivy::Result<()> {
        let edge_id = e_ref.edge.pid().as_u64();
        let src = e_ref.src().name();
        let dst = e_ref.dst().name();

        let binding = e_ref.properties().constant();
        let constant_properties = binding.iter();

        let binding = e_ref.properties().temporal();
        let temporal_properties = binding
            .iter()
            .flat_map(|(key, values)| values.into_iter().map(move |(t, v)| (t, key.clone(), v)));

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

    pub fn index_edges(g: &GraphStorage) -> tantivy::Result<(Index, IndexReader)> {
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

    pub fn new() -> Self {
        let schema = Self::node_schema_builder().build();
        let (node_index, node_reader) =
            Self::new_index(schema, Self::default_node_index_settings());

        let schema = Self::edge_schema_builder().build();
        let (edge_index, edge_reader) =
            Self::new_index(schema, Self::default_edge_index_settings());

        GraphIndex {
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

    pub(crate) fn add_node_update(
        &self,
        graph: &GraphStorage,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let node_id = v.as_u64();
        let node_id_rev = u64::MAX - node_id;
        let node_name = graph.node_name(v);
        let node_type = graph.node_type(v).unwrap_or_else(|| ArcStr::from(""));
        let constant_properties = iter::empty::<(ArcStr, Prop)>();
        let temporal_properties = props.iter().filter_map(move |(prop_id, prop)| {
            let prop_name = graph.node_meta().get_prop_name(*prop_id, false);
            Some((t.t(), prop_name, prop.clone()))
        });

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

        Ok(())
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), GraphError> {
        let edge_id = edge_id.as_u64();
        let src_name = graph.node_name(src);
        let dst_name = graph.node_name(dst);

        let constant_properties = iter::empty::<(ArcStr, Prop)>();
        let temporal_properties = props.iter().filter_map(move |(prop_id, prop)| {
            let prop_name = graph.edge_meta().get_prop_name(*prop_id, false);
            Some((t.t(), prop_name, prop.clone()))
        });

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

        Ok(())
    }
}

fn collect_constant_properties<'a>(
    properties: impl Iterator<Item = (ArcStr, Prop)> + 'a,
) -> serde_json::Map<String, serde_json::Value> {
    properties.map(|(k, v)| (k.to_string(), v.into())).collect()
}

fn collect_temporal_properties<'a>(
    properties: impl Iterator<Item = (i64, ArcStr, Prop)> + 'a,
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
        .map(|(t, props)| {
            time.push(t);
            serde_json::Value::Object(props)
        })
        .collect::<Vec<_>>();

    (time, serde_json::Value::Array(temporal_properties))
}

fn create_node_document<'a>(
    node_id: u64,
    node_id_rev: u64,
    node_name: String,
    node_type: ArcStr,
    constant_properties: impl Iterator<Item = (ArcStr, Prop)> + 'a,
    temporal_properties: impl Iterator<Item = (i64, ArcStr, Prop)> + 'a,
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

fn create_edge_document<'a>(
    edge_id: u64,
    src: String,
    dst: String,
    constant_properties: impl Iterator<Item = (ArcStr, Prop)> + 'a,
    temporal_properties: impl Iterator<Item = (i64, ArcStr, Prop)> + 'a,
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

#[cfg(test)]
mod search_tests {
    use super::*;
    use crate::db::api::{
        mutation::internal::DelegateDeletionOps, view::internal::InternalIndexSearch,
    };
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

        let mut results = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "node_type:collapsed", 5, 0)
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

        let mut results = graph
            .graph()
            .searcher()
            .unwrap()
            .search_nodes(&graph, "balance:0.0", 5, 0)
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

        let mut results = graph
            .graph()
            .searcher()
            .unwrap()
            .search_nodes(
                &graph,
                "cluster_id:0x941900204497226bede1324742eb83af6b0b5eec",
                5,
                0,
            )
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        assert_eq!(results, vec!["0x941900204497226bede1324742eb83af6b0b5eec"]);

        let mut results = graph
            .graph()
            .searcher()
            .unwrap()
            .search_nodes(
                &graph,
                "name:0x941900204497226bede1324742eb83af6b0b5eec",
                5,
                0,
            )
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        assert_eq!(results, vec!["0x941900204497226bede1324742eb83af6b0b5eec"]);

        let results = graph
            .graph()
            .searcher()
            .unwrap()
            .search_nodes(
                &graph,
                "node_type:collapsed AND cluster_id:0x941900204497226bede1324742eb83af6b0b5eec",
                5,
                0,
            )
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        assert_eq!(results, vec!["0x941900204497226bede1324742eb83af6b0b5eec"]);

        let mut results = graph
            .graph()
            .searcher()
            .unwrap()
            .search_nodes(
                &graph,
                "node_type:collapsed OR cluster_id:0x941900204497226bede1324742eb83af6b0b5eec",
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
                [("age", Prop::U64(42)), ("balance", Prop::I64(-1234))],
                None,
            )
            .expect("failed to add node");

        let results = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "age:42", 5, 0)
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

        let elapsed = now.elapsed().unwrap().as_secs();
        info!("indexing took: {:?}", elapsed);

        let issues = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "name:'DEV-1690'", 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        info!("names: {:?}", names);

        Ok(())
    }

    #[test]
    fn create_indexed_graph_from_existing_graph() {
        let graph = Graph::new();
        graph
            .add_node(1, "Gandalf", [("kind", Prop::str("Wizard"))], None)
            .expect("add node failed");
        graph
            .add_node(
                2,
                "Frodo",
                [
                    ("kind", Prop::str("Hobbit")),
                    ("has_ring", Prop::str("yes")),
                ],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(2, "Merry", [("kind", Prop::str("Hobbit"))], None)
            .expect("add node failed");
        graph
            .add_node(4, "Gollum", [("kind", Prop::str("Creature"))], None)
            .expect("add node failed");
        graph
            .add_node(9, "Gollum", [("has_ring", Prop::str("yes"))], None)
            .expect("add node failed");
        graph
            .add_node(9, "Frodo", [("has_ring", Prop::str("no"))], None)
            .expect("add node failed");
        graph
            .add_node(10, "Frodo", [("has_ring", Prop::str("yes"))], None)
            .expect("add node failed");
        graph
            .add_node(10, "Gollum", [("has_ring", Prop::str("no"))], None)
            .expect("add node failed");

        let results = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "kind:Hobbit", 10, 0)
            .expect("search failed");
        let mut actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let mut expected = vec!["Frodo", "Merry"];
        // FIXME: this is not deterministic
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);

        let results = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "kind:Wizard", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        let results = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "kind:Creature", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);

        // search by name
        let results = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, "name:Gollum", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_name() {
        let graph = Graph::new();
        graph
            .add_node(1, "Gandalf", NO_PROPS, None)
            .expect("add node failed");

        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"name:Gandalf"#, 10, 0)
            .expect("search failed");

        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_description() {
        let graph = Graph::new();
        graph
            .add_node(1, "Bilbo", [("description", Prop::str("A hobbit"))], None)
            .expect("add node failed");
        graph
            .add_node(2, "Gandalf", [("description", Prop::str("A wizard"))], None)
            .expect("add node failed");

        // Find the Wizard
        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"description:"A wizard""#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        // Find the Hobbit
        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"description:'hobbit'"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Bilbo"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_node_type() {
        let graph = Graph::new();
        graph
            .add_node(1, "Gandalf", NO_PROPS, Some("wizard"))
            .expect("add node failed");
        graph
            .add_node(2, "Bilbo", NO_PROPS, None)
            .expect("add node failed");

        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"node_type:wizard"#, 10, 0)
            .expect("search failed");

        let actual = nodes
            .into_iter()
            .map(|v| v.node_type().unwrap().to_string())
            .collect::<Vec<_>>();
        let expected = vec!["wizard"];

        assert_eq!(actual, expected);

        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"node_type:''"#, 10, 0)
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
        let graph = Graph::new();
        graph
            .add_node(
                1,
                "Gandalf",
                [("description", Prop::str("The wizard"))],
                None,
            )
            .expect("add node failed");
        graph
            .add_node(
                2,
                "Saruman",
                [("description", Prop::str("Another wizard"))],
                None,
            )
            .expect("add node failed");

        // Find Saruman
        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"description:wizard AND time:[2 TO 5]"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Saruman"];
        assert_eq!(actual, expected);

        // Find Gandalf
        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"description:'wizard' AND time:[1 TO 2}"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        // Find both wizards
        let nodes = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, r#"description:'wizard' AND time:[1 TO 100]"#, 10, 0)
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
        let graph = Graph::new();
        graph
            .add_edge(
                1,
                "Frodo",
                "Gandalf",
                [("type", Prop::str("friends"))],
                None,
            )
            .expect("add edge failed");
        graph
            .add_edge(1, "Frodo", "Gollum", [("type", Prop::str("enemies"))], None)
            .expect("add edge failed");

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, "from:Frodo", 5, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, r#"type:'friends'"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gandalf".to_string())];

        assert_eq!(actual, expected);

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, r#"type:'enemies'"#, 10, 0)
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
        let graph = Graph::new();
        graph
            .add_edge(
                1,
                "Frodo",
                "Gandalf",
                [("type", Prop::str("friends"))],
                None,
            )
            .expect("add edge failed");
        graph
            .add_edge(1, "Frodo", "Gollum", [("type", Prop::str("enemies"))], None)
            .expect("add edge failed");

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, "from:Frodo", 5, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, r#"type:'friends'"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gandalf".to_string())];

        assert_eq!(actual, expected);

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, r#"type:'enemies'"#, 10, 0)
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
        let graph = Graph::new();
        graph
            .add_edge(1, "Frodo", "Gandalf", NO_PROPS, None)
            .expect("add edge failed");
        graph
            .add_edge(1, "Frodo", "Gollum", NO_PROPS, None)
            .expect("add edge failed");

        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, r#"from:Frodo"#, 10, 0)
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
        let results = graph
            .searcher()
            .unwrap()
            .search_edges(&graph, "to:gollum", 10, 0)
            .expect("search failed");
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

        let query_parser = QueryParser::for_index(&index, vec![]);
        let query = query_parser.parse_query(r#"name:"gandalf""#).unwrap();

        let ranking =
            TopDocs::with_limit(10).order_by_fast_field(fields::VERTEX_ID.to_string(), Order::Asc);
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &ranking).unwrap();

        assert!(!top_docs.is_empty());
    }
}
