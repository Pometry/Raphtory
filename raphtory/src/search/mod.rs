// search goes here

pub mod into_indexed;

use std::{collections::HashSet, ops::Deref, path::Path, sync::Arc};

use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use tantivy::{
    collector::TopDocs,
    schema::{Field, Schema, SchemaBuilder, Value, FAST, INDEXED, STORED, TEXT},
    Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, TantivyError,
};

use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, EID, ELID, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
        ArcStr, OptionAsStr, PropType,
    },
    db::{
        api::{
            mutation::internal::{InheritPropertyAdditionOps, InternalAdditionOps},
            storage::edges::edge_storage_ops::EdgeStorageOps,
            view::{
                internal::{DynamicGraph, InheritViewOps, IntoDynamic, Static},
                Base, MaterializedGraph, StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};

#[derive(Clone)]
pub struct IndexedGraph<G> {
    pub graph: G,
    pub(crate) node_index: Arc<Index>,
    pub(crate) edge_index: Arc<Index>,
    pub(crate) reader: IndexReader,
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
    // edges
    // pub const SRC_ID: &str = "src_id";
    pub const SOURCE: &str = "from";
    // pub const DEST_ID: &str = "dest_id";
    pub const DESTINATION: &str = "to";
    pub const EDGE_ID: &str = "edge_id";
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
            reader: self.reader,
            edge_reader: self.edge_reader,
        }
    }
}

impl IndexedGraph<MaterializedGraph> {
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        self.graph.save_to_file(path)
    }
}

impl<'graph, G: GraphViewOps<'graph>> IndexedGraph<G> {
    pub fn graph(&self) -> &G {
        &self.graph
    }
    fn new_node_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();

        // we first add GID time, ID and ID_REV
        // ensure time is part of the index
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        // ensure we add node_id as stored to get back the node id after the search
        schema.add_u64_field(fields::VERTEX_ID, FAST | STORED);
        // reverse to sort by it
        schema.add_u64_field(fields::VERTEX_ID_REV, FAST | STORED);
        // add name
        schema.add_text_field(fields::NAME, TEXT);
        // add node_type
        schema.add_text_field(fields::NODE_TYPE, TEXT);
        schema
    }

    fn new_edge_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        // we first add GID time, ID and ID_REV
        // ensure time is part of the index
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        // ensure we add node_id as stored to get back the node id after the search
        schema.add_text_field(fields::SOURCE, TEXT);
        schema.add_text_field(fields::DESTINATION, TEXT);
        schema.add_u64_field(fields::EDGE_ID, FAST | STORED);

        schema
    }

    fn schema_from_props<S: AsRef<str>, I: IntoIterator<Item = (S, Prop)>>(props: I) -> Schema {
        let mut schema = Self::new_node_schema_builder();

        for (prop_name, prop) in props.into_iter() {
            match prop {
                Prop::Str(_) => {
                    schema.add_text_field(prop_name.as_ref(), TEXT);
                }
                Prop::NDTime(_) => {
                    schema.add_date_field(prop_name.as_ref(), INDEXED);
                }
                _ => todo!(),
            }
        }

        schema.build()
    }

    fn set_schema_field_from_prop(schema: &mut SchemaBuilder, prop: &str, prop_value: Prop) {
        match prop_value {
            Prop::Str(_) => {
                schema.add_text_field(prop, TEXT);
            }
            Prop::NDTime(_) => {
                schema.add_date_field(prop, INDEXED);
            }
            Prop::U8(_) => {
                schema.add_u64_field(prop, INDEXED);
            }
            Prop::U16(_) => {
                schema.add_u64_field(prop, INDEXED);
            }
            Prop::U64(_) => {
                schema.add_u64_field(prop, INDEXED);
            }
            Prop::I64(_) => {
                schema.add_i64_field(prop, INDEXED);
            }
            Prop::I32(_) => {
                schema.add_i64_field(prop, INDEXED);
            }
            Prop::F64(_) => {
                schema.add_f64_field(prop, INDEXED);
            }
            Prop::F32(_) => {
                schema.add_f64_field(prop, INDEXED);
            }
            Prop::Bool(_) => {
                schema.add_bool_field(prop, INDEXED);
            }
            _ => {
                schema.add_text_field(prop, TEXT);
            }
        }
    }

    // we need to check every node for the properties and add them
    // to the schem depending on the type of the property
    //
    fn schema_for_node(g: &G) -> Schema {
        let mut schema = Self::new_node_schema_builder();

        // TODO: load all these from the graph at some point in the future
        let mut prop_names_set = g
            .node_meta()
            .temporal_prop_meta()
            .get_keys()
            .into_iter()
            .chain(g.node_meta().const_prop_meta().get_keys().into_iter())
            .collect::<HashSet<_>>();

        for node in g.nodes() {
            if prop_names_set.is_empty() {
                break;
            }
            let mut found_props: HashSet<ArcStr> = HashSet::from([
                fields::TIME.into(),
                fields::VERTEX_ID.into(),
                fields::VERTEX_ID_REV.into(),
                fields::NAME.into(),
                fields::NODE_TYPE.into(),
            ]);
            found_props.insert("name".into());

            for prop in prop_names_set.iter() {
                // load temporal props
                if let Some(prop_value) = node
                    .properties()
                    .temporal()
                    .get(prop)
                    .and_then(|p| p.latest())
                {
                    if found_props.contains(prop) {
                        continue;
                    }
                    Self::set_schema_field_from_prop(&mut schema, prop, prop_value);
                    found_props.insert(prop.clone());
                }
                // load static props
                if let Some(prop_value) = node.properties().constant().get(prop) {
                    if !found_props.contains(prop) {
                        Self::set_schema_field_from_prop(&mut schema, prop, prop_value);
                        found_props.insert(prop.clone());
                    }
                }
            }

            for found_prop in found_props {
                prop_names_set.remove(&found_prop);
            }
        }

        schema.build()
    }

    // we need to check every node for the properties and add them
    // to the schem depending on the type of the property
    //
    fn schema_for_edge(g: &G) -> Schema {
        let mut schema = Self::new_edge_schema_builder();

        // TODO: load all these from the graph at some point in the future
        let mut prop_names_set = g
            .edge_meta()
            .temporal_prop_meta()
            .get_keys()
            .into_iter()
            .chain(g.edge_meta().const_prop_meta().get_keys())
            .collect::<HashSet<_>>();

        for edge in g.edges() {
            if prop_names_set.is_empty() {
                break;
            }
            let mut found_props: HashSet<ArcStr> = HashSet::from([
                fields::TIME.into(),
                fields::SOURCE.into(),
                fields::DESTINATION.into(),
                fields::EDGE_ID.into(),
            ]);

            for prop in prop_names_set.iter() {
                // load temporal props
                if let Some(prop_value) = edge
                    .properties()
                    .temporal()
                    .get(prop)
                    .and_then(|p| p.latest())
                {
                    if found_props.contains(prop) {
                        continue;
                    }
                    Self::set_schema_field_from_prop(&mut schema, prop, prop_value);
                    found_props.insert(prop.clone());
                }
                // load static props
                if let Some(prop_value) = edge.properties().constant().get(prop) {
                    if !found_props.contains(prop) {
                        Self::set_schema_field_from_prop(&mut schema, prop, prop_value);
                        found_props.insert(prop.clone());
                    }
                }
            }

            for found_prop in found_props {
                prop_names_set.remove(&found_prop);
            }
        }

        schema.build()
    }

    fn index_prop_value(document: &mut TantivyDocument, prop_field: Field, prop_value: Prop) {
        match prop_value {
            Prop::Str(prop_text) => {
                // add the property to the document
                document.add_text(prop_field, prop_text);
            }
            Prop::NDTime(prop_time) => {
                let time = tantivy::DateTime::from_timestamp_nanos(
                    prop_time.and_utc().timestamp_nanos_opt().unwrap(),
                );
                document.add_date(prop_field, time);
            }
            Prop::U8(prop_u8) => {
                document.add_u64(prop_field, u64::from(prop_u8));
            }
            Prop::U16(prop_u16) => {
                document.add_u64(prop_field, u64::from(prop_u16));
            }
            Prop::U64(prop_u64) => {
                document.add_u64(prop_field, prop_u64);
            }
            Prop::I64(prop_i64) => {
                document.add_i64(prop_field, prop_i64);
            }
            Prop::I32(prop_i32) => {
                document.add_i64(prop_field, i64::from(prop_i32));
            }
            Prop::F64(prop_f64) => {
                document.add_f64(prop_field, prop_f64);
            }
            Prop::F32(prop_f32) => {
                document.add_f64(prop_field, f64::from(prop_f32));
            }
            Prop::Bool(prop_bool) => {
                document.add_bool(prop_field, prop_bool);
            }
            prop => document.add_text(prop_field, prop.to_string()),
        }
    }

    fn index_nodes(g: &G) -> tantivy::Result<(Index, IndexReader)> {
        let schema = Self::schema_for_node(g);
        let (index, reader) = Self::new_index(schema.clone(), Self::default_node_index_settings());

        let time_field = schema.get_field(fields::TIME)?;
        let node_id_field = schema.get_field(fields::VERTEX_ID)?;
        let node_id_rev_field = schema.get_field(fields::VERTEX_ID_REV)?;

        let writer = Arc::new(parking_lot::RwLock::new(index.writer(100_000_000)?));

        let v_ids = (0..g.count_nodes()).collect::<Vec<_>>();

        v_ids.par_chunks(128).try_for_each(|v_ids| {
            let writer_lock = writer.clone();
            {
                let writer_guard = writer_lock.read();
                for v_id in v_ids {
                    if let Some(node) = g.node(NodeRef::new((*v_id).into())) {
                        Self::index_node_view(
                            node,
                            &schema,
                            &writer_guard,
                            time_field,
                            node_id_field,
                            node_id_rev_field,
                        )?;
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

    pub fn from_graph(g: &G) -> tantivy::Result<Self> {
        let (node_index, node_reader) = Self::index_nodes(g)?;
        let (edge_index, edge_reader) = Self::index_edges(g)?;

        Ok(IndexedGraph {
            graph: g.clone(),
            node_index: Arc::new(node_index),
            edge_index: Arc::new(edge_index),
            reader: node_reader,
            edge_reader,
        })
    }

    fn index_node_view<W: Deref<Target = IndexWriter>>(
        node: NodeView<G>,
        schema: &Schema,
        writer: &W,
        time_field: Field,
        node_id_field: Field,
        node_id_rev_field: Field,
    ) -> tantivy::Result<()> {
        let node_id: u64 = usize::from(node.node) as u64;

        let mut document = TantivyDocument::new();
        // add the node_id
        document.add_u64(node_id_field, node_id);
        document.add_u64(node_id_rev_field, u64::MAX - node_id);

        let name_field = schema.get_field("name")?;
        document.add_text(name_field, node.name());

        for (temp_prop_name, temp_prop_value) in node.properties().temporal() {
            let prop_field = schema.get_field(&temp_prop_name)?;
            for (time, prop_value) in temp_prop_value {
                // add time to the document
                document.add_i64(time_field, time);

                Self::index_prop_value(&mut document, prop_field, prop_value);
            }
        }

        for (prop_name, prop_value) in node.properties().constant() {
            let prop_field = schema.get_field(&prop_name)?;
            Self::index_prop_value(&mut document, prop_field, prop_value);
        }

        match node.node_type() {
            None => {}
            Some(str) => document.add_text(schema.get_field("node_type")?, (*str).to_string()),
        }

        writer.add_document(document)?;
        Ok(())
    }

    fn index_edge_view<W: Deref<Target = IndexWriter>>(
        e_ref: EdgeView<G, G>,
        schema: &Schema,
        writer: &W,
        time_field: Field,
        source_field: Field,
        destination_field: Field,
        edge_id_field: Field,
    ) -> tantivy::Result<()> {
        let edge_ref = e_ref.edge;

        let src = e_ref.src();
        let dst = e_ref.dst();

        let mut document = TantivyDocument::new();
        let edge_id: u64 = Into::<usize>::into(edge_ref.pid()) as u64;
        document.add_u64(edge_id_field, edge_id);
        document.add_text(source_field, src.name());
        document.add_text(destination_field, dst.name());

        // add all time events
        for e in e_ref.explode() {
            if let Some(t) = e.time() {
                document.add_i64(time_field, t);
            }
        }

        for (temp_prop_name, temp_prop_value) in e_ref.properties().temporal() {
            let prop_field = schema.get_field(&temp_prop_name)?;
            for (time, prop_value) in temp_prop_value {
                // add time to the document
                document.add_i64(time_field, time);
                Self::index_prop_value(&mut document, prop_field, prop_value);
            }
        }

        for (prop_name, prop_value) in e_ref.properties().constant() {
            let prop_field = schema.get_field(&prop_name)?;
            Self::index_prop_value(&mut document, prop_field, prop_value);
        }

        writer.add_document(document)?; // add the edge itself
        Ok(())
    }

    pub fn index_edges(g: &G) -> tantivy::Result<(Index, IndexReader)> {
        let schema = Self::schema_for_edge(g);
        let (index, reader) = Self::new_index(schema.clone(), Self::default_edge_index_settings());

        let time_field = schema.get_field(fields::TIME)?;
        let source_field = schema.get_field(fields::SOURCE)?;
        let destination_field = schema.get_field(fields::DESTINATION)?;
        let edge_id_field = schema.get_field(fields::EDGE_ID)?;

        let writer = Arc::new(parking_lot::RwLock::new(index.writer(100_000_000)?));

        let locked_g = g.core_graph();

        locked_g.edges_par(&g).try_for_each(|e_ref| {
            let writer_lock = writer.clone();
            {
                let writer_guard = writer_lock.read();
                let e_view = EdgeView::new(g.clone(), e_ref);
                Self::index_edge_view(
                    e_view,
                    &schema,
                    &writer_guard,
                    time_field,
                    source_field,
                    destination_field,
                    edge_id_field,
                )?;
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
        (index, reader)
    }

    pub fn new<S, I, I2>(graph: G, node_props: I, edge_props: I2) -> Self
    where
        S: AsRef<str>,
        I: IntoIterator<Item = (S, Prop)>,
        I2: IntoIterator<Item = (S, Prop)>,
    {
        let schema = Self::schema_from_props(node_props);

        let (index, reader) = Self::new_index(schema, Self::default_node_index_settings());

        let schema = Self::schema_from_props(edge_props);

        let (edge_index, edge_reader) =
            Self::new_index(schema, Self::default_edge_index_settings());

        IndexedGraph {
            graph,
            node_index: Arc::new(index),
            edge_index: Arc::new(edge_index),
            reader,
            edge_reader,
        }
    }

    pub fn reload(&self) -> Result<(), GraphError> {
        self.reader.reload()?;
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
        let core_edge = self.graph.core_edge(ELID::new(EID(edge_id), None));
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

    pub fn search_nodes(
        &self,
        q: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let searcher = self.reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&self.node_index, vec![]);

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
        let searcher = self.reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&self.node_index, vec![]);
        let query = query_parser.parse_query(q)?;

        let count = searcher.search(&query, &tantivy::collector::Count)?;

        Ok(count)
    }

    pub fn search_edge_count(&self, q: &str) -> Result<usize, GraphError> {
        let searcher = self.edge_reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&self.edge_index, vec![]);
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
        let query_parser = tantivy::query::QueryParser::for_index(&self.edge_index, vec![]);

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
        let searcher = self.reader.searcher();
        let mut query_parser = tantivy::query::QueryParser::for_index(&self.node_index, vec![]);

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
        let mut query_parser = tantivy::query::QueryParser::for_index(&self.edge_index, vec![]);
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
    fn next_event_id(&self) -> usize {
        self.graph.next_event_id()
    }
    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> usize {
        self.graph.resolve_layer(layer)
    }

    #[inline]
    fn resolve_node_type(&self, v_id: VID, node_type: Option<&str>) -> Result<usize, GraphError> {
        self.graph.resolve_node_type(v_id, node_type)
    }

    #[inline]
    fn resolve_node(&self, id: u64, name: Option<&str>) -> VID {
        self.graph.resolve_node(id, name)
    }

    #[inline]
    fn resolve_graph_property(&self, prop: &str, is_static: bool) -> usize {
        self.graph.resolve_graph_property(prop, is_static)
    }

    #[inline]
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph.resolve_node_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph.resolve_edge_property(prop, dtype, is_static)
    }

    #[inline]
    fn process_prop_value(&self, prop: Prop) -> Prop {
        self.graph.process_prop_value(prop)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
        node_type_id: usize,
    ) -> Result<(), GraphError> {
        let mut document = TantivyDocument::new();
        // add time to the document
        let time = self.node_index.schema().get_field(fields::TIME)?;
        document.add_i64(time, t.t());
        // add name to the document

        let name = self.node_index.schema().get_field(fields::NAME)?;
        document.add_text(name, self.graph.node_name(v));

        // index all props that are declared in the schema
        for (prop_id, prop) in props.iter() {
            let prop_name = self.graph.node_meta().get_prop_name(*prop_id, false);
            if let Ok(field) = self.node_index.schema().get_field(&prop_name) {
                if let Prop::Str(s) = prop {
                    document.add_text(field, s)
                }
            }
        }
        // add the node type to the document
        let node_type_field = self.node_index.schema().get_field(fields::NODE_TYPE)?;
        let node_type = self
            .graph
            .node_meta()
            .get_node_type_name_by_id(node_type_id);
        document.add_text(node_type_field, node_type.as_str().unwrap_or(""));

        // add the node id to the document
        self.graph.internal_add_node(t, v, props, node_type_id)?;
        // get the field from the index
        let node_id = self.node_index.schema().get_field(fields::VERTEX_ID)?;
        let node_id_rev = self.node_index.schema().get_field(fields::VERTEX_ID_REV)?;
        let index_v_id: u64 = Into::<usize>::into(v) as u64;

        document.add_u64(node_id, index_v_id);
        document.add_u64(node_id_rev, u64::MAX - index_v_id);

        let mut writer = self.node_index.writer(50_000_000)?;

        writer.add_document(document)?;

        writer.commit()?;

        Ok(())
    }

    fn internal_add_edge(
        &self,
        _t: TimeIndexEntry,
        _src: VID,
        _dst: VID,
        _props: Vec<(usize, Prop)>,
        _layer: usize,
    ) -> Result<EID, GraphError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;
    use tantivy::{doc, DocAddress, Order};

    use super::*;

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
            .search_nodes("age:42", 5, 0)
            .expect("failed to search for node")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();

        assert_eq!(results, vec!["Blerg"]);
    }

    #[test]
    #[ignore = "this test is for experiments with the jira graph"]
    fn load_jira_graph() -> Result<(), GraphError> {
        let graph = Graph::load_from_file("/tmp/graphs/jira", false).expect("failed to load graph");
        assert!(graph.count_nodes() > 0);

        let now = SystemTime::now();

        let index_graph: IndexedGraph<Graph> = graph.into();
        let elapsed = now.elapsed().unwrap().as_secs();
        println!("indexing took: {:?}", elapsed);

        let issues = index_graph.search_nodes("name:'DEV-1690'", 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        println!("names: {:?}", names);

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
            .search_nodes("kind:hobbit", 10, 0)
            .expect("search failed");
        let mut actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let mut expected = vec!["Frodo", "Merry"];
        // FIXME: this is not deterministic
        actual.sort();
        expected.sort();

        assert_eq!(actual, expected);

        let results = indexed_graph
            .search_nodes("kind:wizard", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        let results = indexed_graph
            .search_nodes("kind:creature", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);

        // search by name
        let results = indexed_graph
            .search_nodes("name:gollum", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_name() {
        let graph = IndexedGraph::new(Graph::new(), NO_PROPS, NO_PROPS);

        graph
            .add_node(1, "Gandalf", NO_PROPS, None)
            .expect("add node failed");

        graph.reload().expect("reload failed");

        let nodes = graph
            .search_nodes(r#"name:gandalf"#, 10, 0)
            .expect("search failed");

        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_description() {
        let graph = IndexedGraph::new(Graph::new(), [("description", Prop::str(""))], NO_PROPS);

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
            .search_nodes(r#"description:wizard"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);
        // Find the Hobbit
        let nodes = graph
            .search_nodes(r#"description:'hobbit'"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Bilbo"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_node_search_by_node_type() {
        let graph = IndexedGraph::new(Graph::new(), NO_PROPS, NO_PROPS);

        graph
            .add_node(1, "Gandalf", NO_PROPS, Some("wizard"))
            .expect("add node failed");

        graph
            .add_node(1, "Bilbo", NO_PROPS, None)
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
        let graph = IndexedGraph::new(Graph::new(), [("description", Prop::str(""))], NO_PROPS);

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
            .search_nodes(r#"description:wizard AND time:[2 TO 5]"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Saruman"];
        assert_eq!(actual, expected);
        // Find Gandalf
        let nodes = graph
            .search_nodes(r#"description:'wizard' AND time:[1 TO 2}"#, 10, 0)
            .expect("search failed");
        let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);
        // Find both wizards
        let nodes = graph
            .search_nodes(r#"description:'wizard' AND time:[1 TO 100]"#, 10, 0)
            .expect("search failed");
        let mut actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let mut expected = vec!["Gandalf", "Saruman"];

        // FIXME: this is not deterministic
        actual.sort();
        expected.sort();

        assert_eq!(actual, expected);
    }

    #[test]
    fn search_by_edge_props() {
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

        let ig: IndexedGraph<Graph> = g.into();

        let results = ig
            .search_edges(r#"type:friends"#, 10, 0)
            .expect("search failed");
        let actual = results
            .into_iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        let expected = vec![("Frodo".to_string(), "Gandalf".to_string())];

        assert_eq!(actual, expected);

        let results = ig
            .search_edges(r#"type:enemies"#, 10, 0)
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
