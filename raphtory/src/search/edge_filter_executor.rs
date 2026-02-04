use crate::{
    db::{
        api::view::{internal::FilterOps, BoxableGraphView, Filter, StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            views::filter::{
                model::{
                    edge_filter::{CompositeEdgeFilter, EdgeFilter},
                    property_filter::PropertyRef,
                },
                CreateFilter,
            },
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps, NodeViewOps, PropertyFilter, TimeOps},
    search::{
        collectors::unique_entity_filter_collector::UniqueEntityFilterCollector,
        fallback_filter_edges, fields, get_reader, graph_index::Index,
        node_filter_executor::NodeFilterExecutor, property_index::PropertyIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::{entities::EID, storage::timeindex::AsTime};
use raphtory_storage::graph::edges::edge_storage_ops::EdgeStorageOps;
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::Collector, query::Query, schema::Value, DocAddress, Document, IndexReader, Score,
    Searcher, TantivyDocument,
};

#[derive(Clone, Copy)]
pub struct EdgeFilterExecutor<'a> {
    index: &'a Index,
    query_builder: QueryBuilder<'a>,
}

impl<'a> EdgeFilterExecutor<'a> {
    pub fn new(index: &'a Index) -> Self {
        Self {
            index,
            query_builder: QueryBuilder::new(index),
        }
    }

    fn execute_filter_query<G: StaticGraphViewOps>(
        &self,
        filter: impl CreateFilter,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let searcher = reader.searcher();
        let collector = UniqueEntityFilterCollector::new(fields::EDGE_ID.to_string());
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_edges_from_edge_ids(filter, graph, edge_ids)?;

        if offset == 0 && limit >= edges.len() {
            Ok(edges)
        } else {
            Ok(edges.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_filter_property_query<G, C>(
        &self,
        filter: &PropertyFilter<EdgeFilter>,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<u64>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(fields::EDGE_ID.to_string());
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_edges_from_edge_ids(filter.clone(), graph, edge_ids)?;

        if offset == 0 && limit >= edges.len() {
            Ok(edges)
        } else {
            Ok(edges.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_or_fallback<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<EdgeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => {
                self.execute_filter_query(filter.clone(), graph, query, &reader, limit, offset)
            }
            // Fallback to raphtory apis
            None => fallback_filter_edges(graph, filter, limit, offset),
        }
    }

    fn execute_or_fallback_temporal<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<EdgeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => self.execute_filter_property_query(
                filter,
                graph,
                query,
                &reader,
                limit,
                offset,
                collector_fn,
            ),
            // Fallback to raphtory apis
            None => fallback_filter_edges(graph, filter, limit, offset),
        }
    }

    fn apply_metadata_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter<EdgeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        if let Some((cpi, _)) = self
            .index
            .edge_index
            .entity_index
            .get_metadata_index(graph.edge_meta(), prop_name)?
        {
            self.execute_or_fallback(graph, &cpi, filter, limit, offset)
        } else {
            fallback_filter_edges(graph, filter, limit, offset)
        }
    }

    fn apply_temporal_property_filter<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter<EdgeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        if let Some((tpi, _)) = self
            .index
            .edge_index
            .entity_index
            .get_temporal_property_index(graph.edge_meta(), prop_name)?
        {
            self.execute_or_fallback_temporal(graph, &tpi, filter, limit, offset, collector_fn)
        } else {
            fallback_filter_edges(graph, filter, limit, offset)
        }
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter<EdgeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        if filter.ops.is_empty() {
            return fallback_filter_edges(graph, filter, limit, offset);
        }

        match &filter.prop_ref {
            PropertyRef::Metadata(prop_name) => {
                self.apply_metadata_filter(graph, prop_name, filter, limit, offset)
            }
            PropertyRef::TemporalProperty(prop_name) | PropertyRef::Property(prop_name) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    UniqueEntityFilterCollector::new,
                ),
        }
    }

    pub fn filter_edges_internal<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        match filter {
            CompositeEdgeFilter::Src(node_filter) => {
                let nfe = NodeFilterExecutor::new(self.index);
                let nodes = nfe.filter_nodes(graph, node_filter, usize::MAX, 0)?;
                let mut edges: Vec<EdgeView<G>> = nodes
                    .into_iter()
                    .flat_map(|n| n.out_edges().into_iter())
                    .collect();
                if offset != 0 || limit < edges.len() {
                    edges = edges.into_iter().skip(offset).take(limit).collect();
                }
                Ok(edges)
            }
            CompositeEdgeFilter::Dst(node_filter) => {
                let nfe = NodeFilterExecutor::new(self.index);
                let nodes = nfe.filter_nodes(graph, node_filter, usize::MAX, 0)?;
                let mut edges: Vec<EdgeView<G>> = nodes
                    .into_iter()
                    .flat_map(|n| n.in_edges().into_iter())
                    .collect();
                if offset != 0 || limit < edges.len() {
                    edges = edges.into_iter().skip(offset).take(limit).collect();
                }
                Ok(edges)
            }
            CompositeEdgeFilter::Property(filter) => {
                self.filter_property_index(graph, filter, limit, offset)
            }
            CompositeEdgeFilter::Windowed(filter) => {
                let start = filter.start.t();
                let end = filter.end.t();
                let dyn_graph: Arc<dyn BoxableGraphView> = Arc::new((*graph).clone());
                let dyn_graph = dyn_graph.window(start, end);
                let res = self.filter_edges(&dyn_graph, &filter.inner, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| EdgeView::new(graph.clone(), x.edge))
                    .collect())
            }
            CompositeEdgeFilter::Latest(filter) => {
                let dyn_graph: Arc<dyn BoxableGraphView> = Arc::new((*graph).clone());
                let dyn_graph = dyn_graph.latest();
                let res = self.filter_edges(&dyn_graph, &filter.inner, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| EdgeView::new(graph.clone(), x.edge))
                    .collect())
            }
            CompositeEdgeFilter::SnapshotAt(filter) => {
                let dyn_graph: Arc<dyn BoxableGraphView> = Arc::new((*graph).clone());
                let dyn_graph = dyn_graph.snapshot_at(filter.time);
                let res = self.filter_edges(&dyn_graph, &filter.inner, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| EdgeView::new(graph.clone(), x.edge))
                    .collect())
            }
            CompositeEdgeFilter::SnapshotLatest(filter) => {
                let dyn_graph: Arc<dyn BoxableGraphView> = Arc::new((*graph).clone());
                let dyn_graph = dyn_graph.snapshot_latest();
                let res = self.filter_edges(&dyn_graph, &filter.inner, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| EdgeView::new(graph.clone(), x.edge))
                    .collect())
            }
            CompositeEdgeFilter::Layered(filter) => {
                let layer = filter.layer.clone();
                let dyn_graph: Arc<dyn BoxableGraphView> = Arc::new((*graph).clone());
                let dyn_graph = dyn_graph.layers(layer)?;
                let res = self.filter_edges(&dyn_graph, &filter.inner, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| EdgeView::new(graph.clone(), x.edge))
                    .collect())
            }
            CompositeEdgeFilter::IsActiveEdge(filter) => {
                fallback_filter_edges(graph, filter, limit, offset)
            }
            CompositeEdgeFilter::IsValidEdge(filter) => {
                fallback_filter_edges(graph, filter, limit, offset)
            }
            CompositeEdgeFilter::IsDeletedEdge(filter) => {
                fallback_filter_edges(graph, filter, limit, offset)
            }
            CompositeEdgeFilter::IsSelfLoopEdge(filter) => {
                fallback_filter_edges(graph, filter, limit, offset)
            }
            CompositeEdgeFilter::And(left, right) => {
                let left_result = self.filter_edges(graph, left, limit, offset)?;
                let right_result = self.filter_edges(graph, right, limit, offset)?;

                // Intersect results
                let left_set: HashSet<_> = left_result.into_iter().collect();
                let intersection = right_result
                    .into_iter()
                    .filter(|e| left_set.contains(e))
                    .collect::<Vec<_>>();

                Ok(intersection)
            }
            CompositeEdgeFilter::Or(left, right) => {
                let left_result = self.filter_edges(graph, left, limit, offset)?;
                let right_result = self.filter_edges(graph, right, limit, offset)?;

                // Union results
                let mut combined = HashSet::new();
                combined.extend(left_result);
                combined.extend(right_result);

                Ok(combined.into_iter().collect())
            }
            CompositeEdgeFilter::Not(_) => fallback_filter_edges(graph, filter, limit, offset),
        }
    }

    pub fn filter_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        self.filter_edges_internal(graph, filter, limit, offset)
    }

    #[allow(dead_code)]
    // Useful for debugging
    fn resolve_edge_from_search_result<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        searcher: &Searcher,
        docs: Vec<(Score, DocAddress)>,
    ) -> tantivy::Result<Vec<EdgeView<G>>> {
        let schema = searcher.schema();
        let edge_id_field = schema.get_field(fields::EDGE_ID)?;

        let edges = docs
            .into_iter()
            .filter_map(|(_score, doc_address)| {
                let doc = searcher.doc::<TantivyDocument>(doc_address).ok()?;
                let edge_id: usize = doc
                    .get_first(edge_id_field)
                    .and_then(|value| value.as_u64())?
                    .try_into()
                    .ok()?;
                let e_ref = graph.core_edge(EID(edge_id));
                graph.edge(e_ref.src(), e_ref.dst())
            })
            .collect::<Vec<_>>();

        Ok(edges)
    }

    // unique index edge ids, which are also filter by index -> candidate edges
    // out of those candidate edges find me which filtered graph edges
    fn resolve_edges_from_edge_ids<G: StaticGraphViewOps>(
        &self,
        filter: impl CreateFilter,
        graph: &G,
        edge_ids: HashSet<u64>,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let filtered_graph = graph.filter(filter)?;
        let edges = edge_ids
            .into_iter()
            .filter_map(|id| {
                let e_ref = graph.core_edge(EID(id as usize));
                filtered_graph
                    .filter_edge(e_ref.as_ref())
                    .then(|| EdgeView::new(graph.clone(), e_ref.out_ref()))
            })
            .collect_vec();
        Ok(edges)
    }

    #[allow(dead_code)]
    // Useful for debugging
    fn print_docs(
        searcher: &Searcher,
        query: &Box<dyn Query>,
        top_docs: &Vec<(Score, DocAddress)>,
    ) {
        println!("Top Docs (debugging):");
        println!("Query:{:?}", query,);
        for (score, doc_address) in top_docs {
            match searcher.doc::<TantivyDocument>(*doc_address) {
                Ok(doc) => {
                    let schema = searcher.schema();
                    println!("Score: {}, Document: {}", score, doc.to_json(schema));
                }
                Err(e) => {
                    println!("Failed to retrieve document: {:?}", e);
                }
            }
        }
    }

    #[allow(dead_code)]
    // Useful for debugging
    fn print_schema_fields(schema: &tantivy::schema::Schema) {
        println!("Schema fields and their IDs:");
        for (field_name, _field_entry) in schema.fields() {
            println!("Field Name: '{:?}'", field_name,);
        }
    }

    #[allow(dead_code)]
    // Useful for debugging
    fn print_schema(schema: &tantivy::schema::Schema) {
        println!("Schema:\n{:?}", schema);
    }
}
