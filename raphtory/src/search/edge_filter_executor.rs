use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{
            edge::EdgeView,
            views::filter::model::{
                edge_filter::{CompositeEdgeFilter, EdgeFieldFilter},
                property_filter::{PropertyRef, Temporal},
                Filter,
            },
        },
    },
    errors::GraphError,
    prelude::{EdgeViewOps, GraphViewOps, PropertyFilter},
    search::{
        collectors::{
            edge_property_filter_collector::EdgePropertyFilterCollector,
            latest_edge_property_filter_collector::LatestEdgePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fallback_filter_edges, fields, get_reader,
        graph_index::Index,
        property_index::PropertyIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::EID;
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
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G, G>>, GraphError> {
        let searcher = reader.searcher();
        let collector = UniqueEntityFilterCollector::new(fields::EDGE_ID.to_string());
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_edges_from_edge_ids(graph, edge_ids)?;

        if offset == 0 && limit >= edges.len() {
            Ok(edges)
        } else {
            Ok(edges.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_filter_property_query<G, C>(
        &self,
        graph: &G,
        query: Box<dyn Query>,
        prop_id: usize,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, G) -> C,
    ) -> Result<Vec<EdgeView<G, G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<u64>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(fields::EDGE_ID.to_string(), prop_id, graph.clone());
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_edges_from_edge_ids(graph, edge_ids)?;

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
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => self.execute_filter_query(graph, query, &reader, limit, offset),
            // Fallback to raphtory apis
            None => fallback_filter_edges(graph, filter, limit, offset),
        }
    }

    fn execute_or_fallback_temporal<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        prop_id: usize,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, G) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => self.execute_filter_property_query(
                graph,
                query,
                prop_id,
                &reader,
                limit,
                offset,
                collector_fn,
            ),
            // Fallback to raphtory apis
            None => fallback_filter_edges(graph, filter, limit, offset),
        }
    }

    fn apply_const_property_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        if let Some((cpi, _)) = self
            .index
            .edge_index
            .entity_index
            .get_const_property_index(graph.edge_meta(), prop_name)?
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
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, G) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        if let Some((tpi, prop_id)) = self
            .index
            .edge_index
            .entity_index
            .get_temporal_property_index(graph.edge_meta(), prop_name)?
        {
            self.execute_or_fallback_temporal(
                graph,
                prop_id,
                &tpi,
                filter,
                limit,
                offset,
                collector_fn,
            )
        } else {
            fallback_filter_edges(graph, filter, limit, offset)
        }
    }

    fn apply_combined_property_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let cpi = self
            .index
            .edge_index
            .entity_index
            .get_const_property_index(graph.edge_meta(), prop_name)?;
        let tpi = self
            .index
            .edge_index
            .entity_index
            .get_temporal_property_index(graph.edge_meta(), prop_name)?;

        match (cpi, tpi) {
            (Some((cpi, _)), Some((tpi, prop_id))) => {
                let cpi_results = self.execute_or_fallback(graph, &cpi, filter, limit, offset)?;
                let tpi_results = self.execute_or_fallback_temporal(
                    graph,
                    prop_id,
                    &tpi,
                    filter,
                    limit,
                    offset,
                    LatestEdgePropertyFilterCollector::new,
                )?;

                let filtered = cpi_results
                    .into_iter()
                    .filter(|n| {
                        n.properties()
                            .temporal()
                            .get_by_id(prop_id)
                            .map(|t| t.is_empty())
                            .unwrap_or(true)
                    })
                    .collect::<HashSet<_>>();

                let combined: Vec<EdgeView<G>> = filtered.into_iter().chain(tpi_results).collect();
                Ok(combined)
            }
            (Some((cpi, _)), None) => self.execute_or_fallback(graph, &cpi, filter, limit, offset),
            (None, Some((tpi, prop_id))) => self.execute_or_fallback_temporal(
                graph,
                prop_id,
                &tpi,
                filter,
                limit,
                offset,
                LatestEdgePropertyFilterCollector::new,
            ),
            _ => fallback_filter_edges(graph, filter, limit, offset),
        }
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        match &filter.prop_ref {
            PropertyRef::ConstantProperty(prop_name) => {
                self.apply_const_property_filter(graph, prop_name, filter, limit, offset)
            }
            PropertyRef::TemporalProperty(prop_name, Temporal::Any) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    EdgePropertyFilterCollector::new,
                ),
            PropertyRef::TemporalProperty(prop_name, Temporal::Latest) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    LatestEdgePropertyFilterCollector::new,
                ),
            PropertyRef::Property(prop_name) => {
                self.apply_combined_property_filter(graph, prop_name, filter, limit, offset)
            }
        }
    }

    fn filter_edge_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let (edge_index, query) = self.query_builder.build_edge_query(filter)?;
        let reader = get_reader(&edge_index.entity_index.index)?;
        let results = match query {
            Some(query) => self.execute_filter_query(graph, query, &reader, limit, offset)?,
            None => fallback_filter_edges(graph, &EdgeFieldFilter(filter.clone()), limit, offset)?,
        };

        Ok(results)
    }

    pub fn filter_edges_internal<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G, G>>, GraphError> {
        match filter {
            CompositeEdgeFilter::Property(filter) => {
                self.filter_property_index(graph, filter, limit, offset)
            }
            CompositeEdgeFilter::Edge(filter) => {
                self.filter_edge_index(graph, filter, limit, offset)
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
    ) -> Result<Vec<EdgeView<G, G>>, GraphError> {
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

    fn resolve_edges_from_edge_ids<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        edge_ids: HashSet<u64>,
    ) -> tantivy::Result<Vec<EdgeView<G>>> {
        let edges = edge_ids
            .into_iter()
            .filter_map(|id| {
                let e_ref = graph.core_edge(EID(id as usize));
                graph.edge(e_ref.src(), e_ref.dst())
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
