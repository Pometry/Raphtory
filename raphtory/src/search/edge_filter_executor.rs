use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{internal::CoreGraphOps, StaticGraphViewOps},
        },
        graph::{
            edge::EdgeView,
            views::property_filter::{
                CompositeEdgeFilter, Filter, FilterOperator, PropertyRef, Temporal,
            },
        },
    },
    prelude::{
        EdgePropertyFilterOps, EdgeViewOps, GraphViewOps, NodeViewOps, PropertyFilter, ResetFilter,
    },
    search::{
        collectors::{
            edge_property_filter_collector::EdgePropertyFilterCollector,
            latest_edge_property_filter_collector::LatestEdgePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fields, get_const_property_index, get_temporal_property_index,
        graph_index::GraphIndex,
        property_index::PropertyIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::{entities::EID, input::input_node::InputNode};
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::{Collector, TopDocs},
    query::Query,
    schema::Value,
    DocAddress, Document, IndexReader, Score, Searcher, TantivyDocument,
};

#[derive(Clone, Copy)]
pub struct EdgeFilterExecutor<'a> {
    index: &'a GraphIndex,
    query_builder: QueryBuilder<'a>,
}

impl<'a> EdgeFilterExecutor<'a> {
    pub fn new(index: &'a GraphIndex) -> Self {
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
        let collector = UniqueEntityFilterCollector::new(
            fields::EDGE_ID.to_string(),
            TopDocs::with_limit(limit).and_offset(offset),
            reader.clone(),
            graph.clone(),
        );
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_edges_from_edge_ids(graph, edge_ids)?;

        Ok(edges)
    }

    fn execute_filter_property_query<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        query: Box<dyn Query>,
        prop_id: usize,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, IndexReader, G) -> C,
    ) -> Result<Vec<EdgeView<G, G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<u64>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(
            fields::EDGE_ID.to_string(),
            prop_id,
            reader.clone(),
            graph.clone(),
        );
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_edges_from_edge_ids(graph, edge_ids)?;
        Ok(edges.into_iter().skip(offset).take(limit).collect())
    }

    fn execute_or_fallback<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let query = self.query_builder.build_property_query::<G>(&pi, filter)?;
        match query {
            Some(query) => self.execute_filter_query(graph, query, &pi.reader, limit, offset),
            // Fallback to raphtory apis
            None => Self::raph_filter_edges(graph, filter, offset, limit),
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
        collector_fn: impl Fn(String, usize, IndexReader, G) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        let query = self.query_builder.build_property_query::<G>(&pi, filter)?;
        match query {
            Some(query) => self.execute_filter_property_query(
                graph,
                query,
                prop_id,
                &pi.reader,
                limit,
                offset,
                collector_fn,
            ),
            // Fallback to raphtory apis
            None => Self::raph_filter_edges(graph, filter, offset, limit),
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
        if let Some((cpi, _)) = get_const_property_index(
            &self.index.edge_index.constant_property_indexes,
            graph.edge_meta(),
            prop_name,
        )? {
            self.execute_or_fallback(graph, &cpi, filter, limit, offset)
        } else {
            Err(GraphError::PropertyNotFound(prop_name.to_string()))
        }
    }

    fn apply_temporal_property_filter<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, IndexReader, G) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        if let Some((tpi, prop_id)) = get_temporal_property_index(
            &self.index.edge_index.temporal_property_indexes,
            graph.edge_meta(),
            prop_name,
        )? {
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
            Err(GraphError::PropertyNotFound(prop_name.to_string()))
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
        let cpi = get_const_property_index(
            &self.index.edge_index.constant_property_indexes,
            graph.edge_meta(),
            prop_name,
        )?;
        let tpi = get_temporal_property_index(
            &self.index.edge_index.temporal_property_indexes,
            graph.edge_meta(),
            prop_name,
        )?;

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

                let mut filtered = cpi_results
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
            _ => Err(GraphError::PropertyNotFound(prop_name.to_string())),
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

        let results = match query {
            Some(query) => {
                self.execute_filter_query(graph, query, &edge_index.reader, limit, offset)?
            }
            None => vec![],
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
            CompositeEdgeFilter::And(filters) => {
                let mut results = None;

                for sub_filter in filters {
                    let sub_result = self.filter_edges(graph, sub_filter, limit, offset)?;
                    results = Some(
                        results
                            .map(|r: Vec<_>| {
                                r.into_iter()
                                    .filter(|item| sub_result.contains(item))
                                    .collect::<Vec<_>>() // Ensure intersection results stay in a Vec
                            })
                            .unwrap_or(sub_result),
                    );
                }

                Ok(results.unwrap_or_default())
            }
            CompositeEdgeFilter::Or(filters) => {
                let mut results = HashSet::new();

                for sub_filter in filters {
                    let sub_result = self.filter_edges(graph, sub_filter, limit, offset)?;
                    results.extend(sub_result);
                }

                Ok(results.into_iter().collect())
            }
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

    fn raph_filter_edges<G: StaticGraphViewOps>(
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        match filter.operator {
            FilterOperator::IsNone => Ok(match &filter.prop_ref {
                PropertyRef::Property(prop_name) => graph
                    .edges()
                    .into_iter()
                    .filter(|e| e.properties().get(prop_name).is_none())
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>(),
                PropertyRef::ConstantProperty(prop_name) => graph
                    .edges()
                    .into_iter()
                    .filter(|e| e.properties().constant().get(prop_name).is_none())
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>(),
                PropertyRef::TemporalProperty(prop_name, temp) => graph
                    .edges()
                    .into_iter()
                    .filter(|e| e.properties().temporal().get(prop_name).is_none())
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>(),
            }),
            _ => Err(GraphError::NotSupported),
        }
    }

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
                    println!("Score: {}, Document: {}", score, doc.to_json(&schema));
                }
                Err(e) => {
                    println!("Failed to retrieve document: {:?}", e);
                }
            }
        }
    }

    fn print_schema_fields(schema: &tantivy::schema::Schema) {
        println!("Schema fields and their IDs:");
        for (field_name, field_entry) in schema.fields() {
            println!("Field Name: '{:?}'", field_name,);
        }
    }

    fn print_schema(schema: &tantivy::schema::Schema) {
        println!("Schema:\n{:?}", schema);
    }
}
