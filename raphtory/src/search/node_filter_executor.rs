use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::TemporalPropertyViewOps,
            view::{internal::CoreGraphOps, StaticGraphViewOps},
        },
        graph::{
            node::NodeView,
            views::property_filter::{CompositeNodeFilter, Filter},
        },
    },
    prelude::{GraphViewOps, NodePropertyFilterOps, NodeViewOps, PropertyFilter, ResetFilter},
    search::{
        collectors::{
            latest_node_property_filter_collector::LatestNodePropertyFilterCollector,
            node_property_filter_collector::NodePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fields, get_property_indexes,
        graph_index::GraphIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use std::collections::HashSet;
use tantivy::{
    collector::{Collector, TopDocs},
    query::Query,
    schema::Value,
    DocAddress, Document, IndexReader, Score, Searcher, TantivyDocument,
};

#[derive(Clone, Copy)]
pub struct NodeFilterExecutor<'a> {
    index: &'a GraphIndex,
    query_builder: QueryBuilder<'a>,
}

impl<'a> NodeFilterExecutor<'a> {
    pub fn new(index: &'a GraphIndex) -> Self {
        Self {
            index,
            query_builder: QueryBuilder::new(index),
        }
    }

    fn execute_filter_nodes_query<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        let searcher = reader.searcher();
        let collector = UniqueEntityFilterCollector::new(
            fields::NODE_ID.to_string(),
            TopDocs::with_limit(limit).and_offset(offset),
            reader.clone(),
            graph.clone(),
        );
        let node_ids = searcher.search(&query, &collector)?;
        let nodes = self.resolve_nodes_from_node_ids(graph, node_ids)?;
        Ok(nodes)
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
    ) -> Result<Vec<NodeView<G, G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<u64>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(
            fields::NODE_ID.to_string(),
            prop_id,
            reader.clone(),
            graph.clone(),
        );
        let node_ids = searcher.search(&query, &collector)?;
        let nodes = self.resolve_nodes_from_node_ids(graph, node_ids)?;
        Ok(nodes.into_iter().skip(offset).take(limit).collect())
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
        latest: bool,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let prop_name = &filter.prop_name;
        let (cpi, tpi) = get_property_indexes(
            &self.index.node_index.constant_property_indexes,
            &self.index.node_index.temporal_property_indexes,
            graph.node_meta(),
            prop_name,
        )?;
        let results = match (cpi, tpi) {
            (
                Some((const_property_index, const_prop_id)),
                Some((temporal_property_index, temporal_prop_id)),
            ) => {
                let (const_property_index, const_property_index_query) = self
                    .query_builder
                    .build_property_query::<G>(const_property_index, filter)?;
                let const_property_index_results = match const_property_index_query {
                    Some(query) => {
                        if latest {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                const_prop_id,
                                &const_property_index.reader,
                                limit,
                                offset,
                                LatestNodePropertyFilterCollector::new,
                            )?
                        } else {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                const_prop_id,
                                &const_property_index.reader,
                                limit,
                                offset,
                                NodePropertyFilterCollector::new,
                            )?
                        }
                    }
                    None => graph
                        .nodes()
                        .filter_nodes(filter.clone())?
                        .into_iter()
                        .map(|n| n.reset_filter())
                        .skip(offset)
                        .take(limit)
                        .collect(),
                };

                let (temporal_property_index, temporal_property_index_query) =
                    self.query_builder
                        .build_property_query::<G>(temporal_property_index, filter)?;
                let temporal_property_index_results = match temporal_property_index_query {
                    Some(query) => {
                        if latest {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                temporal_prop_id,
                                &temporal_property_index.reader,
                                limit,
                                offset,
                                LatestNodePropertyFilterCollector::new,
                            )?
                        } else {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                temporal_prop_id,
                                &temporal_property_index.reader,
                                limit,
                                offset,
                                NodePropertyFilterCollector::new,
                            )?
                        }
                    }
                    None => graph
                        .nodes()
                        .filter_nodes(filter.clone())?
                        .into_iter()
                        .map(|n| n.reset_filter())
                        .skip(offset)
                        .take(limit)
                        .collect(),
                };

                let mut filtered = const_property_index_results.into_iter().filter(|n| {
                    n.properties()
                        .temporal()
                        .get_by_id(temporal_prop_id)
                        .map(|t| t.is_empty())
                        .unwrap_or(true)
                });

                let mut combined: HashSet<NodeView<G, G>> = filtered.into_iter().collect();
                // println!("filtered = {:?}", combined.iter().map(|n| n.name()).collect::<Vec<_>>());
                combined.extend(temporal_property_index_results);
                // println!("combined = {:?}", combined.iter().map(|n| n.name()).collect::<Vec<_>>());
                Ok(combined.into_iter().collect())
            }
            (Some((const_property_index, const_prop_id)), None) => {
                let (const_property_index, const_property_index_query) = self
                    .query_builder
                    .build_property_query::<G>(const_property_index, filter)?;
                let const_property_index_results = match const_property_index_query {
                    Some(query) => {
                        if latest {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                const_prop_id,
                                &const_property_index.reader,
                                limit,
                                offset,
                                LatestNodePropertyFilterCollector::new,
                            )?
                        } else {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                const_prop_id,
                                &const_property_index.reader,
                                limit,
                                offset,
                                NodePropertyFilterCollector::new,
                            )?
                        }
                    }
                    None => graph
                        .nodes()
                        .filter_nodes(filter.clone())?
                        .into_iter()
                        .map(|n| n.reset_filter())
                        .skip(offset)
                        .take(limit)
                        .collect(),
                };
                Ok(const_property_index_results)
            }
            (None, Some((temporal_property_index, temporal_prop_id))) => {
                let (temporal_property_index, temporal_property_index_query) =
                    self.query_builder
                        .build_property_query::<G>(temporal_property_index, filter)?;
                let temporal_property_index_results = match temporal_property_index_query {
                    Some(query) => {
                        if latest {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                temporal_prop_id,
                                &temporal_property_index.reader,
                                limit,
                                offset,
                                LatestNodePropertyFilterCollector::new,
                            )?
                        } else {
                            self.execute_filter_property_query(
                                graph,
                                query,
                                temporal_prop_id,
                                &temporal_property_index.reader,
                                limit,
                                offset,
                                NodePropertyFilterCollector::new,
                            )?
                        }
                    }
                    None => graph
                        .nodes()
                        .filter_nodes(filter.clone())?
                        .into_iter()
                        .map(|n| n.reset_filter())
                        .skip(offset)
                        .take(limit)
                        .collect(),
                };
                Ok(temporal_property_index_results)
            }
            _ => Err(GraphError::PropertyNotFound(prop_name.to_string())),
        }?;

        Ok(results)
    }

    fn filter_node_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let (node_index, query) = self.query_builder.build_node_query(filter)?;

        let results = match query {
            Some(query) => {
                self.execute_filter_nodes_query(graph, query, &node_index.reader, limit, offset)?
            }
            None => {
                vec![]
            }
        };

        Ok(results)
    }

    pub fn filter_nodes_internal<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
        latest: bool,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        match filter {
            CompositeNodeFilter::Property(filter) => {
                self.filter_property_index(graph, filter, limit, offset, latest)
            }
            CompositeNodeFilter::Node(filter) => {
                self.filter_node_index(graph, filter, limit, offset)
            }
            CompositeNodeFilter::And(filters) => {
                let mut results = None;

                for sub_filter in filters {
                    let sub_result = self.filter_nodes(graph, sub_filter, limit, offset)?;

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
            CompositeNodeFilter::Or(filters) => {
                let mut results = Vec::new();

                for sub_filter in filters {
                    let sub_result = self.filter_nodes(graph, sub_filter, limit, offset)?;
                    results.extend(sub_result);
                }

                Ok(results)
            }
        }
    }

    pub fn filter_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        self.filter_nodes_internal(graph, filter, limit, offset, false)
    }

    pub fn filter_nodes_latest<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        self.filter_nodes_internal(graph, filter, limit, offset, true)
    }

    fn resolve_nodes_from_search_results<'graph, G: StaticGraphViewOps>(
        &self,
        graph: &G,
        searcher: &Searcher,
        docs: Vec<(Score, DocAddress)>,
    ) -> tantivy::Result<Vec<NodeView<G>>> {
        let schema = searcher.schema();
        let node_id_field = schema.get_field(fields::NODE_ID)?;

        let nodes = docs
            .into_iter()
            .filter_map(|(_score, doc_address)| {
                let doc = searcher.doc::<TantivyDocument>(doc_address).ok()?;
                let node_id: usize = doc
                    .get_first(node_id_field)
                    .and_then(|value| value.as_u64())?
                    .try_into()
                    .ok()?;
                graph.node(VID(node_id))
            })
            .collect::<Vec<_>>();

        Ok(nodes)
    }

    fn resolve_nodes_from_node_ids<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        node_ids: HashSet<u64>,
    ) -> tantivy::Result<Vec<NodeView<G>>> {
        let nodes = node_ids
            .into_iter()
            .filter_map(|id| graph.node(VID(id as usize)))
            .collect_vec();
        Ok(nodes)
    }

    fn print_docs(searcher: &Searcher, top_docs: &Vec<(Score, DocAddress)>) {
        // println!("Top Docs (debugging):");
        // println!("Query:{:?}", query,);
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
