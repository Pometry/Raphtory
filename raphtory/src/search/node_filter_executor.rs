use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{
            node::NodeView,
            views::filter::model::{
                node_filter::{CompositeNodeFilter, NodeFilter, NodeNameFilter, NodeTypeFilter},
                property_filter::{PropertyRef, Temporal},
                Filter,
            },
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter},
    search::{
        collectors::{
            first_node_property_filter_collector::FirstNodePropertyFilterCollector,
            latest_node_property_filter_collector::LatestNodePropertyFilterCollector,
            node_property_filter_collector::NodePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fallback_filter_nodes, fields, get_reader,
        graph_index::Index,
        property_index::PropertyIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::Collector, query::Query, schema::Value, DocAddress, Document, IndexReader, Score,
    Searcher, TantivyDocument,
};

#[derive(Clone, Copy)]
pub struct NodeFilterExecutor<'a> {
    index: &'a Index,
    query_builder: QueryBuilder<'a>,
}

impl<'a> NodeFilterExecutor<'a> {
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
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        let searcher = reader.searcher();
        let collector = UniqueEntityFilterCollector::new(fields::NODE_ID.to_string());
        let node_ids = searcher.search(&query, &collector)?;
        let nodes = self.resolve_nodes_from_node_ids(graph, node_ids)?;

        if offset == 0 && limit >= nodes.len() {
            Ok(nodes)
        } else {
            Ok(nodes.into_iter().skip(offset).take(limit).collect())
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
    ) -> Result<Vec<NodeView<'static, G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<u64>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(fields::NODE_ID.to_string(), prop_id, graph.clone());
        let node_ids = searcher.search(&query, &collector)?;
        let nodes = self.resolve_nodes_from_node_ids(graph, node_ids)?;

        if offset == 0 && limit >= nodes.len() {
            Ok(nodes)
        } else {
            Ok(nodes.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_or_fallback<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<NodeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => self.execute_filter_query(graph, query, &reader, limit, offset),
            // Fallback to raphtory apis
            None => fallback_filter_nodes(graph, filter, limit, offset),
        }
    }

    fn execute_or_fallback_temporal<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        prop_id: usize,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<NodeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, G) -> C,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError>
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
            None => fallback_filter_nodes(graph, filter, limit, offset),
        }
    }

    fn apply_metadata_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter<NodeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        if let Some((cpi, _)) = self
            .index
            .node_index
            .entity_index
            .get_metadata_index(graph.node_meta(), prop_name)?
        {
            self.execute_or_fallback(graph, &cpi, filter, limit, offset)
        } else {
            fallback_filter_nodes(graph, filter, limit, offset)
        }
    }

    fn apply_temporal_property_filter<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter<NodeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String, usize, G) -> C,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        if let Some((tpi, prop_id)) = self
            .index
            .node_index
            .entity_index
            .get_temporal_property_index(graph.node_meta(), prop_name)?
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
            fallback_filter_nodes(graph, filter, limit, offset)
        }
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter<NodeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        if filter.list_agg.is_some() {
            return fallback_filter_nodes(graph, filter, limit, offset);
        }

        match &filter.prop_ref {
            PropertyRef::Metadata(prop_name) => {
                self.apply_metadata_filter(graph, prop_name, filter, limit, offset)
            }
            PropertyRef::TemporalProperty(prop_name, Temporal::Any) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    NodePropertyFilterCollector::new,
                ),
            PropertyRef::TemporalProperty(prop_name, Temporal::Latest)
            | PropertyRef::Property(prop_name) => self.apply_temporal_property_filter(
                graph,
                prop_name,
                filter,
                limit,
                offset,
                LatestNodePropertyFilterCollector::new,
            ),
            PropertyRef::TemporalProperty(prop_name, Temporal::First) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    FirstNodePropertyFilterCollector::new,
                ),
            PropertyRef::TemporalProperty(_, Temporal::All) => {
                fallback_filter_nodes(graph, filter, limit, offset)
            }
        }
    }

    fn filter_node_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        let (node_index, query) = self.query_builder.build_node_query(filter)?;
        let reader = get_reader(&node_index.entity_index.index)?;
        let results = match query {
            Some(query) => self.execute_filter_query(graph, query, &reader, limit, offset)?,
            None => match filter.field_name.as_str() {
                "node_name" => {
                    fallback_filter_nodes(graph, &NodeNameFilter(filter.clone()), limit, offset)?
                }
                "node_type" => {
                    fallback_filter_nodes(graph, &NodeTypeFilter(filter.clone()), limit, offset)?
                }
                _ => vec![],
            },
        };

        Ok(results)
    }

    pub fn filter_nodes_internal<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        match filter {
            CompositeNodeFilter::Property(filter) => {
                self.filter_property_index(graph, filter, limit, offset)
            }
            CompositeNodeFilter::Node(filter) => {
                self.filter_node_index(graph, filter, limit, offset)
            }
            CompositeNodeFilter::And(left, right) => {
                let left_result = self.filter_nodes(graph, left, limit, offset)?;
                let right_result = self.filter_nodes(graph, right, limit, offset)?;

                let left_set: HashSet<_> = left_result.into_iter().collect();
                let intersection = right_result
                    .into_iter()
                    .filter(|n| left_set.contains(n))
                    .collect::<Vec<_>>();

                Ok(intersection)
            }
            CompositeNodeFilter::Or(left, right) => {
                let left_result = self.filter_nodes(graph, left, limit, offset)?;
                let right_result = self.filter_nodes(graph, right, limit, offset)?;

                let mut combined = HashSet::new();
                combined.extend(left_result);
                combined.extend(right_result);

                Ok(combined.into_iter().collect())
            }
            CompositeNodeFilter::Not(_) => fallback_filter_nodes(graph, filter, limit, offset),
        }
    }

    pub fn filter_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        self.filter_nodes_internal(graph, filter, limit, offset)
    }

    #[allow(dead_code)]
    // Useful for debugging
    fn resolve_nodes_from_search_results<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        searcher: &Searcher,
        docs: Vec<(Score, DocAddress)>,
    ) -> tantivy::Result<Vec<NodeView<'static, G>>> {
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
    ) -> tantivy::Result<Vec<NodeView<'static, G>>> {
        let nodes = node_ids
            .into_iter()
            .filter_map(|id| graph.node(VID(id as usize)))
            .collect_vec();
        Ok(nodes)
    }

    #[allow(dead_code)]
    // Useful for debugging
    fn print_docs(searcher: &Searcher, top_docs: &Vec<(Score, DocAddress)>) {
        // println!("Top Docs (debugging):");
        // println!("Query:{:?}", query,);
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
