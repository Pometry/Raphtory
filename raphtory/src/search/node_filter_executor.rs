use crate::{
    db::{
        api::view::{internal::FilterOps, BaseFilterOps, StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::filter::{
                internal::CreateFilter,
                model::{
                    node_filter::{CompositeNodeFilter, NodeFilter},
                    property_filter::PropertyRef,
                    Filter,
                },
            },
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter, TimeOps},
    search::{
        collectors::unique_entity_filter_collector::UniqueEntityFilterCollector,
        fallback_filter_nodes, fields, get_reader, graph_index::Index,
        property_index::PropertyIndex, query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::{entities::VID, storage::timeindex::AsTime};
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::{Collector, Count, TopDocs},
    query::Query,
    schema::Value,
    DocAddress, Document, IndexReader, Score, Searcher, TantivyDocument,
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
        filter: impl CreateFilter + std::fmt::Display + std::fmt::Debug,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        let searcher = reader.searcher();
        let collector = UniqueEntityFilterCollector::new(fields::NODE_ID.to_string());
        let node_ids = searcher.search(&query, &collector)?;
        println!("node_ids: {:?}", node_ids);
        let nodes = self.resolve_nodes_from_node_ids(filter, graph, node_ids)?;
        println!("resolved_nodes: {:?}", nodes);

        if offset == 0 && limit >= nodes.len() {
            Ok(nodes)
        } else {
            Ok(nodes.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_filter_property_query<G, C>(
        &self,
        filter: impl CreateFilter + std::fmt::Display + std::fmt::Debug,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<u64>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(fields::NODE_ID.to_string());
        let node_ids = searcher.search(&query, &collector)?;
        let nodes = self.resolve_nodes_from_node_ids(filter, graph, node_ids)?;

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
            Some(query) => {
                self.execute_filter_query(filter.clone(), graph, query, &reader, limit, offset)
            }
            // Fallback to raphtory apis
            None => fallback_filter_nodes(graph, filter, limit, offset),
        }
    }

    fn execute_or_fallback_temporal<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<NodeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => self.execute_filter_property_query(
                filter.clone(),
                graph,
                query,
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
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<u64>>,
    {
        if let Some((tpi, _)) = self
            .index
            .node_index
            .entity_index
            .get_temporal_property_index(graph.node_meta(), prop_name)?
        {
            self.execute_or_fallback_temporal(graph, &tpi, filter, limit, offset, collector_fn)
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
        if !filter.ops.is_empty() {
            return fallback_filter_nodes(graph, filter, limit, offset);
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

    fn filter_node_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        let (node_index, query) = self.query_builder.build_node_query(filter)?;
        println!("query {:?}, filter {}", query, filter);
        let reader = get_reader(&node_index.entity_index.index)?;
        let results = match query {
            Some(query) => self.execute_filter_query(
                CompositeNodeFilter::Node(filter.clone()),
                graph,
                query,
                &reader,
                limit,
                offset,
            )?,
            None => fallback_filter_nodes(
                graph,
                &CompositeNodeFilter::Node(filter.clone()),
                limit,
                offset,
            )?,
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
            CompositeNodeFilter::PropertyWindowed(filter) => {
                let start = filter.entity.start.t();
                let end = filter.entity.end.t();

                let filter = PropertyFilter {
                    prop_ref: filter.prop_ref.clone(),
                    prop_value: filter.prop_value.clone(),
                    operator: filter.operator,
                    ops: filter.ops.clone(),
                    entity: NodeFilter,
                };

                let res =
                    self.filter_property_index(&graph.window(start, end), &filter, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| NodeView::new_internal(graph.clone(), x.node))
                    .collect())
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
        filter: impl CreateFilter + std::fmt::Display + std::fmt::Debug,
        graph: &G,
        node_ids: HashSet<u64>,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        println!("filter {:?}", filter);
        let filtered_graph = graph.filter(filter)?;
        let nodes = node_ids
            .into_iter()
            .filter_map(|id| {
                let n_ref = graph.core_node(VID(id as usize));
                filtered_graph
                    .filter_node(n_ref.as_ref())
                    .then(|| NodeView::new_internal(graph.clone(), VID(id as usize)))
            })
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
