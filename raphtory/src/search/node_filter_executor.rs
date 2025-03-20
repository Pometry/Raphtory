use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::StaticGraphViewOps,
        graph::{
            node::NodeView,
            views::property_filter::{CompositeNodeFilter, Filter, PropertyRef, Temporal},
        },
    },
    prelude::{NodePropertyFilterOps, NodeViewOps, PropertyFilter, ResetFilter},
    search::{
        collectors::{
            latest_node_property_filter_collector::LatestNodePropertyFilterCollector,
            node_property_filter_collector::NodePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fields,
        graph_index::GraphIndex,
        property_index::PropertyIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use std::{collections::HashSet, sync::Arc};
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

    fn execute_filter_query<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
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
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let query = self.query_builder.build_property_query::<G>(&pi, filter)?;
        match query {
            Some(query) => self.execute_filter_query(graph, query, &pi.reader, limit, offset),
            // Fallback to raphtory apis
            None => Self::raph_filter_nodes(graph, filter, offset, limit),
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
    ) -> Result<Vec<NodeView<G>>, GraphError>
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
            // Query is none for "is_none" filters because it's cheaper to just ask raphtory
            None => Self::raph_filter_nodes(graph, filter, limit, offset),
        }
    }

    fn apply_const_property_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        if let Some((cpi, _)) = self
            .index
            .node_index
            .entity_index
            .get_const_property_index(graph.node_meta(), prop_name)?
        {
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
    ) -> Result<Vec<NodeView<G>>, GraphError>
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
            Err(GraphError::PropertyNotFound(prop_name.to_string()))
        }
    }

    // Property Semantics:
    // There is a possibility that a const and temporal property share same name. This means that if a node
    // or an edge doesn't have a value for that temporal property, we fall back to its const property value.
    // Otherwise, the temporal property takes precedence.
    //
    // Search semantics:
    // This means that a property filter criteria, say p == 1, is looked for in both the const and temporal
    // property indexes for the given property name (if shared by both const and temporal properties). Now,
    // if the filter matches to docs in const property index but there already is a temporal property with a
    // different value, the doc is rejected i.e., fails the property filter criteria because temporal property
    // takes precedence.
    //          Search p == 1
    //      t_prop      c_prop
    //        T           T
    //        T           F
    //  (p=2) F     (p=1) T
    //        F           F
    //
    // This applies to both node and edge properties.
    fn apply_combined_property_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let cpi = self
            .index
            .node_index
            .entity_index
            .get_const_property_index(graph.node_meta(), prop_name)?;
        let tpi = self
            .index
            .node_index
            .entity_index
            .get_temporal_property_index(graph.node_meta(), prop_name)?;

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
                    LatestNodePropertyFilterCollector::new,
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

                let combined: Vec<NodeView<G>> = filtered.into_iter().chain(tpi_results).collect();
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
                LatestNodePropertyFilterCollector::new,
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
    ) -> Result<Vec<NodeView<G>>, GraphError> {
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
                    NodePropertyFilterCollector::new,
                ),
            PropertyRef::TemporalProperty(prop_name, Temporal::Latest) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    LatestNodePropertyFilterCollector::new,
                ),
            PropertyRef::Property(prop_name) => {
                self.apply_combined_property_filter(graph, prop_name, filter, limit, offset)
            }
        }
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
            Some(query) => self.execute_filter_query(
                graph,
                query,
                &node_index.entity_index.reader,
                limit,
                offset,
            )?,
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
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        match filter {
            CompositeNodeFilter::Property(filter) => {
                self.filter_property_index(graph, filter, limit, offset)
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
                let mut results = HashSet::new();

                for sub_filter in filters {
                    let sub_result = self.filter_nodes(graph, sub_filter, limit, offset)?;
                    results.extend(sub_result);
                }

                Ok(results.into_iter().collect())
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
        self.filter_nodes_internal(graph, filter, limit, offset)
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

    fn raph_filter_nodes<G: StaticGraphViewOps>(
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        Ok(graph
            .nodes()
            .filter_nodes(filter.clone())?
            .into_iter()
            .map(|n| n.reset_filter())
            .skip(offset)
            .take(limit)
            .collect())
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
        for (field_name, _field_entry) in schema.fields() {
            println!("Field Name: '{:?}'", field_name,);
        }
    }

    fn print_schema(schema: &tantivy::schema::Schema) {
        println!("Schema:\n{:?}", schema);
    }
}
