use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::{internal::CoreGraphOps, StaticGraphViewOps},
        graph::{
            node::NodeView,
            views::property_filter::{CompositeNodeFilter, Filter},
        },
    },
    prelude::{GraphViewOps, NodePropertyFilterOps, PropertyFilter, ResetFilter},
    search::{
        collectors::{
            unique_filter_collector::UniqueFilterCollector,
            node_property_filter_collector::NodePropertyFilterCollector,
        },
        fields,
        graph_index::GraphIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use std::collections::HashSet;
use tantivy::{
    collector::TopDocs,
    query::Query,
    schema::{Field, Value},
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
        let collector = UniqueFilterCollector::new(
            fields::NODE_ID.to_string(),
            TopDocs::with_limit(limit).and_offset(offset),
            reader.clone(),
            graph.clone(),
        );
        let docs = searcher.search(&query, &collector)?; // TODO: Need to debug this
        let docs = searcher.search(&query, &TopDocs::with_limit(limit).and_offset(offset))?;
        let nodes = self.resolve_nodes_from_search_results(graph, &searcher, docs)?;
        Ok(nodes)
    }

    fn execute_filter_property_query<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        query: Box<dyn Query>,
        prop_id: usize,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        let searcher = reader.searcher();
        let collector =
            NodePropertyFilterCollector::new(fields::NODE_ID.to_string(), prop_id, graph.clone());
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
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let prop_name = &filter.prop_name;
        let (property_index, prop_id) = self
            .index
            .node_index
            .get_property_index(graph.node_meta(), prop_name)?;
        let (property_index, query) = self
            .query_builder
            .build_property_query::<G>(property_index, filter)?;

        let results = match query {
            Some(query) => self.execute_filter_property_query(
                graph,
                query,
                prop_id,
                &property_index.reader,
                limit,
                offset,
            )?,
            None => graph
                .nodes()
                .filter_nodes(filter.clone())?
                .into_iter()
                .map(|n| n.reset_filter())
                .skip(offset)
                .take(limit)
                .collect(),
        };

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

    pub fn filter_nodes<G: StaticGraphViewOps>(
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
                let mut results = Vec::new();

                for sub_filter in filters {
                    let sub_result = self.filter_nodes(graph, sub_filter, limit, offset)?;
                    results.extend(sub_result);
                }

                Ok(results)
            }
        }
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
                let entity_id = VID(node_id);
                graph.node(entity_id)
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
