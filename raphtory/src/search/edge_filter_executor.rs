use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{internal::CoreGraphOps, StaticGraphViewOps},
        },
        graph::{
            edge::EdgeView,
            views::property_filter::{CompositeEdgeFilter, Filter},
        },
    },
    prelude::{EdgePropertyFilterOps, GraphViewOps, NodeViewOps, PropertyFilter, ResetFilter},
    search::{
        collectors::{
            edge_property_filter_collector::EdgePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fields,
        graph_index::GraphIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::EID;
use std::collections::HashSet;
use tantivy::{
    collector::{FilterCollector, TopDocs},
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

    fn execute_filter_edge_query<G: StaticGraphViewOps>(
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

    // fn execute_filter_property_query<G: StaticGraphViewOps>(
    //     &self,
    //     graph: &G,
    //     query: Box<dyn Query>,
    //     prop_id: usize,
    //     reader: &IndexReader,
    //     limit: usize,
    //     offset: usize,
    // ) -> Result<Vec<EdgeView<G, G>>, GraphError> {
    //     let searcher = reader.searcher();
    //     let collector =
    //         WindowFilterCollector::new(fields::EDGE_ID.to_string(), prop_id, graph.clone());
    //     let edge_ids = searcher.search(&query, &collector)?;
    //     // let edges = self.resolve_nodes_from_node_ids(graph, edge_ids)?;
    //     Ok(edges.into_iter().skip(offset).take(limit).collect())
    // }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let prop_name = &filter.prop_name;
        let property_index = self
            .index
            .edge_index
            .get_property_index(graph.edge_meta(), prop_name)?;
        let (property_index, query) = self
            .query_builder
            .build_property_query::<G>(property_index, filter)?;

        let results = match query {
            Some(query) => {
                self.execute_filter_edge_query(graph, query, &property_index.reader, limit, offset)?
            }
            // None => graph
            //     .edges()
            //     .filter_edges(filter.clone())?
            //     .into_iter()
            //     .map(|n| n.reset_filter())
            //     .skip(offset)
            //     .take(limit)
            //     .collect(),
            None => vec![],
        };

        Ok(results)
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
                self.execute_filter_edge_query(graph, query, &edge_index.reader, limit, offset)?
            }
            None => vec![],
        };

        Ok(results)
    }

    pub fn filter_edges<G: StaticGraphViewOps>(
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
                let mut results = Vec::new();

                for sub_filter in filters {
                    let sub_result = self.filter_edges(graph, sub_filter, limit, offset)?;
                    results.extend(sub_result);
                }

                Ok(results)
            }
        }
    }

    fn edge_id_filter_collector<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        limit: usize,
        offset: usize,
    ) -> FilterCollector<TopDocs, impl Fn(u64) -> bool + Clone, u64> {
        let ranking = TopDocs::with_limit(limit).and_offset(offset);
        let graph = graph.clone();
        FilterCollector::new(
            fields::EDGE_ID.to_string(),
            move |edge_id: u64| {
                let core_edge = graph.core_edge(EID(edge_id as usize));
                let layer_ids = graph.layer_ids();
                graph.filter_edge(core_edge.as_ref(), layer_ids)
                    && (!graph.nodes_filtered()
                        || (graph.filter_node(
                            graph.core_node_entry(core_edge.src()).as_ref(),
                            layer_ids,
                        ) && graph.filter_node(
                            graph.core_node_entry(core_edge.dst()).as_ref(),
                            layer_ids,
                        )))
            },
            ranking,
        )
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
