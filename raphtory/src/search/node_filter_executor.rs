use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::{storage::graph::edges::edge_storage_ops::EdgeStorageOps, view::StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::property_filter::{CompositeNodeFilter, Filter},
        },
    },
    prelude::{GraphViewOps, NodePropertyFilterOps, NodeViewOps, PropertyFilter, ResetFilter},
    search::{fields, graph_index::GraphIndex, query_builder::QueryBuilder},
};
use itertools::Itertools;
use raphtory_api::core::entities::{EID, VID};
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::{FilterCollector, TopDocs},
    query::Query,
    schema::{Field, Value},
    DocAddress, Document, Index, IndexReader, Score, Searcher, TantivyDocument,
};

#[derive(Clone, Copy)]
pub struct NodeFilterExecutor<'a> {
    query_builder: QueryBuilder<'a>,
}

impl<'a> NodeFilterExecutor<'a> {
    pub fn new(index: &'a GraphIndex) -> Self {
        Self {
            query_builder: QueryBuilder::new(index),
        }
    }

    fn execute_query<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        query: Box<dyn Query>,
        index: &Arc<Index>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        let searcher = reader.searcher();

        println!("query = {:?}", query);
        let top_docs =
            searcher.search(&query, &self.node_id_filter_collector(graph, limit, offset))?;
        println!();
        Self::print_docs(&searcher, &query, &top_docs);
        println!();

        let node_id = index.schema().get_field(fields::NODE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(graph, node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G>>, GraphError> {
        let (property_index, query) = self.query_builder.build_property_query(graph, filter)?;

        println!();
        println!("Printing property index schema::start");
        Self::print_schema(&property_index.index.schema());
        println!("Printing property index schema::end");
        println!();

        let results = match query {
            Some(query) => self.execute_query(
                graph,
                query,
                &property_index.index,
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

        let unique_results: HashSet<_> = results.into_iter().collect();

        println!(
            "prop filter: {:?}, result: {:?}",
            filter,
            unique_results.iter().map(|n| n.name()).collect_vec()
        );

        Ok(unique_results)
    }

    fn filter_node_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G>>, GraphError> {
        let (node_index, query) = self.query_builder.build_node_query(filter)?;

        println!();
        println!("Printing node index::start");
        node_index.print()?;
        println!("Printing node index::end");
        println!();
        println!("Printing node index schema::start");
        Self::print_schema(&node_index.index.schema());
        println!("Printing node index schema::end");
        println!();

        let results = match query {
            Some(query) => self.execute_query(
                graph,
                query,
                &node_index.index,
                &node_index.reader,
                limit,
                offset,
            )?,
            None => {
                vec![]
            }
        };

        let unique_results: HashSet<_> = results.into_iter().collect();

        println!(
            "node filter: {:?}, result: {:?}",
            filter,
            unique_results.iter().map(|n| n.name()).collect_vec()
        );

        Ok(unique_results)
    }

    pub fn filter_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G, G>>, GraphError> {
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
                            .map(|r: HashSet<_>| r.intersection(&sub_result).cloned().collect())
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

                Ok(results)
            }
        }
    }

    fn node_id_filter_collector<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        limit: usize,
        offset: usize,
    ) -> FilterCollector<TopDocs, impl Fn(u64) -> bool + Clone, u64> {
        let ranking = TopDocs::with_limit(limit).and_offset(offset);
        let graph = graph.clone();
        FilterCollector::new(
            fields::NODE_ID.to_string(),
            move |node_id: u64| graph.has_node(VID(node_id as usize)),
            ranking,
        )
    }

    fn resolve_node_from_search_result<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        node_id: Field,
        doc: TantivyDocument,
    ) -> Option<NodeView<G>> {
        let node_id: usize = doc
            .get_first(node_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let node_id = NodeRef::Internal(node_id.into());
        graph.node(node_id)
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
