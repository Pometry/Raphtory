use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::view::StaticGraphViewOps,
        graph::{
            node::NodeView,
            views::property_filter::{CompositeNodeFilter, Filter},
        },
    },
    prelude::{GraphViewOps, NodePropertyFilterOps, NodeViewOps, PropertyFilter, ResetFilter},
    search::{fields, graph_index::GraphIndex, query_builder::QueryBuilder},
};
use itertools::Itertools;
use raphtory_api::core::entities::{GID, VID};
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::{FilterCollector, TopDocs},
    query::Query,
    schema::{Field, Value},
    DocAddress, Document, Index, IndexReader, Score, Searcher, TantivyDocument,
};
use crate::db::api::view::internal::CoreGraphOps;

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
        index: &Arc<Index>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        let searcher = reader.searcher();

        // println!("query = {:?}", query);
        let top_docs =
            searcher.search(&query, &self.node_id_filter_collector(graph, limit, offset))?;
        // println!();
        // Self::print_docs(&searcher, &query, &top_docs);
        // println!();

        let node_id = index.schema().get_field(fields::NODE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(graph, node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    fn execute_filter_count_query(
        &self,
        query: Box<dyn Query>,
        reader: &IndexReader,
    ) -> Result<usize, GraphError> {
        let searcher = reader.searcher();
        let docs_count =
            searcher.search(&query, &tantivy::collector::Count)?;
        Ok(docs_count)
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G>>, GraphError> {
        let prop_name = &filter.prop_name;
        let property_index = self
            .index
            .node_index
            .get_property_index(graph.node_meta(), prop_name)?;
        let (property_index, query) = self.query_builder.build_property_query::<G>(property_index, filter)?;

        // println!();
        // println!("Printing property index schema::start");
        // Self::print_schema(&property_index.index.schema());
        // println!("Printing property index schema::end");
        // println!();

        let results = match query {
            Some(query) => self.execute_filter_nodes_query(
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

        // println!(
        //     "prop filter: {:?}, result: {:?}",
        //     filter,
        //     unique_results.iter().map(|n| n.name()).collect_vec()
        // );

        Ok(unique_results)
    }

    fn filter_count_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
    ) -> Result<usize, GraphError> {
        let prop_name = &filter.prop_name;
        let property_index = self
            .index
            .node_index
            .get_property_index(graph.node_meta(), prop_name)?;
        let (property_index, query) = self.query_builder.build_property_query::<G>(property_index, filter)?;

        // println!();
        // println!("Printing property index schema::start");
        // Self::print_schema(&property_index.index.schema());
        // println!("Printing property index schema::end");
        // println!();

        let results = match query {
            Some(query) => self.execute_filter_count_query(
                query,
                &property_index.reader,
            )?,
            None => 0
        };

        // println!("filter = {}, count = {}", filter, results);

        Ok(results)
    }

    fn filter_node_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G>>, GraphError> {
        let (node_index, query) = self.query_builder.build_node_query(filter)?;

        // println!();
        // println!("Printing node index::start");
        // node_index.print()?;
        // println!("Printing node index::end");
        // println!();
        // println!("Printing node index schema::start");
        // Self::print_schema(&node_index.index.schema());
        // println!("Printing node index schema::end");
        // println!();

        let results = match query {
            Some(query) => self.execute_filter_nodes_query(
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

        // println!(
        //     "node filter: {:?}, result: {:?}",
        //     filter,
        //     unique_results.iter().map(|n| n.name()).collect_vec()
        // );

        Ok(unique_results)
    }

    fn filter_count_node_index(
        &self,
        filter: &Filter,
    ) -> Result<usize, GraphError> {
        let (node_index, query) = self.query_builder.build_node_query(filter)?;

        let results = match query {
            Some(query) => self.execute_filter_count_query(
                query,
                &node_index.reader,
            )?,
            None => 0
        };

        Ok(results)
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

    pub fn filter_count<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
    ) -> Result<usize, GraphError> {
        match filter {
            CompositeNodeFilter::Property(filter) => {
                self.filter_count_property_index(graph, filter)
            }
            CompositeNodeFilter::Node(filter) => {
                self.filter_count_node_index(filter)
            }
            CompositeNodeFilter::And(filters) => {
                let mut seen_ids: Option<HashSet<String>> = None;

                for sub_filter in filters {
                    let sub_count = self.filter_count(graph, sub_filter)?;
                    let effective_limit = std::cmp::max(sub_count, 1);

                    let sub_results = self.filter_nodes(graph, sub_filter, effective_limit, 0)?
                        .into_iter()
                        .map(|node| node.name())
                        .collect::<HashSet<_>>();

                    seen_ids = Some(
                        seen_ids
                            .map(|ids| ids.intersection(&sub_results).cloned().collect())
                            .unwrap_or(sub_results), // Initialize if not already done.
                    );

                    // Early exit if the intersection is empty.
                    if let Some(ref ids) = seen_ids {
                        if ids.is_empty() {
                            return Ok(0);
                        }
                    }
                }

                Ok(seen_ids.map_or(0, |ids| ids.len()))
            }
            CompositeNodeFilter::Or(filters) => {
                let mut total_count = 0;
                let mut seen_ids = HashSet::new();

                for sub_filter in filters {
                    let sub_count = self.filter_count(graph, sub_filter)?;
                    let effective_limit = std::cmp::max(sub_count, 1);

                    if sub_count > 0 {
                        let sub_results = self.filter_nodes(graph, sub_filter, effective_limit, 0)?;
                        for node in sub_results {
                            if seen_ids.insert(node.id()) {
                                total_count += 1; // Count only unique results
                            }
                        }
                    }
                }

                Ok(total_count)
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
