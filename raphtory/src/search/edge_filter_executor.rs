use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{storage::graph::edges::edge_storage_ops::EdgeStorageOps, view::StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            views::property_filter::{CompositeEdgeFilter, CompositeNodeFilter, Filter},
        },
    },
    prelude::{
        EdgePropertyFilterOps, EdgeViewOps, GraphViewOps, NodeViewOps, PropertyFilter, ResetFilter,
    },
    search::{fields, graph_index::GraphIndex, query_builder::QueryBuilder},
};
use itertools::Itertools;
use raphtory_api::core::entities::EID;
use std::{collections::HashSet, sync::Arc};
use tantivy::{
    collector::{FilterCollector, TopDocs},
    query::Query,
    schema::{Field, Value},
    DocAddress, Document, Index, IndexReader, Score, Searcher, TantivyDocument,
};
use crate::db::api::view::internal::CoreGraphOps;

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
        index: &Arc<Index>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G, G>>, GraphError> {
        let searcher = reader.searcher();

        println!("query = {:?}", query);
        let top_docs =
            searcher.search(&query, &self.edge_id_filter_collector(graph, limit, offset))?;
        println!();
        Self::print_docs(&searcher, &query, &top_docs);
        println!();

        let edge_id = index.schema().get_field(fields::EDGE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_edge_from_search_result(graph, edge_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    fn execute_filter_count_query(
        &self,
        query: Box<dyn Query>,
        reader: &IndexReader,
    ) -> Result<usize, GraphError> {
        let searcher = reader.searcher();
        let docs_count = searcher.search(&query, &tantivy::collector::Count)?;
        Ok(docs_count)
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<EdgeView<G>>, GraphError> {
        let prop_name = &filter.prop_name;
        let property_index = self
            .index
            .edge_index
            .get_property_index(graph.edge_meta(), prop_name)?;

        let (property_index, query) = self
            .query_builder
            .build_property_query::<G>(property_index, filter)?;

        println!();
        println!("Printing property index schema::start");
        Self::print_schema(&property_index.index.schema());
        println!("Printing property index schema::end");
        println!();

        let results = match query {
            Some(query) => self.execute_filter_edge_query(
                graph,
                query,
                &property_index.index,
                &property_index.reader,
                limit,
                offset,
            )?,
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

        let unique_results: HashSet<_> = results.into_iter().collect();

        println!(
            "prop filter: {:?}, result: {:?}",
            filter,
            unique_results
                .iter()
                .map(|n| format!("{} -> {}", n.src(), n.dst()))
                .collect_vec()
        );

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
            .edge_index
            .get_property_index(graph.edge_meta(), prop_name)?;

        let (property_index, query) = self.query_builder.build_property_query::<G>(property_index, filter)?;

        // println!();
        // println!("Printing property index schema::start");
        // Self::print_schema(&property_index.index.schema());
        // println!("Printing property index schema::end");
        // println!();

        let results = match query {
            Some(query) => self.execute_filter_count_query(query, &property_index.reader)?,
            None => 0,
        };

        // println!("filter = {}, count = {}", filter, results);

        Ok(results)
    }

    fn filter_edge_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &Filter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<EdgeView<G>>, GraphError> {
        let (edge_index, query) = self.query_builder.build_edge_query(filter)?;

        println!();
        println!("Printing node index::start");
        edge_index.print()?;
        println!("Printing node index::end");
        println!();
        println!("Printing node index schema::start");
        Self::print_schema(&edge_index.index.schema());
        println!("Printing node index schema::end");
        println!();

        let results = match query {
            Some(query) => self.execute_filter_edge_query(
                graph,
                query,
                &edge_index.index,
                &edge_index.reader,
                limit,
                offset,
            )?,
            None => vec![],
        };

        let unique_results: HashSet<_> = results.into_iter().collect();

        println!(
            "edge filter: {:?}, result: {:?}",
            filter,
            unique_results
                .iter()
                .map(|e| format!("{} -> {}", e.src(), e.dst()))
                .collect_vec()
        );

        Ok(unique_results)
    }

    fn filter_count_edge_index(&self, filter: &Filter) -> Result<usize, GraphError> {
        let (edge_index, query) = self.query_builder.build_edge_query(filter)?;

        let results = match query {
            Some(query) => self.execute_filter_count_query(query, &edge_index.reader)?,
            None => 0,
        };

        Ok(results)
    }

    pub fn filter_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<EdgeView<G, G>>, GraphError> {
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
                            .map(|r: HashSet<_>| r.intersection(&sub_result).cloned().collect())
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

                Ok(results)
            }
        }
    }

    pub fn filter_count<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
    ) -> Result<usize, GraphError> {
        match filter {
            CompositeEdgeFilter::Property(filter) => {
                self.filter_count_property_index(graph, filter)
            }
            CompositeEdgeFilter::Edge(filter) => self.filter_count_edge_index(filter),
            CompositeEdgeFilter::And(filters) => {
                let mut results = None;

                for sub_filter in filters {
                    let sub_count = self.filter_count(graph, sub_filter)?;
                    results = Some(
                        results
                            .map(|count| std::cmp::min(count, sub_count))
                            .unwrap_or(sub_count),
                    );
                }

                Ok(results.unwrap_or(0))
            }
            CompositeEdgeFilter::Or(filters) => {
                let mut total_count = 0;
                let mut seen_ids = HashSet::new();

                for sub_filter in filters {
                    let sub_count = self.filter_count(graph, sub_filter)?;

                    if sub_count > 0 {
                        let sub_results = self.filter_edges(graph, sub_filter, sub_count, 0)?;
                        for edge in sub_results {
                            if seen_ids.insert(edge.id()) {
                                total_count += 1; // Count only unique results
                            }
                        }
                    }
                }

                Ok(total_count)
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
        edge_id: Field,
        doc: TantivyDocument,
    ) -> Option<EdgeView<G>> {
        let edge_id: usize = doc
            .get_first(edge_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let core_edge = graph.core_edge(EID(edge_id));
        let layer_ids = graph.layer_ids();
        if !graph.filter_edge(core_edge.as_ref(), layer_ids) {
            return None;
        }
        if graph.nodes_filtered() {
            if !graph.filter_node(graph.core_node_entry(core_edge.src()).as_ref(), layer_ids)
                || !graph.filter_node(graph.core_node_entry(core_edge.dst()).as_ref(), layer_ids)
            {
                return None;
            }
        }
        let e_view = EdgeView::new(graph.clone(), core_edge.out_ref());
        Some(e_view)
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
