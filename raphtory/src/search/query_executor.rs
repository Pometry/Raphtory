use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::view::StaticGraphViewOps,
        graph::{node::NodeView, views::property_filter::CompositeFilter},
    },
    prelude::{GraphViewOps, NodePropertyFilterOps, PropertyFilter, ResetFilter},
    search::{fields, graph_index::GraphIndex, query_builder::QueryBuilder},
};
use std::collections::HashSet;
use itertools::Itertools;
use tantivy::{
    schema::{Field, Value},
    TantivyDocument,
};
use tantivy::collector::{FilterCollector, TopDocs};
use raphtory_api::core::entities::VID;
use crate::prelude::NodeViewOps;

pub struct QueryExecutor<'a> {
    index: &'a GraphIndex,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(index: &'a GraphIndex) -> Self {
        Self { index }
    }

    pub fn execute<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G, G>>, GraphError> {
        match filter {
            CompositeFilter::Single(single_filter) => {
                self.execute_property_filter(graph, single_filter, limit, offset)
            }
            CompositeFilter::And(filters) => {
                let mut results = None;

                for sub_filter in filters {
                    let sub_result = self.execute(graph, sub_filter, limit, offset)?;
                    results = Some(
                        results
                            .map(|r: HashSet<_>| r.intersection(&sub_result).cloned().collect())
                            .unwrap_or(sub_result),
                    );
                }

                Ok(results.unwrap_or_default())
            }
            CompositeFilter::Or(filters) => {
                let mut results = HashSet::new();

                for sub_filter in filters {
                    let sub_result = self.execute(graph, sub_filter, limit, offset)?;
                    results.extend(sub_result);
                }

                Ok(results)
            }
        }
    }

    fn execute_property_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
        limit: usize,
        offset: usize,
    ) -> Result<HashSet<NodeView<G>>, GraphError> {
        let query_builder = QueryBuilder::new(self.index);
        let (property_index, query) = query_builder.build_query(graph, filter)?;

        let results = match query {
            Some(query) => {
                let searcher = property_index.reader.searcher();
                let top_docs =
                    searcher.search(&query, &self.node_filter_collector(graph, limit, offset))?;

                let node_id = property_index.index.schema().get_field(fields::NODE_ID)?;

                top_docs
                    .into_iter()
                    .map(|(_, doc_address)| searcher.doc(doc_address))
                    .filter_map(Result::ok)
                    .filter_map(|doc| self.resolve_node_from_search_result(graph, node_id, doc))
                    .collect::<Vec<_>>()
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

        let unique_results: HashSet<_> = results.into_iter().collect();

        println!("filter: {:?}, result: {:?}", filter, unique_results.iter().map(|n| n.name()).collect_vec());

        Ok(unique_results)
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

    fn node_filter_collector<G: StaticGraphViewOps>(
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
}
