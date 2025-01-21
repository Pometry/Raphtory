use crate::{
    core::{
        entities::{nodes::node_ref::NodeRef, EID, VID},
        storage::timeindex::AsTime,
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::internal::TemporalPropertiesOps,
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{
                internal::{core_ops::CoreGraphOps, InternalIndexSearch, NodeFilterOps},
                StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::property_filter::{CompositeEdgeFilter, CompositeNodeFilter},
        },
    },
    prelude::*,
    search::{
        edge_filter_executor::EdgeFilterExecutor, fields, graph_index::GraphIndex,
        latest_value_collector::LatestValueCollector, node_filter_collector::NodeFilterCollector,
        node_filter_executor::NodeFilterExecutor,
    },
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use std::{fmt::Debug, ops::Deref};
use tantivy::{
    collector::{FilterCollector, TopDocs},
    query::{Query, QueryParser, RangeQuery},
    schema::{Field, Schema, Value, FAST, INDEXED, STORED},
    Document, Index, TantivyDocument,
};

#[derive(Copy, Clone)]
pub struct Searcher<'a> {
    pub(crate) index: &'a GraphIndex,
    node_filter_executor: NodeFilterExecutor<'a>,
    edge_filter_executor: EdgeFilterExecutor<'a>,
}

impl<'a> Searcher<'a> {
    pub(crate) fn new(index: &'a GraphIndex) -> Self {
        let node_query_executor = NodeFilterExecutor::new(index);
        let edge_query_executor = EdgeFilterExecutor::new(index);

        Self {
            index,
            node_filter_executor: node_query_executor,
            edge_filter_executor: edge_query_executor,
        }
    }

    // For persistent graph , we need 3 args: node id, time of the event, and the prop id
    // Dedup: unique results returned from the query itself
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

    fn edge_filter_collector<G: StaticGraphViewOps>(
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

    pub fn search_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let result = self
            .node_filter_executor
            .filter_nodes(graph, filter, limit, offset)?;

        Ok(result.into_iter().collect_vec())
    }

    pub fn search_nodes_count<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
    ) -> Result<usize, GraphError> {
        let count = self.node_filter_executor.filter_count(graph, filter)?;
        Ok(count)
    }

    pub fn search_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let result = self
            .edge_filter_executor
            .filter_edges(graph, filter, limit, offset)?;

        Ok(result.into_iter().collect_vec())
    }

    pub fn search_edges_count<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
    ) -> Result<usize, GraphError> {
        let count = self.edge_filter_executor.filter_count(graph, filter)?;
        Ok(count)
    }

    pub fn fuzzy_search_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        q: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let searcher = self.index.node_index.reader.searcher();
        let mut query_parser = self.index.node_parser()?;

        self.index
            .node_index
            .index
            .schema()
            .fields()
            .for_each(|(f, _)| query_parser.set_field_fuzzy(f, prefix, levenshtein_distance, true));

        let query = query_parser.parse_query(q)?;

        let top_docs =
            searcher.search(&query, &self.node_filter_collector(graph, limit, offset))?;

        let node_id = self
            .index
            .node_index
            .index
            .schema()
            .get_field(fields::NODE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_node_from_search_result(graph, node_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn fuzzy_search_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        q: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let searcher = self.index.edge_index.reader.searcher();
        let mut query_parser = self.index.edge_parser()?;
        self.index
            .edge_index
            .index
            .schema()
            .fields()
            .for_each(|(f, _)| query_parser.set_field_fuzzy(f, prefix, levenshtein_distance, true));

        let query = query_parser.parse_query(q)?;

        let top_docs =
            searcher.search(&query, &self.edge_filter_collector(graph, limit, offset))?;

        let edge_id = self
            .index
            .edge_index
            .index
            .schema()
            .get_field(fields::EDGE_ID)?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_edge_from_search_result(graph, edge_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }
}

#[cfg(test)]
mod search_tests {
    use super::*;
    use crate::db::{
        api::{
            mutation::internal::DelegateDeletionOps,
            view::{internal::InternalIndexSearch, SearchableGraphOps},
        },
        graph::views::{deletion_graph::PersistentGraph, property_filter::Filter},
    };
    use proptest::collection::vec;
    use raphtory_api::core::utils::logging::global_info_logger;
    use std::time::SystemTime;
    use tantivy::{doc, query::AllQuery, schema::TEXT, DocAddress, Order};
    use tracing::info;

    #[cfg(test)]
    mod search_nodes {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::property_filter::{CompositeNodeFilter, Filter},
            },
            prelude::{AdditionOps, Graph, NodeViewOps, PropertyFilter},
        };

        fn search_nodes_by_composite_filter(filter: &CompositeNodeFilter) -> Vec<String> {
            let graph = Graph::new();
            graph
                .add_node(1, 1, [("p1", "shivam_kapoor")], Some("fire_nation"))
                .unwrap();
            graph
                .add_node(
                    2,
                    2,
                    [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_node(3, 3, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
                .unwrap();
            graph.add_node(3, 4, [("p4", "pometry")], None).unwrap();
            graph.add_node(4, 4, [("p5", 12u64)], None).unwrap();

            let mut results = graph
                .search_nodes(&filter, 5, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();

            results
        }

        #[test]
        fn test_search_nodes_by_composite_filter() {
            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", 3u64)),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", "shivam")),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "2"]);

            let filter = CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", "pometry")),
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Property(PropertyFilter::eq("p2", 6u64)),
                    CompositeNodeFilter::Property(PropertyFilter::eq("p3", 1u64)),
                ]),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);

            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Node(Filter::eq("node_type", "fire_nation")),
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", "prop1")),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, Vec::<String>::new());
        }

        #[test]
        fn search_nodes_for_node_name_eq() {
            let filter = CompositeNodeFilter::Node(Filter::eq("node_name", "3"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_node_name_ne() {
            let filter = CompositeNodeFilter::Node(Filter::ne("node_name", "2"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "3", "4"]);
        }

        #[test]
        fn search_nodes_for_node_name_in() {
            let filter = CompositeNodeFilter::Node(Filter::any("node_name", vec!["1".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1"]);

            let filter =
                CompositeNodeFilter::Node(Filter::any("node_name", vec!["2".into(), "3".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_node_name_not_in() {
            let filter = CompositeNodeFilter::Node(Filter::not_any("node_name", vec!["1".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3", "4"]);
        }

        #[test]
        fn search_nodes_for_node_type_eq() {
            let filter = CompositeNodeFilter::Node(Filter::eq("node_type", "fire_nation"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "3"]);
        }

        #[test]
        fn search_nodes_for_node_type_ne() {
            let filter = CompositeNodeFilter::Node(Filter::ne("node_type", "fire_nation"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "4"]);
        }

        #[test]
        fn search_nodes_for_node_type_in() {
            let filter =
                CompositeNodeFilter::Node(Filter::any("node_type", vec!["fire_nation".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "3"]);

            let filter = CompositeNodeFilter::Node(Filter::any(
                "node_type",
                vec!["fire_nation".into(), "air_nomads".into()],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "2", "3"]);
        }

        #[test]
        fn search_nodes_for_node_type_not_in() {
            let filter =
                CompositeNodeFilter::Node(Filter::not_any("node_type", vec!["fire_nation".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "4"]);
        }

        #[test]
        fn search_nodes_for_property_eq() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2"]);
        }

        #[test]
        fn search_nodes_for_property_ne() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::ne("p2", 2u64));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_property_lt() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::lt("p2", 10u64));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_le() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::le("p2", 6u64));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_gt() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::gt("p2", 2u64));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_property_ge() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::ge("p2", 2u64));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_in() {
            let filter =
                CompositeNodeFilter::Property(PropertyFilter::any("p2", vec![Prop::U64(6)]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);

            let filter = CompositeNodeFilter::Property(PropertyFilter::any(
                "p2",
                vec![Prop::U64(2), Prop::U64(6)],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_not_in() {
            let filter =
                CompositeNodeFilter::Property(PropertyFilter::not_any("p2", vec![Prop::U64(6)]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2"]);
        }

        #[test]
        fn search_nodes_for_property_is_some() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::is_some("p2"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_is_none() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::is_none("p2"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "4"]);
        }

        #[test]
        fn test_search_nodes_by_props_added_at_different_times() {
            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p4", "pometry")),
                CompositeNodeFilter::Property(PropertyFilter::eq("p5", 12u64)),
            ]);

            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["4"]);
        }
    }

    #[cfg(test)]
    mod search_nodes_count {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::property_filter::{CompositeNodeFilter, Filter},
            },
            prelude::{AdditionOps, Graph, PropertyFilter},
        };

        fn search_nodes_count_by_composite_filter(filter: &CompositeNodeFilter) -> usize {
            let graph = Graph::new();
            graph
                .add_node(1, 1, [("p1", "shivam_kapoor")], Some("fire_nation"))
                .unwrap();
            graph
                .add_node(
                    2,
                    2,
                    [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_node(3, 3, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
                .unwrap();

            let results = graph
                .search_nodes_count(&filter)
                .expect("Failed to search for nodes");

            results
        }

        #[test]
        fn test_search_count_nodes_by_composite_filter() {
            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", 3u64)),
            ]);
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 0);

            let filter = CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", "shivam")),
            ]);
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);

            let filter = CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", "pometry")),
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Property(PropertyFilter::eq("p2", 6u64)),
                    CompositeNodeFilter::Property(PropertyFilter::eq("p3", 1u64)),
                ]),
            ]);
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Node(Filter::eq("node_type", "fire_nation")),
                CompositeNodeFilter::Property(PropertyFilter::eq("p1", "prop1")),
            ]);
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 0);
        }

        #[test]
        fn search_count_nodes_for_node_name_eq() {
            let filter = CompositeNodeFilter::Node(Filter::eq("node_name", "3"));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_node_name_ne() {
            let filter = CompositeNodeFilter::Node(Filter::ne("node_name", "2"));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_node_name_in() {
            let filter = CompositeNodeFilter::Node(Filter::any("node_name", vec!["1".into()]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter =
                CompositeNodeFilter::Node(Filter::any("node_name", vec!["2".into(), "3".into()]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_node_name_not_in() {
            let filter = CompositeNodeFilter::Node(Filter::not_any("node_name", vec!["1".into()]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_node_type_eq() {
            let filter = CompositeNodeFilter::Node(Filter::eq("node_type", "fire_nation"));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_node_type_ne() {
            let filter = CompositeNodeFilter::Node(Filter::ne("node_type", "fire_nation"));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_node_type_in() {
            let filter =
                CompositeNodeFilter::Node(Filter::any("node_type", vec!["fire_nation".into()]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);

            let filter = CompositeNodeFilter::Node(Filter::any(
                "node_type",
                vec!["fire_nation".into(), "air_nomads".into()],
            ));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 3);
        }

        #[test]
        fn search_count_nodes_for_node_type_not_in() {
            let filter =
                CompositeNodeFilter::Node(Filter::not_any("node_type", vec!["fire_nation".into()]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_property_eq() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_property_ne() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::ne("p2", 2u64));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_property_lt() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::lt("p2", 10u64));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_property_le() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::le("p2", 6u64));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_property_gt() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::gt("p2", 2u64));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_property_ge() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::ge("p2", 2u64));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_property_in() {
            let filter =
                CompositeNodeFilter::Property(PropertyFilter::any("p2", vec![Prop::U64(6)]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter = CompositeNodeFilter::Property(PropertyFilter::any(
                "p2",
                vec![Prop::U64(2), Prop::U64(6)],
            ));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_nodes_for_property_not_in() {
            let filter =
                CompositeNodeFilter::Property(PropertyFilter::not_any("p2", vec![Prop::U64(6)]));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_nodes_for_property_is_some() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::is_some("p2"));
            let results = search_nodes_count_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        // #[test]
        // fn search_count_nodes_for_property_is_none() {
        //     let filter = CompositeNodeFilter::Property(PropertyFilter::is_none("p2"));
        //     let results = search_nodes_count_by_composite_filter(&filter);
        //     assert_eq!(results, 1);
        // }
    }

    #[cfg(test)]
    mod search_edges {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::property_filter::{CompositeEdgeFilter, Filter},
            },
            prelude::{AdditionOps, EdgeViewOps, Graph, NodeViewOps, PropertyFilter},
        };

        fn search_edges_by_composite_filter(filter: &CompositeEdgeFilter) -> Vec<(String, String)> {
            let graph = Graph::new();
            graph
                .add_edge(1, 1, 2, [("p1", "shivam_kapoor")], Some("fire_nation"))
                .unwrap();
            graph
                .add_edge(
                    2,
                    2,
                    3,
                    [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_edge(3, 3, 1, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
                .unwrap();

            let mut results = graph
                .search_edges(&filter, 5, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|e| (e.src().name(), e.dst().name()))
                .collect::<Vec<_>>();
            results.sort();

            results
        }

        #[test]
        fn test_search_edges_by_composite_filter() {
            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", 3u64)),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", "shivam")),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );

            let filter = CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", "pometry")),
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 6u64)),
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p3", 1u64)),
                ]),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);

            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::eq("from", "13")),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", "prop1")),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, Vec::<(String, String)>::new());
        }

        #[test]
        fn search_edges_for_src_from_eq() {
            let filter = CompositeEdgeFilter::Edge(Filter::eq("from", "2"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("2".into(), "3".into())]);
        }

        #[test]
        fn search_edges_for_src_to_ne() {
            let filter = CompositeEdgeFilter::Edge(Filter::ne("to", "2"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_to_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::any("to", vec!["2".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("1".into(), "2".into())]);

            let filter = CompositeEdgeFilter::Edge(Filter::any("to", vec!["2".into(), "3".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_to_not_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::not_any("to", vec!["1".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_from_eq() {
            let filter = CompositeEdgeFilter::Edge(Filter::eq("from", "3"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn search_edges_for_from_ne() {
            let filter = CompositeEdgeFilter::Edge(Filter::ne("from", "1"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_from_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::any("from", vec!["1".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("1".into(), "2".into())]);

            let filter =
                CompositeEdgeFilter::Edge(Filter::any("from", vec!["1".into(), "2".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_from_not_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::not_any("from", vec!["1".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_property_eq() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("2".into(), "3".into())]);
        }

        #[test]
        fn search_edges_for_property_ne() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::ne("p2", 2u64));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn search_edges_for_property_lt() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::lt("p2", 10u64));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_property_le() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::le("p2", 6u64));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_property_gt() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::gt("p2", 2u64));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn search_edges_for_property_ge() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::ge("p2", 2u64));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_property_in() {
            let filter =
                CompositeEdgeFilter::Property(PropertyFilter::any("p2", vec![Prop::U64(6)]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);

            let filter = CompositeEdgeFilter::Property(PropertyFilter::any(
                "p2",
                vec![Prop::U64(2), Prop::U64(6)],
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        #[test]
        fn search_edges_for_property_not_in() {
            let filter =
                CompositeEdgeFilter::Property(PropertyFilter::not_any("p2", vec![Prop::U64(6)]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("2".into(), "3".into())]);
        }

        #[test]
        fn search_edges_for_property_is_some() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::is_some("p2"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "3".into()), ("3".into(), "1".into())]
            );
        }

        // #[test]
        // fn search_edges_for_property_is_none() {
        //     let filter = CompositeNodeFilter::Property(PropertyFilter::is_none("p2"));
        //     let results = search_nodes_by_composite_filter(&filter);
        //     assert_eq!(results, vec!["1"]);
        // }

        #[test]
        fn search_edge_by_src_dst() {
            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::eq("from", "3")),
                CompositeEdgeFilter::Edge(Filter::eq("to", "1")),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }
    }

    #[cfg(test)]
    mod search_edges_count {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::property_filter::{CompositeEdgeFilter, Filter},
            },
            prelude::{AdditionOps, Graph, PropertyFilter},
        };

        fn search_count_edges_by_composite_filter(filter: &CompositeEdgeFilter) -> usize {
            let graph = Graph::new();
            graph
                .add_edge(1, 1, 2, [("p1", "shivam_kapoor")], Some("fire_nation"))
                .unwrap();
            graph
                .add_edge(
                    2,
                    2,
                    3,
                    [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_edge(3, 3, 1, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
                .unwrap();

            let results = graph
                .search_edges_count(filter)
                .expect("Failed to search for nodes");

            results
        }

        #[test]
        fn test_search_count_edges_by_composite_filter() {
            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", 3u64)),
            ]);
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 0);

            let filter = CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", "shivam")),
            ]);
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);

            let filter = CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", "pometry")),
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 6u64)),
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p3", 1u64)),
                ]),
            ]);
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::eq("from", "13")),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p1", "prop1")),
            ]);
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 0);
        }

        #[test]
        fn search_count_edges_for_src_from_eq() {
            let filter = CompositeEdgeFilter::Edge(Filter::eq("from", "2"));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_edges_for_src_to_ne() {
            let filter = CompositeEdgeFilter::Edge(Filter::ne("to", "2"));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_to_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::any("to", vec!["2".into()]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter = CompositeEdgeFilter::Edge(Filter::any("to", vec!["2".into(), "3".into()]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_to_not_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::not_any("to", vec!["1".into()]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_from_eq() {
            let filter = CompositeEdgeFilter::Edge(Filter::eq("from", "3"));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_edges_for_from_ne() {
            let filter = CompositeEdgeFilter::Edge(Filter::ne("from", "1"));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_from_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::any("from", vec!["1".into()]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter =
                CompositeEdgeFilter::Edge(Filter::any("from", vec!["1".into(), "2".into()]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_from_not_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::not_any("from", vec!["1".into()]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_property_eq() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_edges_for_property_ne() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::ne("p2", 2u64));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_edges_for_property_lt() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::lt("p2", 10u64));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_property_le() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::le("p2", 6u64));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_property_gt() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::gt("p2", 2u64));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_edges_for_property_ge() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::ge("p2", 2u64));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_property_in() {
            let filter =
                CompositeEdgeFilter::Property(PropertyFilter::any("p2", vec![Prop::U64(6)]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);

            let filter = CompositeEdgeFilter::Property(PropertyFilter::any(
                "p2",
                vec![Prop::U64(2), Prop::U64(6)],
            ));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        #[test]
        fn search_count_edges_for_property_not_in() {
            let filter =
                CompositeEdgeFilter::Property(PropertyFilter::not_any("p2", vec![Prop::U64(6)]));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 1);
        }

        #[test]
        fn search_count_edges_for_property_is_some() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::is_some("p2"));
            let results = search_count_edges_by_composite_filter(&filter);
            assert_eq!(results, 2);
        }

        // #[test]
        // fn search_count_edges_for_property_is_none() {
        //     let filter = CompositeNodeFilter::Property(PropertyFilter::is_none("p2"));
        //     let results = search_count_edges_by_composite_filter(&filter);
        //     assert_eq!(results, 0);
        // }
    }

    // #[test]
    // fn test_node_update_index() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(
    //             0,
    //             1,
    //             [("t_prop1", 1u64), ("t_prop2", 2u64)],
    //             Some("fire_nation"),
    //         )
    //         .unwrap();
    //     graph
    //         .add_node(1, 1, [("t_prop1", 5u64)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(0, 2, [("t_prop1", 2u64)], Some("air_nomads"))
    //         .unwrap();
    //     graph
    //         .add_node(0, 3, [("t_prop1", 3u64)], Some("water_tribe"))
    //         .unwrap();
    //     graph
    //         .add_node(0, 4, [("t_prop1", 4u64)], Some("earth_kingdom"))
    //         .unwrap();
    //     graph
    //         .node(1)
    //         .unwrap()
    //         .add_constant_properties([("c_prop1", Prop::Bool(true))])
    //         .unwrap();
    //
    //     // Create index from graph
    //     let _ = graph.searcher().unwrap();
    //
    //     // Delayed graph node update
    //     graph
    //         .add_node(0, 1, [("t_prop3", 3u64)], Some("fire_nation"))
    //         .unwrap();
    //     //
    //     // let query = AllQuery;
    //     //
    //     // let searcher = graph.searcher().unwrap().index.node_reader.searcher();
    //     // let top_docs = searcher
    //     //     .search(&AllQuery, &TopDocs::with_limit(100))
    //     //     .unwrap();
    //     //
    //     // println!("Total doc count: {}", top_docs.len());
    //     //
    //     // for (_score, doc_address) in top_docs {
    //     //     let doc: TantivyDocument = searcher.doc(doc_address).unwrap();
    //     //     println!("Document: {:?}", doc.to_json(searcher.schema()));
    //     // }
    //
    //     let filter = CompositeNodeFilter::And(vec![
    //         CompositeNodeFilter::Property(PropertyFilter::eq("t_prop1", 1u64)),
    //         CompositeNodeFilter::Property(PropertyFilter::eq("t_prop1", 1u64)),
    //     ]);
    //
    //     let mut results = graph
    //         .search_nodes(&filter, 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    //
    //     let filter = CompositeNodeFilter::And(vec![
    //         CompositeNodeFilter::Property(PropertyFilter::eq("t_prop1", 5u64)),
    //         CompositeNodeFilter::Property(PropertyFilter::eq("c_prop1", true)),
    //     ]);
    //
    //     let mut results = graph
    //         .search_nodes(&filter, 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    // }

    #[test]
    #[cfg(feature = "proto")]
    #[ignore = "this test is for experiments with the jira graph"]
    fn load_jira_graph() -> Result<(), GraphError> {
        global_info_logger();
        let graph = Graph::decode("/tmp/graphs/jira").expect("failed to load graph");
        assert!(graph.count_nodes() > 0);

        let now = SystemTime::now();

        let elapsed = now.elapsed().unwrap().as_secs();
        info!("indexing took: {:?}", elapsed);

        let filter = CompositeNodeFilter::Node(Filter::eq("name", "DEV-1690"));
        let issues = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, &filter, 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        info!("names: {:?}", names);

        Ok(())
    }

    // #[test]
    // fn test_search_windowed_graph() {
    //     let graph = Graph::new();
    //     for t in 0..10 {
    //         graph.add_node(t, 1, [("prop", t)], None).unwrap();
    //     }
    //     let wg = graph.window(8, 12);
    //     let results = wg
    //         .search_nodes("prop:1", 5, 0)
    //         .expect("Failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "no results" as for window 8-12, node 1 has props 8-9
    // }
    //
    // #[test]
    // fn test_search_windowed_persistent_graph() {
    //     let graph = PersistentGraph::new();
    //     for t in 0..10 {
    //         graph.add_node(t, 1, [("test", t)], None).unwrap();
    //     }
    //     let wg = graph.window(8, 12);
    //     let results = wg
    //         .search_nodes("test:1", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "no results" as for window 8-12, node 1 has props 8-9
    //
    //     let wg = graph.window(10, 12);
    //     let results = wg
    //         .search_nodes("test:1", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "no results" as for window 10-12, node 1 has prop 9 as last prop update
    //
    //     let wg = graph.window(10, 12);
    //     let results = wg
    //         .search_nodes("test:9", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "node 1" as for window 10-12, node 9 has props 9 as last prop update
    //
    //     // Edge case:
    //     // let graph = PersistentGraph::new();
    //     // graph.add_node(0, 1, [("test1", 0)], None).unwrap(); // Creates doc which has both props
    //     // graph.add_node(0, 1, [("test2", 0)], None).unwrap(); // Creates doc which has both props
    //     // graph.add_node(2, 1, [("test2", 1)], None).unwrap(); // Creates doc which has only test2 prop
    //
    //     // Edge case:
    //     let graph = PersistentGraph::new();
    //     graph
    //         .add_node(0, 1, [("test1", 0), ("test2", 0)], None)
    //         .unwrap(); // Creates doc which has both props
    //     graph.add_node(2, 1, [("test2", 1)], None).unwrap(); // Creates doc which has only test2 prop
    //
    //     // Searching for test 2 in window 2-10 should return "no results"
    //     let wg = graph.window(2, 10);
    //     let results = wg
    //         .search_nodes("test2:0", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     // Searching for test 2 in window 2-10 should return "node 1"
    //     let wg = graph.window(2, 10);
    //     let results = wg
    //         .search_nodes("test2:1", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     // Searching for test 1 in window 2-10 should return "node 1"
    //     let wg = graph.window(2, 10);
    //     let results = wg
    //         .search_nodes("test1:0", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "node 1" as for window 10-12, node 9 has props 9 as last prop update
    // }
    //
    //
    // #[test]
    // fn test_search_nodes_by_props_added_at_different_times_range() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(1, 1, [("t_prop1", 1)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(3, 1, [("t_prop3", 3)], Some("fire_nation"))
    //         .unwrap();
    //
    //     let mut results = graph
    //         .search_nodes("t_prop1:1 AND t_prop3:3 AND time:[3 TO 3]", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    // }
    //
    // #[test]
    // fn test_search_nodes_range() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(2, 1, [("t_prop1", 1)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(4, 2, [("t_prop1", 1)], Some("air_nomads"))
    //         .unwrap();
    //     graph
    //         .add_node(6, 3, [("t_prop3", 3)], Some("fire_nation"))
    //         .unwrap();
    //
    //     let mut results = graph
    //         .search_nodes("node_type:fire_nation AND time:[1 TO 6]", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    // }
    //
    // // Discuss with lucas
    // #[test]
    // fn test1() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(0, 1, [("t_prop1", 1), ("t_prop2", 2)], Some("fire_nation"))
    //         .unwrap(); // doc 0
    //     graph
    //         .add_node(1, 1, [("t_prop1", 1), ("t_prop2", 2)], Some("fire_nation"))
    //         .unwrap(); // doc 1
    //     graph
    //         .add_node(
    //             2,
    //             2,
    //             [("t_prop1", 11), ("t_prop2", 12)],
    //             Some("fire_nation"),
    //         )
    //         .unwrap(); // doc 2
    //     graph
    //         .add_node(3, 3, [("t_prop1", 1), ("t_prop2", 2)], Some("water_tribe"))
    //         .unwrap(); // doc 3
    //     graph
    //         .add_node(
    //             4,
    //             4,
    //             [("t_prop1", 31), ("t_prop2", 32)],
    //             Some("fire_nation"),
    //         )
    //         .unwrap(); // doc 4
    //
    //     let mut results = graph
    //         .window(2, 5)
    //         .search_nodes("t_prop1:1 AND t_prop2:2", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["3"]);
    // }
    //
    // #[test]
    // fn test2() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(2, 2, [("p4", 5)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"))
    //         .unwrap();
    //
    //     let graph = graph.window(2, 5);
    //     let mut results = graph
    //         .searcher()
    //         .unwrap()
    //         .event_graph_search_nodes(&graph, "p2:4 AND p3:3", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     // assert_eq!(results, vec!["3"]);
    // }
    //
    //
    // #[test]
    // fn add_node_search_by_description_and_time() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(
    //             1,
    //             "Gandalf",
    //             [("description", Prop::str("The wizard"))],
    //             None,
    //         )
    //         .expect("add node failed");
    //     graph
    //         .add_node(
    //             2,
    //             "Saruman",
    //             [("description", Prop::str("Another wizard"))],
    //             None,
    //         )
    //         .expect("add node failed");
    //
    //     // Find Saruman
    //     let nodes = graph
    //         .searcher()
    //         .unwrap()
    //         .search_nodes(&graph, r#"description:wizard AND time:[2 TO 5]"#, 10, 0)
    //         .expect("search failed");
    //     let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
    //     let expected = vec!["Saruman"];
    //     assert_eq!(actual, expected);
    //
    //     // Find Gandalf
    //     let nodes = graph
    //         .searcher()
    //         .unwrap()
    //         .search_nodes(&graph, r#"description:'wizard' AND time:[1 TO 2}"#, 10, 0)
    //         .expect("search failed");
    //     let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
    //     let expected = vec!["Gandalf"];
    //     assert_eq!(actual, expected);
    //
    //     // Find both wizards
    //     let nodes = graph
    //         .searcher()
    //         .unwrap()
    //         .search_nodes(&graph, r#"description:'wizard' AND time:[1 TO 100]"#, 10, 0)
    //         .expect("search failed");
    //     let mut actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
    //     let mut expected = vec!["Gandalf", "Saruman"];
    //
    //     // FIXME: this is not deterministic
    //     actual.sort();
    //     expected.sort();
    //
    //     assert_eq!(actual, expected);
    // }
}
