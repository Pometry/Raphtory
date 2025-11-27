use super::GraphStorage;
use crate::{
    core::{storage::timeindex::TimeIndexOps, utils::iter::GenLockedDIter},
    db::api::view::internal::{GraphTimeSemanticsOps, TimeSemantics},
    prelude::Prop,
};
use raphtory_api::{
    core::{
        entities::properties::tprop::TPropOps,
        storage::timeindex::{AsTime, TimeIndexEntry},
    },
    iter::{BoxedLDIter, IntoDynDBoxed},
};
use raphtory_storage::graph::nodes::node_storage_ops::NodeStorageOps;
use rayon::iter::ParallelIterator;
use std::ops::{Deref, Range};

impl GraphTimeSemanticsOps for GraphStorage {
    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    fn view_start(&self) -> Option<i64> {
        None
    }

    fn view_end(&self) -> Option<i64> {
        None
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_earliest_time(),
            GraphStorage::Unlocked(storage) => storage.graph_earliest_time(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.earliest(),
        }
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_latest_time(),
            GraphStorage::Unlocked(storage) => storage.graph_latest_time(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.latest(),
        }
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map(|node| node.additions().range_t(start..end).first_t())
            .min()
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map(|node| node.additions().range_t(start..end).last_t())
            .max()
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        prop_id < self.graph_meta().temporal_mapper().len()
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<'_, (TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .into_iter()
            .flat_map(move |prop| {
                GenLockedDIter::from(prop, |prop| prop.deref().iter().into_dyn_dboxed())
            })
            .into_dyn_dboxed()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .filter(|p| p.deref().iter_window_t(w).next().is_some())
            .is_some()
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<'_, (TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .into_iter()
            .flat_map(move |prop| {
                GenLockedDIter::from(prop, |prop| {
                    prop.deref()
                        .iter_window(TimeIndexEntry::range(start..end))
                        .into_dyn_dboxed()
                })
            })
            .into_dyn_dboxed()
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .and_then(|p| p.deref().last_before(t.next()))
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let w = TimeIndexEntry::range(w);
        if w.contains(&t) {
            self.graph_meta().get_temporal_prop(prop_id).and_then(|p| {
                p.deref()
                    .last_before(t.next())
                    .filter(|(t, _)| w.contains(t))
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test_graph_storage {
    use crate::{db::api::view::StaticGraphViewOps, prelude::AdditionOps};
    use raphtory_api::core::entities::properties::prop::Prop;

    fn init_graph_for_nodes_tests<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let nodes = vec![
            (6, "N1", vec![("p1", Prop::U64(2u64))]),
            (7, "N1", vec![("p1", Prop::U64(1u64))]),
            (6, "N2", vec![("p1", Prop::U64(1u64))]),
            (7, "N2", vec![("p1", Prop::U64(2u64))]),
            (8, "N3", vec![("p1", Prop::U64(1u64))]),
            (9, "N4", vec![("p1", Prop::U64(1u64))]),
            (5, "N5", vec![("p1", Prop::U64(1u64))]),
            (6, "N5", vec![("p1", Prop::U64(2u64))]),
            (5, "N6", vec![("p1", Prop::U64(1u64))]),
            (6, "N6", vec![("p1", Prop::U64(1u64))]),
            (3, "N7", vec![("p1", Prop::U64(1u64))]),
            (5, "N7", vec![("p1", Prop::U64(1u64))]),
            (3, "N8", vec![("p1", Prop::U64(1u64))]),
            (4, "N8", vec![("p1", Prop::U64(2u64))]),
        ];

        for (id, name, props) in nodes {
            graph.add_node(id, name, props, None).unwrap();
        }

        graph
    }

    fn init_graph_for_edges_tests<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edges = vec![
            (6, "N1", "N2", vec![("p1", Prop::U64(2u64))], Some("layer1")),
            (7, "N1", "N2", vec![("p1", Prop::U64(1u64))], Some("layer2")),
            (6, "N2", "N3", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (7, "N2", "N3", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            (8, "N3", "N4", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (9, "N4", "N5", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (5, "N5", "N6", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (6, "N5", "N6", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            (5, "N6", "N7", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (6, "N6", "N7", vec![("p1", Prop::U64(1u64))], Some("layer2")),
            (3, "N7", "N8", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (5, "N7", "N8", vec![("p1", Prop::U64(1u64))], Some("layer2")),
            (3, "N8", "N1", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (4, "N8", "N1", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            // Tests secondary indexes
            (3, "N9", "N2", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (3, "N9", "N2", vec![("p1", Prop::U64(2u64))], Some("layer2")),
        ];

        for (id, src, dst, props, layer) in edges {
            graph.add_edge(id, src, dst, props, layer).unwrap();
        }

        graph
    }

    #[cfg(all(test, feature = "search"))]
    mod search_nodes {
        use super::*;
        use crate::{
            db::{
                api::view::SearchableGraphOps,
                graph::views::filter::model::{
                    node_filter::NodeFilter, property_filter::PropertyFilterOps,
                    PropertyFilterFactory,
                },
            },
            prelude::{Graph, IndexMutationOps, NodeViewOps},
        };

        #[test]
        fn test_search_nodes_latest() {
            let g = Graph::new();
            let g = init_graph_for_nodes_tests(g);
            g.create_index().unwrap();
            let filter = NodeFilter.property("p1").eq(1u64);
            let mut results = g
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();

            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);
        }
    }

    #[cfg(all(test, feature = "search"))]
    mod search_edges {
        use super::*;
        use crate::{
            db::{
                api::view::SearchableGraphOps,
                graph::views::filter::model::{
                    edge_filter::EdgeFilter, property_filter::PropertyFilterOps,
                    PropertyFilterFactory,
                },
            },
            prelude::{EdgeViewOps, Graph, IndexMutationOps, NodeViewOps},
        };

        #[test]
        fn test_search_edges_latest() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);
            g.create_index().unwrap();
            let filter = EdgeFilter.property("p1").eq(1u64);
            let mut results = g
                .search_edges(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<Vec<_>>();
            results.sort();

            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );
        }
    }
}
