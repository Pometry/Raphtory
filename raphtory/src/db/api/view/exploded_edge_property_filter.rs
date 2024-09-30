use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::{GraphType, InternalMaterialize, OneHopFilter},
        graph::views::property_filter::internal::InternalExplodedEdgeFilterOps,
    },
    prelude::GraphViewOps,
};

pub trait ExplodedEdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_exploded_edges<F: InternalExplodedEdgeFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::ExplodedEdgeFiltered<'graph, Self::FilteredGraph>>, GraphError>
    {
        if matches!(
            self.current_filter().graph_type(),
            GraphType::PersistentGraph
        ) {
            return Err(GraphError::PropertyFilteringNotImplemented);
        }
        let graph = filter.create_exploded_edge_filter(self.current_filter().clone())?;
        Ok(self.one_hop_filtered(graph))
    }
}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilterOps<'graph> for G {}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::view::{
                exploded_edge_property_filter::ExplodedEdgePropertyFilterOps, node::NodeViewOps,
            },
            graph::{
                graph::{assert_graph_equal, assert_node_equal, assert_nodes_equal},
                views::deletion_graph::PersistentGraph,
            },
        },
        prelude::*,
        test_utils::{
            build_edge_deletions, build_edge_list, build_graph_from_edge_list, build_window,
        },
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use std::ops::Range;

    fn build_filtered_graph(
        edges: &[(u64, u64, i64, String, i64)],
        filter: impl Fn(i64) -> bool,
    ) -> Graph {
        let g = Graph::new();
        for (src, dst, t, str_prop, int_prop) in edges {
            g.add_node(*t, *src, NO_PROPS, None).unwrap();
            g.add_node(*t, *dst, NO_PROPS, None).unwrap();
            if filter(*int_prop) {
                g.add_edge(
                    *t,
                    *src,
                    *dst,
                    [
                        ("str_prop", str_prop.into()),
                        ("int_prop", Prop::I64(*int_prop)),
                    ],
                    None,
                )
                .unwrap();
            }
        }
        g
    }

    #[test]
    fn test_filter_gt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::gt("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_one_edge() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        let filtered = g
            .filter_exploded_edges(PropertyFilter::gt("int_prop", 1i64))
            .unwrap();
        let edges = filtered
            .edges()
            .explode()
            .iter()
            .map(|e| (e.src().id(), e.dst().id()))
            .collect_vec();
        let gf = Graph::new();
        gf.add_node(0, 1, NO_PROPS, None).unwrap();
        gf.add_node(0, 2, NO_PROPS, None).unwrap();

        println!("{:?}", edges);
        assert_graph_equal(&filtered, &gf);
    }

    #[test]
    fn test_filter_ge() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::ge("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv >= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_lt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges( PropertyFilter::lt("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv < v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_le() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::le("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv <= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_eq() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ne() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges( PropertyFilter::ne("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_window() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        })
    }

    #[test]
    fn test_filter_materialise_is_consistent() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
            assert_graph_equal(&filtered, &filtered.materialize().unwrap());
        })
    }

    #[test]
    fn test_filter_on_nodes() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered_nodes = g.nodes().filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_nodes_equal(&filtered_nodes, &expected_filtered_g.nodes());
        })
    }

    #[test]
    fn test_filter_on_node() {
        proptest!(|(
            edges in build_edge_list(100, 2), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            if let Some(node) = g.node(0) {
                let filtered_node = node.filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
                let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
                assert_node_equal(filtered_node, expected_filtered_g.node(0).unwrap())
            }
        })
    }

    #[test]
    fn test_filter_materialise_window_is_consistent() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
            assert_graph_equal(&filtered.window(start, end), &filtered.window(start, end).materialize().unwrap());
        })
    }

    // FIXME: Semantics for materialised graph and filtering are not implemented properly, disabled for now
    //
    // #[test]
    // fn test_persistent_graph() {
    //     let g = PersistentGraph::new();
    //     g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
    //     g.delete_edge(2, 1, 2, None).unwrap();
    //     g.add_edge(5, 1, 2, [("int_prop", 5i64)], None).unwrap();
    //     g.delete_edge(7, 1, 2, None).unwrap();
    //
    //     let edges = g
    //         .node(1)
    //         .unwrap()
    //         .filter_exploded_edges(PropertyFilter::gt("int_prop", 1i64))
    //         .unwrap()
    //         .edges()
    //         .explode()
    //         .collect();
    //     println!("{:?}", edges);
    //
    //     assert_eq!(edges.len(), 1);
    //     let gf = g
    //         .filter_exploded_edges(PropertyFilter::gt("int_prop", 1i64))
    //         .unwrap();
    //     let gfm = gf.materialize().unwrap();
    //
    //     assert_graph_equal(&gf, &gfm); // check materialise is consistent
    // }
    //
    // #[test]
    // fn test_persistent_graph_materialise() {
    //     proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>())| {
    //         let g = build_graph_from_edge_list(&edges);
    //         let g = g.persistent_graph();
    //         for (src, dst, t) in edge_deletions {
    //             g.delete_edge(t, src, dst, None).unwrap();
    //         }
    //         let gf = g
    //             .filter_exploded_edges(PropertyFilter::gt("int_prop", v))
    //             .unwrap();
    //         let gfm = gf.materialize().unwrap();
    //         assert_graph_equal(&gf, &gfm)
    //     })
    // }
    //
    // #[test]
    // fn test_persistent_graph_materialise_window() {
    //     proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>(), (start, end) in build_window())| {
    //         let g = build_graph_from_edge_list(&edges);
    //         let g = g.persistent_graph();
    //         for (src, dst, t) in edge_deletions {
    //             if let Some(earliest) = g.edge(src, dst).and_then(|e| e.earliest_time()) {
    //                 // FIXME: many known bugs with the windowing semantics for edges that are deleted first so don't make them for now
    //                 if earliest <= t {
    //                     g.delete_edge(t, src, dst, None).unwrap();
    //                 }
    //             }
    //
    //         }
    //         let gwf = g.window(start, end)
    //             .filter_exploded_edges(PropertyFilter::gt("int_prop", v))
    //             .unwrap();
    //
    //
    //         let gwfm = gwf.materialize().unwrap();
    //         assert_graph_equal(&gwf, &gwfm);
    //
    //         let gfw = g
    //             .filter_exploded_edges(PropertyFilter::gt("int_prop", v)).unwrap()
    //             .window(start, end);
    //         let gfwm = gfw.materialize().unwrap();
    //         assert_graph_equal(&gfw, &gfwm);
    //     })
    // }
    //
    // #[test]
    // fn test_persistent_graph_only_deletion() {
    //     let g = PersistentGraph::new();
    //     g.delete_edge(0, 0, 0, None).unwrap();
    //     let gfw = g
    //         .filter_exploded_edges(PropertyFilter::gt("int_prop", 1i64))
    //         .unwrap()
    //         .window(-1, 1);
    //     println!(
    //         "earliest: {:?}, latest: {:?}",
    //         gfw.earliest_time(),
    //         gfw.latest_time()
    //     );
    //     let gfwm = gfw.materialize().unwrap();
    //     println!(
    //         "earliest: {:?}, latest: {:?}",
    //         gfwm.earliest_time(),
    //         gfwm.latest_time()
    //     );
    //     assert!(gfw.node(0).is_some());
    //     assert!(gfwm.node(0).is_some());
    //     assert_eq!(gfw.earliest_time(), None);
    // }
}
