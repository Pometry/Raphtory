use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::OneHopFilter,
        graph::views::filter::internal::InternalExplodedEdgeFilterOps,
    },
    prelude::GraphViewOps,
};

pub trait ExplodedEdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_exploded_edges<F: InternalExplodedEdgeFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::ExplodedEdgeFiltered<'graph, Self::FilteredGraph>>, GraphError>
    {
        let graph = filter.create_exploded_edge_filter(self.current_filter().clone())?;
        Ok(self.one_hop_filtered(graph))
    }
}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilterOps<'graph> for G {}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::{
                mutation::internal::InternalAdditionOps,
                view::exploded_edge_property_filter::ExplodedEdgePropertyFilterOps,
            },
            graph::{
                graph::{
                    assert_edges_equal, assert_graph_equal, assert_node_equal, assert_nodes_equal,
                },
                views::{
                    deletion_graph::PersistentGraph, filter::model::property_filter::PropertyRef,
                },
            },
        },
        prelude::*,
        test_utils::{
            build_edge_list, build_edge_list_with_deletions, build_graph_from_edge_list,
            build_window, Update,
        },
    };
    use proptest::{arbitrary::any, proptest};
    use std::collections::HashMap;

    fn build_filtered_graph(
        edges: &[(u64, u64, i64, String, i64)],
        filter: impl Fn(i64) -> bool,
    ) -> Graph {
        let g = Graph::new();
        for (src, dst, t, str_prop, int_prop) in edges {
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
        if !edges.is_empty() {
            g.resolve_layer(None).unwrap();
        }
        g
    }

    fn build_filtered_persistent_graph(
        edges: HashMap<(u64, u64), Vec<(i64, Update)>>,
        filter: impl Fn(i64) -> bool,
    ) -> (PersistentGraph, PersistentGraph) {
        let g = PersistentGraph::new();
        let g_filtered = PersistentGraph::new();
        if !edges.iter().all(|(_, v)| v.is_empty()) {
            g_filtered.resolve_layer(None).unwrap();
        }
        for ((src, dst), mut updates) in edges {
            let mut keep = false;
            updates.sort();
            for (t, update) in updates {
                match update {
                    Update::Deletion => {
                        g.delete_edge(t, src, dst, None).unwrap();
                        if keep {
                            g_filtered.delete_edge(t, src, dst, None).unwrap();
                        }
                        keep = false;
                    }
                    Update::Addition(str_prop, int_prop) => {
                        g.add_edge(
                            t,
                            src,
                            dst,
                            [
                                ("str_prop", str_prop.clone().into()),
                                ("int_prop", Prop::I64(int_prop)),
                            ],
                            None,
                        )
                        .unwrap();
                        keep = filter(int_prop);
                        if keep {
                            g_filtered
                                .add_edge(
                                    t,
                                    src,
                                    dst,
                                    [
                                        ("str_prop", str_prop.into()),
                                        ("int_prop", Prop::I64(int_prop)),
                                    ],
                                    None,
                                )
                                .unwrap();
                        }
                    }
                }
            }
        }
        (g, g_filtered)
    }

    #[test]
    fn test_filter_gt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(
                PropertyFilter::gt(PropertyRef::Property("int_prop".to_string()), v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_gt_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv > v);
            let filtered = g.filter_exploded_edges(PropertyFilter::gt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_one_edge() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        let filtered = g
            .filter_exploded_edges(PropertyFilter::gt(
                PropertyRef::Property("int_prop".to_string()),
                1i64,
            ))
            .unwrap();
        let gf = Graph::new();
        gf.resolve_layer(None).unwrap();
        assert_eq!(filtered.count_nodes(), 0);
        assert_eq!(filtered.count_edges(), 0);
        assert_graph_equal(&filtered, &gf);
    }

    #[test]
    fn test_filter_ge() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv >= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ge_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv >= v);
            let filtered = g.filter_exploded_edges(PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_lt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges( PropertyFilter::lt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv < v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_lt_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv < v);
            let filtered = g.filter_exploded_edges(PropertyFilter::lt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_le() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::le(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv <= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_le_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv <= v);
            let filtered = g.filter_exploded_edges(PropertyFilter::le(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_eq() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_eq_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv == v);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ne() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges( PropertyFilter::ne(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ne_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv != v);
            let filtered = g.filter_exploded_edges(PropertyFilter::ne(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_window() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        })
    }

    #[test]
    fn test_filter_window_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv >= v);
            let filtered = g.filter_exploded_edges(PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        })
    }

    #[test]
    fn test_filter_materialise_is_consistent() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let mat = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &mat);
        })
    }

    #[test]
    fn test_filter_persistent_materialise_is_consistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected) = build_filtered_persistent_graph(edges, |vv| vv >= v);
            let filtered = g.filter_exploded_edges(PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let mat = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &mat);
            assert_graph_equal(&expected, &mat);
        })
    }

    #[test]
    fn test_filter_on_nodes() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered_nodes = g.nodes().filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
                let filtered_node = node.filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
                let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
                if filtered_node.degree() == 0 {
                    // should be filtered out
                    assert!(expected_filtered_g.node(0).is_none())
                } else {
                    assert_node_equal(filtered_node, expected_filtered_g.node(0).unwrap())
                }
            }
        })
    }

    #[test]
    fn test_filter_materialise_window_is_consistent() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let left = filtered.window(start, end);
            let right = filtered.window(start, end).materialize().unwrap();
            assert_edges_equal(&left.edges(), &right.edges());
            // FIXME filtered_exploded_edges doesn't propagate timestamps to nodes assert_graph_equal(&filtered, &filtered.materialize().unwrap());
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
