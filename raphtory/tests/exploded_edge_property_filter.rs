use itertools::Itertools;
use proptest::{arbitrary::any, proptest};
use raphtory::{
    db::{
        api::view::{Filter, StaticGraphViewOps},
        graph::{
            assertions::assert_ok_or_missing_edges,
            edge::EdgeView,
            graph::{
                assert_graph_equal, assert_node_equal, assert_nodes_equal,
                assert_persistent_materialize_graph_equal,
            },
            views::{
                deletion_graph::PersistentGraph,
                filter::model::{
                    property_filter::ops::PropertyFilterOps, ExplodedEdgeFilter,
                    PropertyFilterFactory,
                },
            },
        },
    },
    prelude::*,
    test_utils::{
        build_edge_deletions, build_edge_list, build_edge_list_with_deletions,
        build_graph_from_edge_list, build_window, Update,
    },
};
use raphtory_api::core::{
    entities::properties::prop::PropType,
    storage::{arc_str::ArcStr, timeindex::AsTime},
};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps},
};
use std::collections::HashMap;

fn build_filtered_graph(
    edges: &[(u64, u64, i64, String, i64)],
    filter: impl Fn(i64) -> bool,
) -> Graph {
    let g = Graph::new();
    for (index, (src, dst, t, str_prop, int_prop)) in edges.iter().enumerate() {
        if filter(*int_prop) {
            g.add_edge(
                (*t, index),
                *src,
                *dst,
                [
                    ("str_prop", Prop::str(str_prop.as_ref())),
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

fn build_filtered_nodes_graph(
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
                    ("str_prop", str_prop.as_str().into()),
                    ("int_prop", Prop::I64(*int_prop)),
                ],
                None,
            )
            .unwrap();
        }
        g.atomic_add_node(src.as_node_ref()).unwrap();
        g.atomic_add_node(dst.as_node_ref()).unwrap();
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
    for ((src, dst), updates) in edges {
        for (t, update) in updates {
            match update {
                Update::Deletion => {
                    g.delete_edge(t, src, dst, None).unwrap();
                    g_filtered.delete_edge(t, src, dst, None).unwrap();
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
                    if filter(int_prop) {
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
                    } else {
                        g_filtered.delete_edge(t, src, dst, None).unwrap();
                        // properties still exist after filtering
                        let session = g_filtered.write_session().unwrap();
                        session
                            .resolve_edge_property("str_prop", PropType::Str, false)
                            .unwrap();
                        session
                            .resolve_edge_property("int_prop", PropType::I64, false)
                            .unwrap();
                    }
                }
            }
        }
    }
    (g, g_filtered)
}

fn edge_attr<G: StaticGraphViewOps>(
    e: &EdgeView<G>,
) -> (String, String, Option<i64>, Option<ArcStr>) {
    let src = e.src().name();
    let dst = e.dst().name();
    let int_prop = e.properties().get("int_prop");
    let str_prop = e.properties().get("str_prop");
    (src, dst, int_prop.into_i64(), str_prop.into_str())
}

#[test]
fn test_filter_gt() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").gt(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
            #[cfg(feature = "search")]
            {
                let search_ee = g.search_exploded_edges(filter, 100, 0).unwrap();
                let filter_ee = filtered.edges().explode().collect();
                let from_search = search_ee.iter().map(edge_attr).collect_vec();
                let from_filter = filter_ee.iter().map(edge_attr).collect_vec();
                assert_eq!(from_search, from_filter);
            }
        });
    })
}

#[test]
fn test_filter_gt_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv > v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").gt(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
fn test_one_edge() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
    let filtered = g
        .filter(ExplodedEdgeFilter.property("int_prop").gt(1i64))
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
        let filter = ExplodedEdgeFilter.property("int_prop").ge(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv >= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        });
    })
}

#[test]
fn test_filter_ge_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv >= v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        if p.is_some() {
        let filtered = g.filter(
            ExplodedEdgeFilter.property("int_prop").ge(v)
        ).unwrap();
        assert_graph_equal(&filtered, &expected_filtered_g);
        }

    })
}

#[test]
fn test_filter_persistent_single_filtered_edge() {
    let g = PersistentGraph::new();
    g.add_edge(0, 0, 0, [("test", 1i64)], None).unwrap();
    let gf = g
        .filter(ExplodedEdgeFilter.property("test").gt(1i64))
        .unwrap();

    assert_eq!(gf.count_edges(), 1);
    assert_eq!(gf.count_temporal_edges(), 0);

    let expected = PersistentGraph::new();
    expected.delete_edge(0, 0, 0, None).unwrap();
    //the property still exists!
    expected
        .write_session()
        .unwrap()
        .resolve_edge_property("test", PropType::I64, false)
        .unwrap();

    assert_graph_equal(&gf, &expected);
}

#[test]
fn test_filter_lt() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").lt(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv < v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        });
    })
}

#[test]
fn test_filter_lt_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv < v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").lt(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
        assert_graph_equal(&filtered, &expected_filtered_g);
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
fn test_filter_le() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").le(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv <= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        });
    })
}

#[test]
fn test_filter_le_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv <= v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").le(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
        assert_graph_equal(&filtered, &expected_filtered_g);
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
fn test_filter_eq() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").eq(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        });
    })
}

#[test]
fn test_filter_eq_one_edge() {
    let g = Graph::new();
    g.add_edge(0, 0, 0, [("int_prop", Prop::I64(0))], None)
        .unwrap();
    let filter = ExplodedEdgeFilter.property("int_prop").eq(0i64);
    assert_graph_equal(&g.filter(filter.clone()).unwrap(), &g);
}

#[test]
fn test_filter_eq_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv == v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").eq(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
        assert_graph_equal(&filtered, &expected_filtered_g);
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
fn test_filter_ne() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").ne(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        });
    })
}

#[test]
fn test_filter_ne_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv != v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").ne(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
        assert_graph_equal(&filtered, &expected_filtered_g);
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
fn test_filter_window() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").eq(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        });
    })
}

#[test]
fn test_filter_window_persistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>(), (start, end) in build_window()
    )| {
        let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv >= v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").ge(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
        assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
fn test_filter_materialise_is_consistent() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").eq(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let mat = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &mat);
        });
    })
}

#[test]
fn test_filter_persistent_materialise_is_consistent() {
    proptest!(|(
        edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
    )| {
        let (g, expected) = build_filtered_persistent_graph(edges, |vv| vv >= v);
        let p = g.edge_meta().temporal_prop_mapper().get_id("int_prop");
        let filtered = g.filter(
                ExplodedEdgeFilter.property("int_prop").ge(v)
            );
        if p.is_some() {
            let filtered = filtered.unwrap();
        let mat = filtered.materialize().unwrap();
        assert_graph_equal(&filtered, &mat);
        assert_graph_equal(&expected, &mat);
        } else {
            assert!(filtered.is_err())
        }
    })
}

#[test]
#[ignore = "need a way to add a node without timestamp"]
fn test_filter_on_nodes() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = ExplodedEdgeFilter.property("int_prop").eq(v);
        assert_ok_or_missing_edges(&edges, g.nodes().filter(filter.clone()), |filtered| {
            let expected_filtered_g = build_filtered_nodes_graph(&edges, |vv| vv == v);
            assert_nodes_equal(&filtered, &expected_filtered_g.nodes());
        });
    })
}

#[test]
#[ignore = "need a way to add a node without timestamp"]
fn test_filter_on_nodes_simple() {
    let edges = [(1u64, 2u64, 0i64, "a".to_string(), 10i64)];
    let v = -1;
    let g = build_graph_from_edge_list(&edges);
    let filter = ExplodedEdgeFilter.property("int_prop").eq(v);
    assert_ok_or_missing_edges(&edges, g.nodes().filter(filter.clone()), |filtered| {
        let expected_filtered_g = build_filtered_nodes_graph(&edges, |vv| vv == v);
        assert_nodes_equal(&filtered, &expected_filtered_g.nodes());
    });
}

#[test]
fn test_filter_on_node() {
    proptest!(|(
        edges in build_edge_list(100, 2), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        if let Some(node) = g.node(0) {
            let filtered_node = node.filter(
                ExplodedEdgeFilter.property("int_prop").eq(v)
            ).unwrap();
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
        let filter = ExplodedEdgeFilter.property("int_prop").eq(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let left = filtered.window(start, end);
            let right = filtered.window(start, end).materialize().unwrap();
            assert_graph_equal(&left, &right);
        });
    })
}

#[test]
fn test_persistent_graph() {
    let g = PersistentGraph::new();
    g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
    g.delete_edge(2, 1, 2, None).unwrap();
    g.add_edge(5, 1, 2, [("int_prop", 5i64)], None).unwrap();
    g.delete_edge(7, 1, 2, None).unwrap();

    let edges = g
        .node(1)
        .unwrap()
        .filter(ExplodedEdgeFilter.property("int_prop").gt(1i64))
        .unwrap()
        .edges()
        .explode()
        .collect();
    println!("{:?}", edges);

    assert_eq!(edges.len(), 1);
    let gf = g
        .filter(ExplodedEdgeFilter.property("int_prop").gt(1i64))
        .unwrap();
    let gfm = gf.materialize().unwrap();

    assert_graph_equal(&gf, &gfm); // check materialise is consistent
}

#[test]
fn test_persistent_graph_explode_semantics() {
    let g = PersistentGraph::new();
    g.add_edge(0, 0, 1, [("test", 0i64)], None).unwrap();
    g.add_edge(2, 0, 1, [("test", 2i64)], None).unwrap();
    g.add_edge(5, 0, 1, [("test", 5i64)], None).unwrap();
    g.delete_edge(10, 0, 1, None).unwrap();

    let gf = g
        .filter(ExplodedEdgeFilter.property("test").ne(2i64))
        .unwrap();
    assert_eq!(
        gf.edges()
            .explode()
            .earliest_time()
            .map(|t_opt| t_opt.map(|t| t.t()))
            .collect_vec(),
        [Some(0i64), Some(5i64)]
    );
    assert_eq!(
        gf.edges()
            .explode()
            .latest_time()
            .map(|t_opt| t_opt.map(|t| t.t()))
            .collect_vec(),
        [Some(2i64), Some(10i64)]
    );
}

#[test]
fn test_persistent_graph_materialise() {
    proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>())| {
        let g = build_graph_from_edge_list(&edges);
        let g = g.persistent_graph();
        for (src, dst, t) in edge_deletions {
            g.delete_edge(t, src, dst, None).unwrap();
        }
        let filter = ExplodedEdgeFilter.property("int_prop").gt(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let gfm = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &gfm)
        });
    })
}

#[test]
fn test_single_filtered_edge_persistent() {
    let g = PersistentGraph::new();
    g.add_edge(0, 0, 0, [("test", 0i64)], None).unwrap();
    let gf = g
        .filter(ExplodedEdgeFilter.property("test").gt(0i64))
        .unwrap();
    let gfm = gf.materialize().unwrap();
    dbg!(&gfm);
    assert_eq!(gf.node(0).unwrap().out_degree(), 1);
    assert_eq!(gfm.node(0).unwrap().out_degree(), 1);
    assert_graph_equal(&gf, &gfm);
}

#[test]
fn test_persistent_graph_materialise_window() {
    proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>(), (start, end) in build_window())| {
        let g = build_graph_from_edge_list(&edges);
        let g = g.persistent_graph();
        for (src, dst, t) in edge_deletions {
            g.delete_edge(t, src, dst, None).unwrap();
        }
        let filter = ExplodedEdgeFilter.property("int_prop").gt(v);
        assert_ok_or_missing_edges(&edges, g.window(start, end).filter(filter.clone()), |filtered| {
            let gwfm = filtered.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&filtered, &gwfm);
        });
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let gfw = filtered.window(start, end);
            let gfwm = gfw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gfw, &gfwm);
        });
    })
}

#[test]
fn test_persistent_failure() {
    let g = PersistentGraph::new();
    g.add_edge(-1, 0, 1, [("test", Prop::I32(-1))], None)
        .unwrap();
    g.add_edge(0, 0, 1, [("test", Prop::I32(1))], None).unwrap();

    let gwf = g
        .filter(ExplodedEdgeFilter.property("test").gt(0))
        .unwrap()
        .window(-1, 0);
    assert_eq!(gwf.count_nodes(), 0);
    assert_eq!(gwf.count_edges(), 0);
    let gm = gwf.materialize().unwrap();

    assert_persistent_materialize_graph_equal(&gwf, &gm);

    let gfw = g
        .window(-1, 0)
        .filter(ExplodedEdgeFilter.property("test").gt(0))
        .unwrap();
    assert_eq!(gfw.count_edges(), 0);
    let gm = gfw.materialize().unwrap();
    assert_persistent_materialize_graph_equal(&gfw, &gm);
}
