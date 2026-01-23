use itertools::Itertools;
use proptest::{arbitrary::any, proptest};
use raphtory::{
    db::{
        api::view::Filter,
        graph::{
            assertions::assert_ok_or_missing_edges,
            graph::{assert_graph_equal, assert_persistent_materialize_graph_equal},
            views::{
                deletion_graph::PersistentGraph,
                filter::model::{
                    node_filter::ops::NodeFilterOps, property_filter::ops::PropertyFilterOps,
                    ComposableFilter, EdgeFilter, PropertyFilterFactory,
                },
            },
        },
    },
    prelude::*,
    test_utils::{build_edge_deletions, build_edge_list, build_graph_from_edge_list, build_window},
};
use raphtory_api::core::{entities::properties::prop::PropType, storage::timeindex::AsTime};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};

#[test]
fn test_edge_filter() {
    let g = Graph::new();
    g.add_edge(0, "Jimi", "John", [("band", "JH Experience")], None)
        .unwrap();
    g.add_edge(1, "John", "David", [("band", "Dead & Company")], None)
        .unwrap();
    g.add_edge(2, "David", "Jimi", [("band", "Pink Floyd")], None)
        .unwrap();

    let filter_expr = EdgeFilter::dst()
        .name()
        .eq("David")
        .and(EdgeFilter.property("band").eq("Dead & Company"));
    let filtered_edges = g.filter(filter_expr).unwrap();

    assert_eq!(
        filtered_edges
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>(),
        vec!["John->David"]
    );

    let g_expected = Graph::new();
    g_expected
        .add_edge((1, 1), "John", "David", [("band", "Dead & Company")], None)
        .unwrap();

    assert_graph_equal(&filtered_edges, &g_expected);
}

#[test]
fn test_edge_filter_persistent() {
    let g = PersistentGraph::new();
    g.add_edge(0, "Jimi", "John", [("band", "JH Experience")], None)
        .unwrap();
    g.add_edge(1, "John", "David", [("band", "Dead & Company")], None)
        .unwrap();
    g.add_edge(2, "David", "Jimi", [("band", "Pink Floyd")], None)
        .unwrap();

    let filter_expr = EdgeFilter::dst()
        .name()
        .eq("David")
        .and(EdgeFilter.property("band").eq("Dead & Company"));
    let filtered_edges = g.filter(filter_expr).unwrap();

    let g_expected = PersistentGraph::new();
    g_expected
        .add_edge((1, 1), "John", "David", [("band", "Dead & Company")], None)
        .unwrap();

    assert_eq!(
        filtered_edges
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>(),
        vec!["John->David"]
    );
    assert_graph_equal(&filtered_edges, &g_expected);
}

#[test]
fn test_edge_property_filter_on_nodes() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, [("test", 1i64)], None).unwrap();
    g.add_edge(0, 1, 3, [("test", 3i64)], None).unwrap();
    g.add_edge(1, 2, 3, [("test", 2i64)], None).unwrap();
    g.add_edge(1, 2, 4, [("test", 0i64)], None).unwrap();

    let filter = EdgeFilter.property("test").eq(1i64);
    let n1 = g.node(1).unwrap().filter(filter).unwrap();
    assert_eq!(
        n1.edges().id().collect_vec(),
        vec![(GID::U64(1), GID::U64(2))]
    );
    let n2 = g
        .node(2)
        .unwrap()
        .filter(EdgeFilter.property("test").gt(1i64))
        .unwrap();
    assert_eq!(
        n2.edges().id().collect_vec(),
        vec![(GID::U64(2), GID::U64(3))]
    );
}

#[test]
fn test_filter() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, [("test", 1i64)], None).unwrap();
    g.add_edge(1, 2, 3, [("test", 2i64)], None).unwrap();

    let filter = EdgeFilter.property("test").eq(1i64);
    let gf = g.filter(filter).unwrap();
    assert_eq!(
        gf.edges().id().collect_vec(),
        vec![(GID::U64(1), GID::U64(2))]
    );
    let gf = g.filter(EdgeFilter.property("test").gt(1i64)).unwrap();
    assert_eq!(
        gf.edges().id().collect_vec(),
        vec![(GID::U64(2), GID::U64(3))]
    );
}

#[test]
fn test_filter_gt() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = EdgeFilter.property("int_prop").gt(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() > v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        });
    })
}

#[test]
fn test_filter_ge() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = EdgeFilter.property("int_prop").ge(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() >= v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        });
    })
}

#[test]
fn test_filter_lt() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = EdgeFilter.property("int_prop").lt(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() < v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        });
    })
}

#[test]
fn test_filter_le() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = EdgeFilter.property("int_prop").le(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() <= v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        });
    })
}

#[test]
fn test_filter_eq() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = EdgeFilter.property("int_prop").eq(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() == v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        });
    })
}

#[test]
fn test_filter_ne() {
    proptest!(|(
        edges in build_edge_list(100, 100), v in any::<i64>()
    )| {
        let g = build_graph_from_edge_list(&edges);
        let filter = EdgeFilter.property("int_prop").ne(v);
        assert_ok_or_missing_edges(&edges, g.filter(filter), |filtered| {
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() != v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        });
    })
}

#[test]
fn test_graph_materialise_window() {
    proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>(), (start, end) in build_window())| {
        let g = build_graph_from_edge_list(&edges);
        for (src, dst, t) in edge_deletions {
            g.delete_edge(t, src, dst, None).unwrap();
        }
        let filter = EdgeFilter.property("int_prop").gt(v);
        assert_ok_or_missing_edges(&edges, g.window(start, end).filter(filter.clone()), |filtered| {
            let gwfm = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &gwfm);
        });
        assert_ok_or_missing_edges(&edges, g.filter(filter.clone()), |filtered| {
            let gwf = filtered.window(start, end);
                let gwfm = gwf.materialize().unwrap();
            assert_graph_equal(&gwf, &gwfm);
        });
    })
}

#[test]
fn test_persistent_graph_materialise_window() {
    proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>(), (start, end) in build_window())| {
        let g = build_graph_from_edge_list(&edges);
        let g = g.persistent_graph();
        for (src, dst, t) in edge_deletions {
            g.delete_edge(t, src, dst, None).unwrap();
        }
        let filter = EdgeFilter.property("int_prop").gt(v);
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
fn test_single_unfiltered_edge_empty_window_persistent() {
    let g = PersistentGraph::new();
    g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
    g.delete_edge(10, 0, 1, None).unwrap();
    let gw = g
        .filter(EdgeFilter.property("test").gt(0i64))
        .unwrap()
        .window(0, 0);

    assert_eq!(gw.count_edges(), 0);
    let expected = PersistentGraph::new();
    expected
        .write_session()
        .unwrap()
        .resolve_edge_property("test", PropType::I64, false)
        .unwrap();
    expected.resolve_layer(None).unwrap();
    assert_graph_equal(&gw, &expected)
}

#[test]
fn test_single_deleted_edge_window_persistent() {
    let g = PersistentGraph::new();
    g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
    g.delete_edge(1, 0, 1, None).unwrap();
    let gw = g
        .filter(EdgeFilter.property("test").gt(0i64))
        .unwrap()
        .window(0, 2);
    let gm = gw.materialize().unwrap();

    assert_eq!(gw.count_edges(), 1);
    assert_eq!(gw.count_temporal_edges(), 1);

    assert_eq!(gw.node(0).unwrap().edge_history_count(), 2);
    assert_eq!(gw.node(0).unwrap().after(0).edge_history_count(), 1);

    assert_persistent_materialize_graph_equal(&gw, &gm)
}

#[test]
fn test_single_unfiltered_edge_window_persistent_2() {
    let g = PersistentGraph::new();
    g.add_edge(1, 0, 1, [("test", 1i64)], None).unwrap();
    g.delete_edge(0, 0, 0, None).unwrap();

    let gwf = g
        .window(-1, 2)
        .filter(EdgeFilter.property("test").gt(0i64))
        .unwrap();
    assert!(gwf.has_edge(0, 1));
    assert!(!gwf.has_edge(0, 0));
    assert_eq!(gwf.node(0).unwrap().earliest_time().map(|t| t.t()), Some(1));
    assert_persistent_materialize_graph_equal(&gwf, &gwf.materialize().unwrap());

    let gfw = g
        .filter(EdgeFilter.property("test").gt(0i64))
        .unwrap()
        .window(-1, 2);
    let gm = gfw.materialize().unwrap();

    assert_eq!(gfw.count_edges(), 1);
    assert_eq!(gfw.count_temporal_edges(), 1);

    assert_eq!(gfw.node(0).unwrap().edge_history_count(), 1);
    assert_eq!(gfw.node(0).unwrap().after(0).edge_history_count(), 1);

    assert_persistent_materialize_graph_equal(&gfw, &gm)
}
