use raphtory::{db::api::view::IntoDynamic, prelude::*};
use raphtory_tests::{test_storage, utils::test_graph};

#[test]
fn test_exploded_edge_properties() {
    let graph = Graph::new();
    let actual_prop_values = vec![0, 1, 2, 3];
    for v in actual_prop_values.iter() {
        graph.add_edge(0, 1, 2, [("test", *v)], None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let prop_values: Vec<_> = graph
            .edge(1, 2)
            .unwrap()
            .explode()
            .properties()
            .flat_map(|p| p.get("test").into_i32())
            .collect();
        assert_eq!(prop_values, actual_prop_values)
    });
}

#[test]
fn test_exploded_edge_properties_window() {
    let graph = Graph::new();
    let actual_prop_values_0 = vec![0, 1, 2, 3];
    for v in actual_prop_values_0.iter() {
        graph.add_edge(0, 1, 2, [("test", *v)], None).unwrap();
    }
    let actual_prop_values_1 = vec![4, 5, 6];
    for v in actual_prop_values_1.iter() {
        graph.add_edge(1, 1, 2, [("test", *v)], None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let prop_values: Vec<_> = graph
            .at(0)
            .edge(1, 2)
            .unwrap()
            .explode()
            .properties()
            .flat_map(|p| p.get("test").into_i32())
            .collect();
        assert_eq!(prop_values, actual_prop_values_0);
        let prop_values: Vec<_> = graph
            .at(1)
            .edge(1, 2)
            .unwrap()
            .explode()
            .properties()
            .flat_map(|p| p.get("test").into_i32())
            .collect();
        assert_eq!(prop_values, actual_prop_values_1)
    });
}

#[test]
fn test_exploded_edge_multilayer() {
    let graph = Graph::new();
    let expected_prop_values = vec![0, 1, 2, 3];
    for v in expected_prop_values.iter() {
        graph
            .add_edge(0, 1, 2, [("test", *v)], Some((v % 2).to_string().as_str()))
            .unwrap();
    }
    // FIXME: Needs support for event id from EventTime in disk storage (Issue #30)
    test_graph(&graph, |graph| {
        let prop_values: Vec<_> = graph
            .edge(1, 2)
            .unwrap()
            .explode()
            .properties()
            .flat_map(|p| p.get("test").into_i32())
            .collect();
        let actual_layers: Vec<_> = graph
            .edge(1, 2)
            .unwrap()
            .explode()
            .layer_name()
            .flatten()
            .collect();
        let expected_layers: Vec<_> = expected_prop_values
            .iter()
            .map(|v| (v % 2).to_string())
            .collect();
        assert_eq!(prop_values, expected_prop_values);
        assert_eq!(actual_layers, expected_layers);

        assert!(graph.edge(1, 2).unwrap().layer_name().is_err());
        assert!(graph.edges().layer_name().all(|l| l.is_err()));
        assert!(graph
            .edge(1, 2)
            .unwrap()
            .explode()
            .layer_name()
            .all(|l| l.is_ok()));
        assert!(graph
            .edge(1, 2)
            .unwrap()
            .explode_layers()
            .layer_name()
            .all(|l| l.is_ok()));
        assert!(graph.edges().explode().layer_name().all(|l| l.is_ok()));
        assert!(graph
            .edges()
            .explode_layers()
            .layer_name()
            .all(|l| l.is_ok()));

        assert!(graph.edge(1, 2).unwrap().time().is_err());
        assert!(graph.edges().time().all(|l| l.is_err()));
        assert!(graph
            .edge(1, 2)
            .unwrap()
            .explode()
            .time()
            .all(|l| l.is_ok()));
        assert!(graph
            .edge(1, 2)
            .unwrap()
            .explode_layers()
            .time()
            .all(|l| l.is_err()));
        assert!(graph.edges().explode().time().all(|l| l.is_ok()));
        assert!(graph.edges().explode_layers().time().all(|l| l.is_err()));
    });
}

#[test]
fn test_sorting_by_event_id() {
    let graph = Graph::new();
    graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(0, 1, 2, [("second", true)], None).unwrap();
    graph.add_edge(0, 2, 3, [("second", true)], None).unwrap();

    //FIXME: DiskGraph does not preserve event id (see #1780)
    test_graph(&graph, |graph| {
        let mut exploded_edges: Vec<_> = graph.edges().explode().into_iter().collect();
        exploded_edges.sort_by_key(|a| a.time_and_event_id().unwrap());

        let res: Vec<_> = exploded_edges
            .into_iter()
            .filter_map(|e| {
                Some((
                    e.src().id().as_u64()?,
                    e.dst().id().as_u64()?,
                    e.properties().get("second").into_bool(),
                ))
            })
            .collect();
        assert_eq!(
            res,
            vec![
                (2, 3, None),
                (1, 2, None),
                (1, 2, Some(true)),
                (2, 3, Some(true))
            ]
        )
    });
}

/// A part of an edge on a layer the view hides has no answer: a layer edge or an
/// exploded edge on layer `x`, read through a view of layer `y`, shows no property,
/// history, metadata or activity.
#[test]
fn a_read_through_a_hidden_layer_has_no_answer() {
    let g = Graph::new();
    g.add_edge(1, "a", "b", [("w", 1i64)], Some("x")).unwrap();
    g.add_edge(2, "a", "b", [("w", 2i64)], Some("y")).unwrap();
    g.edge("a", "b")
        .unwrap()
        .add_metadata([("m", 9i64)], Some("x"))
        .unwrap();
    for graph in [
        g.clone().into_dynamic(),
        g.persistent_graph().into_dynamic(),
    ] {
        let edge = graph.edge("a", "b").unwrap();
        let (layers, exploded) = (edge.explode_layers(), edge.explode());
        let on_x = layers
            .iter()
            .chain(exploded.iter())
            .filter(|e| e.layer_name().unwrap() == "x")
            .collect::<Vec<_>>();
        assert_eq!(on_x.len(), 2);
        for e in on_x {
            let hidden = e.valid_layers(["y"]);
            assert_eq!(hidden.properties().get("w"), None);
            assert_eq!(
                hidden
                    .properties()
                    .temporal()
                    .get("w")
                    .map(|p| p.values().collect::<Vec<_>>()),
                Some(vec![])
            );
            assert_eq!(hidden.metadata().get("m"), None);
            assert!(!hidden.is_active());
            assert!(!hidden.is_valid());
        }
    }
}
