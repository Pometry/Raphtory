use itertools::Itertools;
use raphtory::prelude::*;
use raphtory_api::core::storage::arc_str::ArcStr;
use std::collections::HashMap;

use crate::test_utils::test_graph;

pub mod test_utils;

#[test]
fn test_properties() {
    let graph = Graph::new();
    let props = [(ArcStr::from("test"), "test".into_prop())];
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(2, 1, 2, props.clone(), None).unwrap();
    test_storage!(&graph, |graph| {
        let e1 = graph.edge(1, 2).unwrap();
        let e1_w = graph.window(0, 1).edge(1, 2).unwrap();
        assert_eq!(
            HashMap::from_iter(e1.properties().as_vec()),
            props.clone().into()
        );
        assert!(e1_w.properties().as_vec().is_empty())
    });
}

#[test]
fn test_metadata() {
    let graph = Graph::new();
    graph
        .add_edge(1, 1, 2, NO_PROPS, Some("layer 1"))
        .unwrap()
        .add_metadata([("test_prop", "test_val")], None)
        .unwrap();
    graph
        .add_edge(1, 2, 3, NO_PROPS, Some("layer 2"))
        .unwrap()
        .add_metadata([("test_prop", "test_val"), ("other", "2")], None)
        .unwrap();

    graph
        .add_edge(1, 2, 3, NO_PROPS, Some("layer 3"))
        .unwrap()
        .add_metadata([("test_prop", "test_val"), ("other", "3")], None)
        .unwrap();

    // FIXME: #18 metadata prop for edges
    test_graph(&graph, |graph| {
        assert_eq!(
            graph.edge(1, 2).unwrap().metadata().get("test_prop"),
            Some(Prop::map([("layer 1", "test_val")]))
        );
        assert_eq!(
            graph.edge(2, 3).unwrap().metadata().get("test_prop"),
            Some(Prop::map([
                ("layer 2", "test_val"),
                ("layer 3", "test_val")
            ]))
        );

        assert_eq!(
            graph.edge(2, 3).unwrap().metadata().get("other"),
            Some(Prop::map([("layer 2", "2"), ("layer 3", "3")]))
        );

        assert_eq!(
            graph
                .valid_layers(["layer 3", "layer 2"])
                .edge(2, 3)
                .unwrap()
                .metadata()
                .get("other"),
            Some(Prop::map([("layer 2", "2"), ("layer 3", "3")]))
        );

        for e in graph.edges() {
            for ee in e.explode() {
                assert_eq!(ee.metadata().get("test_prop"), Some("test_val".into()))
            }
        }
    });
}

#[test]
fn test_property_additions() {
    let graph = Graph::new();
    let props = [("test", "test")];
    let e1 = graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    e1.add_updates(2, props, None).unwrap(); // same layer works
    assert!(e1.add_updates(2, props, Some("test2")).is_err()); // different layer is error
    let e = graph.edge(1, 2).unwrap();
    e.add_updates(2, props, Some("test2")).unwrap(); // non-restricted edge view can create new layers
    let layered_views = e.explode_layers().into_iter().collect_vec();
    for ev in layered_views {
        let layer = ev.layer_name().unwrap();
        assert!(ev.add_updates(1, props, Some("test")).is_err()); // restricted edge view cannot create updates in different layer
        ev.add_updates(1, [("test2", layer)], None).unwrap() // this will add an update to the same layer as the view (not the default layer)
    }

    let e1_w = e1.window(0, 1);
    assert_eq!(
        e1.properties().as_map(),
        props
            .into_iter()
            .map(|(k, v)| (ArcStr::from(k), v.into_prop()))
            .chain([(ArcStr::from("test2"), "_default".into_prop())])
            .collect()
    );
    assert_eq!(
        e.layers("test2").unwrap().properties().as_map(),
        props
            .into_iter()
            .map(|(k, v)| (ArcStr::from(k), v.into_prop()))
            .chain([(ArcStr::from("test2"), "test2".into_prop())])
            .collect()
    );
    assert_eq!(e1_w.properties().as_map(), HashMap::default())
}

#[test]
fn test_metadata_additions() {
    let g = Graph::new();
    let e = g.add_edge(0, 1, 2, NO_PROPS, Some("test")).unwrap();
    assert_eq!(e.edge.layer(), Some(1)); // 0 is static graph
    assert!(e.add_metadata([("test1", "test1")], None).is_ok()); // adds properties to layer `"test"`
    assert!(e.add_metadata([("test", "test")], Some("test2")).is_err()); // cannot add properties to a different layer
    e.add_metadata([("test", "test")], Some("test")).unwrap(); // layer is consistent
    assert_eq!(e.metadata().get("test"), Some("test".into()));
    assert_eq!(e.metadata().get("test1"), Some("test1".into()));
}

#[test]
fn test_metadata_updates() {
    let g = Graph::new();
    let e = g.add_edge(0, 1, 2, NO_PROPS, Some("test")).unwrap();
    assert!(e.add_metadata([("test1", "test1")], None).is_ok()); // adds properties to layer `"test"`
    assert!(e.update_metadata([("test1", "test2")], None).is_ok());
    assert_eq!(e.metadata().get("test1"), Some("test2".into()));
}

#[test]
fn test_layers_earliest_time() {
    let g = Graph::new();
    let e = g.add_edge(1, 1, 2, NO_PROPS, Some("test")).unwrap();
    assert_eq!(e.earliest_time(), Some(1));
}
