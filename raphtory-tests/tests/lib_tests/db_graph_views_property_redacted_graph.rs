#[cfg(test)]
mod tests {
    use raphtory::{
        algorithms::{
            metrics::balance::balance, pathing::dijkstra::dijkstra_single_source_shortest_paths,
        },
        db::{
            api::view::Filter,
            graph::{
                graph::Graph,
                views::{filter::model::PropertyExprFactory, property_redacted_graph::*},
            },
        },
        errors::GraphError,
        prelude::*,
    };
    use raphtory_api::core::Direction;

    fn make_graph() -> Graph {
        let g = Graph::new();
        // node with temporal prop "salary" and metadata "ssn"
        let n = g
            .add_node(
                1,
                "alice",
                [
                    ("salary", Prop::I64(100_000)),
                    ("region", Prop::str("us-west")),
                ],
                None,
                None,
            )
            .unwrap();
        n.add_metadata([
            ("ssn", Prop::str("123-45-6789")),
            ("dept", Prop::str("eng")),
        ])
        .unwrap();
        // edge with temporal prop "amount" and metadata "ref"
        let e = g
            .add_edge(
                1,
                "alice",
                "bob",
                [("amount", Prop::I64(50)), ("kind", Prop::str("transfer"))],
                None,
            )
            .unwrap();
        e.add_metadata(
            [("ref", Prop::str("REF001")), ("channel", Prop::str("web"))],
            None,
        )
        .unwrap();
        // graph-level temporal prop and metadata
        g.add_properties(
            1,
            [("env", Prop::str("prod")), ("version", Prop::str("1.0"))],
        )
        .unwrap();
        g.add_metadata([("secret", Prop::str("s3cr3t")), ("org", Prop::str("acme"))])
            .unwrap();
        g
    }

    #[test]
    fn test_exclude_node_temporal_property() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_node_props(["salary"]));
        let alice = view.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be hidden"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should be visible"
        );
    }

    #[test]
    fn test_exclude_node_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_node_meta(["ssn"]));
        let alice = view.node("alice").unwrap();
        assert!(
            alice.metadata().get("ssn").is_none(),
            "ssn metadata should be hidden"
        );
        assert!(
            alice.metadata().get("dept").is_some(),
            "dept should be visible"
        );
        assert!(
            alice.properties().get("salary").is_some(),
            "salary temporal prop should still be visible"
        );
    }

    #[test]
    fn test_exclude_node_properties_does_not_hide_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_node_props(["salary"]));
        let alice = view.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary temporal prop should be hidden"
        );
        assert!(
            alice.metadata().get("ssn").is_some(),
            "ssn metadata should still be visible"
        );
    }

    #[test]
    fn test_exclude_edge_properties() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_edge_props(["amount"]));
        let e = view.edge("alice", "bob").unwrap();
        assert!(
            e.properties().get("amount").is_none(),
            "amount should be hidden"
        );
        assert!(
            e.properties().get("kind").is_some(),
            "kind should be visible"
        );
        assert!(
            e.metadata().get("ref").is_some(),
            "ref metadata should still be visible"
        );
    }

    #[test]
    fn test_exclude_edge_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_edge_meta(["ref"]));
        let e = view.edge("alice", "bob").unwrap();
        assert!(
            e.metadata().get("ref").is_none(),
            "ref metadata should be hidden"
        );
        assert!(
            e.metadata().get("channel").is_some(),
            "channel should be visible"
        );
        assert!(
            e.properties().get("amount").is_some(),
            "amount temporal prop should still be visible"
        );
    }

    #[test]
    fn test_exclude_graph_properties() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_graph_props(["env"]));
        assert!(
            view.properties().get("env").is_none(),
            "env should be hidden"
        );
        assert!(
            view.properties().get("version").is_some(),
            "version should be visible"
        );
        assert!(
            view.metadata().get("secret").is_some(),
            "secret metadata should still be visible"
        );
    }

    #[test]
    fn test_exclude_graph_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_graph_meta(["secret"]));
        assert!(
            view.metadata().get("secret").is_none(),
            "secret metadata should be hidden"
        );
        assert!(
            view.metadata().get("org").is_some(),
            "org should be visible"
        );
        assert!(
            view.properties().get("env").is_some(),
            "env temporal prop should still be visible"
        );
    }

    #[test]
    fn test_chaining_node_and_edge() {
        let g = make_graph();
        let view = g.exclude_properties(
            &PropertyRedaction::default()
                .with_node_props(["salary"])
                .with_edge_props(["amount"]),
        );
        let alice = view.node("alice").unwrap();
        let e = view.edge("alice", "bob").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be hidden"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should be visible"
        );
        assert!(
            e.properties().get("amount").is_none(),
            "amount should be hidden"
        );
        assert!(
            e.properties().get("kind").is_some(),
            "kind should be visible"
        );
    }

    #[test]
    fn test_materialize_redacts_node_temporal_property() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_node_props(["salary"]));
        let materialized = view.materialize().unwrap();
        let alice = materialized.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be absent from materialized graph"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should be in materialized graph"
        );
    }

    #[test]
    fn test_materialize_redacts_node_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_node_meta(["ssn"]));
        let materialized = view.materialize().unwrap();
        let alice = materialized.node("alice").unwrap();
        assert!(
            alice.metadata().get("ssn").is_none(),
            "ssn should be absent from materialized graph"
        );
        assert!(
            alice.metadata().get("dept").is_some(),
            "dept should be in materialized graph"
        );
    }

    #[test]
    fn test_materialize_redacts_edge_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_edge_meta(["ref"]));
        let materialized = view.materialize().unwrap();
        let e = materialized.edge("alice", "bob").unwrap();
        assert!(
            e.metadata().get("ref").is_none(),
            "ref should be absent from materialized graph"
        );
        assert!(
            e.metadata().get("channel").is_some(),
            "channel should be in materialized graph"
        );
    }

    #[test]
    fn test_materialize_redacts_graph_metadata() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_graph_meta(["secret"]));
        let materialized = view.materialize().unwrap();
        assert!(
            materialized.metadata().get("secret").is_none(),
            "secret should be absent from materialized graph"
        );
        assert!(
            materialized.metadata().get("org").is_some(),
            "org should be in materialized graph"
        );
    }

    #[test]
    fn test_window_then_exclude() {
        let g = make_graph();
        let view = g
            .window(0, 2)
            .exclude_properties(&PropertyRedaction::default().with_node_props(["salary"]));
        let alice = view.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be hidden in windowed+redacted view"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should still be visible"
        );
    }

    /// A hidden key answers as a key the graph never had, so a filter cannot test its values.
    #[test]
    fn test_filter_reads_a_hidden_key_as_missing() {
        let g = make_graph();
        let view = g.exclude_properties(
            &PropertyRedaction::default()
                .with_node_props(["salary"])
                .with_node_meta(["ssn"])
                .with_edge_props(["amount"])
                .with_edge_meta(["ref"]),
        );
        assert!(matches!(
            view.filter(NodeFilter.property("salary").gt(0i64)),
            Err(GraphError::PropertyMissingError(_))
        ));
        assert!(matches!(
            view.filter(NodeFilter.metadata("ssn").eq("123-45-6789")),
            Err(GraphError::MetadataMissingError(_))
        ));
        assert!(matches!(
            view.filter(EdgeFilter.property("amount").gt(0i64)),
            Err(GraphError::PropertyMissingError(_))
        ));
        assert!(matches!(
            view.filter(EdgeFilter.metadata("ref").eq("REF001")),
            Err(GraphError::MetadataMissingError(_))
        ));
        let visible = view
            .filter(NodeFilter.property("region").eq("us-west"))
            .unwrap();
        assert_eq!(visible.count_nodes(), 1);
        // The id lookups on `Nodes` and `Edges` hide the same keys.
        assert_eq!(view.nodes().get_temporal_prop_id("salary"), None);
        assert_eq!(view.nodes().get_metadata_id("ssn"), None);
        assert_eq!(view.edges().get_temporal_prop_id("amount"), None);
        assert_eq!(view.edges().get_metadata_id("ref"), None);
        assert!(view.nodes().get_temporal_prop_id("region").is_some());
    }

    #[test]
    fn test_algorithms_read_a_hidden_edge_property_as_missing() {
        let g = make_graph();
        let view = g.exclude_properties(&PropertyRedaction::default().with_edge_props(["amount"]));
        assert!(balance(&view, "amount".to_string(), Direction::BOTH).is_err());
        assert!(matches!(
            dijkstra_single_source_shortest_paths(
                &view,
                "alice",
                vec!["bob"],
                Some("amount"),
                Direction::OUT,
                None,
            ),
            Err(GraphError::PropertyMissingError(_))
        ));
    }
}
