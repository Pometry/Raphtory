#[cfg(test)]
mod template_tests {
    use indoc::indoc;

    use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, PropertyAdditionOps, NO_PROPS};

    use raphtory_vectors::template::*;

    #[test]
    fn test_default_templates() {
        let graph = Graph::new();
        graph.add_metadata([("name", "test-name")]).unwrap();

        let node1 = graph
            .add_node(0, "node1", [("temp_test", "value_at_0")], None, None)
            .unwrap();
        graph
            .add_node(1, "node1", [("temp_test", "value_at_1")], None, None)
            .unwrap();
        node1
            .add_metadata([("key1", "value1"), ("key2", "value2")])
            .unwrap();

        for time in [0, 60_000] {
            graph
                .add_edge(time, "node1", "node2", NO_PROPS, Some("fancy-layer"))
                .unwrap();
        }

        let template = DocumentTemplate {
            node_template: Some(DEFAULT_NODE_TEMPLATE.to_owned()),
            edge_template: Some(DEFAULT_EDGE_TEMPLATE.to_owned()),
        };

        let rendered = template.node(graph.node("node1").unwrap()).unwrap();
        let expected = indoc! {"
            Node node1 has the following properties:
            key1: value1
            key2: value2
            temp_test:
             - changed to value_at_0 at Jan 1 1970 00:00
             - changed to value_at_1 at Jan 1 1970 00:00
        "};
        assert_eq!(&rendered, expected);

        let rendered = template
            .edge(graph.edge("node1", "node2").unwrap())
            .unwrap();
        let expected = indoc! {"
            There is an edge from node1 to node2 with events at:
            - Jan 1 1970 00:00
            - Jan 1 1970 00:01
        "};
        assert_eq!(&rendered, expected);
    }

    #[test]
    fn test_node_template() {
        let graph = Graph::new();

        let node1 = graph
            .add_node(0, "node1", [("temp_test", "value_at_0")], None, None)
            .unwrap();
        graph
            .add_node(1, "node1", [("temp_test", "value_at_1")], None, None)
            .unwrap();
        node1
            .add_metadata([("key1", "value1"), ("key2", "value2")])
            .unwrap();
        let node2 = graph
            .add_node(0, "node2", NO_PROPS, Some("person"), None)
            .unwrap();
        node2
            .add_metadata([("const_test", "const_test_value")])
            .unwrap();

        // I should be able to iterate over properties without doing properties|items, which would be solved by implementing Object for Properties
        let node_template = indoc! {"
            node {{ name }} is {% if node_type is none %}an unknown entity{% else %}a {{ node_type }}{% endif %} with the following properties:
            {% if metadata.const_test is not none %}const_test: {{ metadata.const_test }} {% endif %}
            {% if temporal_properties.temp_test is defined and temporal_properties.temp_test|length > 0 %}
            temp_test:
            {% for (time, value) in temporal_properties.temp_test %}
             - changed to {{ value }} at {{ time }}
            {% endfor %}
            {% endif %}
            {% for (key, value) in properties|items if key != \"temp_test\" and key != \"const_test\" %}
            {{ key }}: {{ value }}
            {% endfor %}
            {% for (key, value) in metadata|items if key != \"const_test\" %}
            {% if value is not none %}
            {{ key }}: {{ value }}
            {% endif %}
            {% endfor %}
        "};
        let template = DocumentTemplate {
            node_template: Some(node_template.to_owned()),
            edge_template: None,
        };

        let rendered = template.node(graph.node("node1").unwrap()).unwrap();
        let expected = indoc! {"
            node node1 is an unknown entity with the following properties:
            temp_test:
             - changed to value_at_0 at 0
             - changed to value_at_1 at 1
            key1: value1
            key2: value2
        "};
        assert_eq!(&rendered, expected);

        let rendered = template.node(graph.node("node2").unwrap()).unwrap();
        let expected = indoc! {"
            node node2 is a person with the following properties:
            const_test: const_test_value "};
        assert_eq!(&rendered, expected);
    }

    #[test]
    fn test_datetimes() {
        let graph = Graph::new();
        graph
            .add_node(
                "2024-09-09T09:08:01",
                "node1",
                [("temp", "value")],
                None,
                None,
            )
            .unwrap();

        // I should be able to iteate over properties without doing properties|items, which would be solved by implementing Object for Properties
        let node_template =
            "{{ (temporal_properties.temp|first).time|datetimeformat(format=\"long\") }}";
        let template = DocumentTemplate {
            node_template: Some(node_template.to_owned()),
            edge_template: None,
        };

        let rendered = template.node(graph.node("node1").unwrap()).unwrap();
        let expected = "September 9 2024 09:08:01";
        assert_eq!(&rendered, expected);
    }
}
