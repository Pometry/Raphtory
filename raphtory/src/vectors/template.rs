use super::datetimeformat::datetimeformat;
use crate::{
    core::{DocumentInput, Prop},
    db::{
        api::properties::TemporalPropertyView,
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
};
use minijinja::{
    value::{Enumerator, Object},
    Environment, Template, Value,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

#[derive(Debug)]
struct PropUpdate {
    time: i64,
    value: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<TemporalPropertyView<NodeView<G>>> for Value {
    fn from(value: TemporalPropertyView<NodeView<G>>) -> Self {
        value
            .iter()
            .map(|(time, value)| PropUpdate {
                time,
                value: value.into(),
            })
            .map(Value::from_object)
            .collect()
    }
}

// FIXME: merge with the one above
impl<'graph, G: GraphViewOps<'graph>> From<TemporalPropertyView<G>> for Value {
    fn from(value: TemporalPropertyView<G>) -> Self {
        value
            .iter()
            .map(|(time, value)| PropUpdate {
                time,
                value: value.into(),
            })
            .map(Value::from_object)
            .collect()
    }
}

impl Object for PropUpdate {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "time" => Some(Value::from(self.time)),
            "value" => Some(self.value.clone()),
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Values(vec![self.time.into(), self.value.clone()])
    }
}

#[derive(Serialize)]
struct NodeTemplateContext {
    name: String,
    node_type: Option<ArcStr>,
    properties: Value,
    constant_properties: Value,
    temporal_properties: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<NodeView<G>> for NodeTemplateContext {
    fn from(value: NodeView<G>) -> Self {
        Self {
            name: value.name(),
            node_type: value.node_type(),
            properties: value
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            constant_properties: value
                .properties()
                .constant()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            temporal_properties: value
                .properties()
                .temporal()
                .iter()
                .map(|(key, prop)| (key.to_string(), Into::<Value>::into(prop)))
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct GraphTemplateContext {
    properties: Value,
    constant_properties: Value,
    temporal_properties: Value,
}

// FIXME: boilerplate for the properties
impl<'graph, G: GraphViewOps<'graph>> From<G> for GraphTemplateContext {
    fn from(value: G) -> Self {
        Self {
            properties: value
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            constant_properties: value
                .properties()
                .constant()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            temporal_properties: value
                .properties()
                .temporal()
                .iter()
                .map(|(key, prop)| (key.to_string(), Into::<Value>::into(prop)))
                .collect(),
        }
    }
}

// FIXME: this is eagerly allocating a lot of stuff, we should implement Object instead for Prop
impl From<Prop> for Value {
    fn from(value: Prop) -> Self {
        match value {
            Prop::Bool(value) => Value::from(value),
            Prop::F32(value) => Value::from(value),
            Prop::F64(value) => Value::from(value),
            Prop::I32(value) => Value::from(value),
            Prop::I64(value) => Value::from(value),
            Prop::U8(value) => Value::from(value),
            Prop::U16(value) => Value::from(value),
            Prop::U32(value) => Value::from(value),
            Prop::U64(value) => Value::from(value),
            Prop::Str(value) => Value::from(value.0.to_owned()),
            Prop::DTime(value) => Value::from(value.timestamp_millis()),
            Prop::NDTime(value) => Value::from(value.and_utc().timestamp_millis()),
            Prop::List(value) => value.iter().cloned().collect(),
            Prop::Map(value) => value
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            Prop::Document(value) => Value::from(value.content),
            Prop::Graph(value) => Value::from(value.to_string()),
            Prop::PersistentGraph(value) => Value::from(value.to_string()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct DocumentTemplate {
    pub graph_template: Option<String>,
    pub node_template: Option<String>,
    pub edge_template: Option<String>,
}

fn empty_iter() -> Box<dyn Iterator<Item = DocumentInput> + Send> {
    Box::new(std::iter::empty())
}

impl DocumentTemplate {
    pub(crate) fn graph<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: G,
    ) -> Box<dyn Iterator<Item = DocumentInput> + Send> {
        match &self.graph_template {
            Some(template) => {
                // TODO: create the environment only once and store it on the DocumentTemplate struct
                let mut env = Environment::new();
                let template = build_template(&mut env, template);
                match template.render(GraphTemplateContext::from(graph)) {
                    Ok(mut document) => {
                        truncate(&mut document);
                        Box::new(std::iter::once(document.into()))
                    }
                    Err(error) => {
                        error!("Template render failed for a node, skipping: {error}");
                        empty_iter()
                    }
                }
            }
            None => empty_iter(),
        }
    }

    /// A function that translate a node into an iterator of documents
    pub(crate) fn node<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeView<G>,
    ) -> Box<dyn Iterator<Item = DocumentInput> + Send> {
        match &self.node_template {
            Some(template) => {
                let mut env = Environment::new();
                let template = build_template(&mut env, template);
                match template.render(NodeTemplateContext::from(node)) {
                    Ok(mut document) => {
                        truncate(&mut document);
                        Box::new(std::iter::once(document.into()))
                    }
                    Err(error) => {
                        error!("Template render failed for a node, skipping: {error}");
                        empty_iter()
                    }
                }
            }
            None => empty_iter(),
        }
    }

    /// A function that translate an edge into an iterator of documents
    pub(crate) fn edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeView<G, G>,
    ) -> Box<dyn Iterator<Item = DocumentInput> + Send> {
        match &self.edge_template {
            Some(template) => {
                let mut env = Environment::new();
                let template = build_template(&mut env, template);
                match template.render(EdgeTemplateContext::from(edge)) {
                    Ok(mut document) => {
                        truncate(&mut document);
                        Box::new(std::iter::once(document.into()))
                    }
                    Err(error) => {
                        error!("Template render failed for an edge, skipping: {error}");
                        empty_iter()
                    }
                }
            }
            None => empty_iter(),
        }
    }
}

fn truncate(text: &mut String) {
    let limit = text.char_indices().nth(1000);
    if let Some((index, _)) = limit {
        text.truncate(index);
    }
}

fn build_template<'a>(env: &'a mut Environment<'a>, template: &'a str) -> Template<'a, 'a> {
    minijinja_contrib::add_to_environment(env);
    env.add_filter("datetimeformat", datetimeformat);
    // it's important adding these settings
    env.set_trim_blocks(true);
    env.set_lstrip_blocks(true);
    // before adding any template
    env.add_template("template", template).unwrap();
    env.get_template("template").unwrap()
}

#[derive(Serialize)]
struct EdgeTemplateContext {
    src: NodeTemplateContext,
    dst: NodeTemplateContext,
    history: Vec<i64>,
    layers: Vec<String>,
    properties: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<EdgeView<G>> for EdgeTemplateContext {
    fn from(value: EdgeView<G>) -> Self {
        Self {
            src: value.src().into(),
            dst: value.dst().into(),
            history: value.history(),
            layers: value
                .layer_names()
                .into_iter()
                .map(|name| name.into())
                .collect(),
            properties: value // FIXME: boilerplate
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
        }
    }
}

pub const DEFAULT_NODE_TEMPLATE: &str = "Node {{ name }} {% if node_type is none %} has the following properties:{% else %} is a {{ node_type }} with the following properties:{% endif %}

{% for (key, value) in constant_properties|items %}
{{ key }}: {{ value }}
{% endfor %}
{% for (key, values) in temporal_properties|items %}
{{ key }}:
{% for (time, value) in values %}
 - changed to {{ value }} at {{ time|datetimeformat }}
{% endfor %}
{% endfor %}";

pub const DEFAULT_EDGE_TEMPLATE: &str =
    "There is an edge from {{ src.name }} to {{ dst.name }} with events at:
{% for time in history %}
- {{ time|datetimeformat }}
{% endfor %}";

pub const DEFAULT_GRAPH_TEMPLATE: &str = "Graph with the following properties:
{% for (key, value) in constant_properties|items %}
{{ key }}: {{ value }}
{% endfor %}
{% for (key, values) in temporal_properties|items %}
{{ key }}:
{% for (time, value) in values %}
 - changed to {{ value }} at {{ time|datetimeformat }}
{% endfor %}
{% endfor %}";

#[cfg(test)]
mod template_tests {
    use indoc::indoc;

    use crate::prelude::{AdditionOps, Graph, GraphViewOps, PropertyAdditionOps, NO_PROPS};

    use super::*;

    #[test]
    fn test_default_templates() {
        let graph = Graph::new();
        graph
            .add_constant_properties([("name", "test-name")])
            .unwrap();

        let node1 = graph
            .add_node(0, "node1", [("temp_test", "value_at_0")], None)
            .unwrap();
        graph
            .add_node(1, "node1", [("temp_test", "value_at_1")], None)
            .unwrap();
        node1
            .add_constant_properties([("key1", "value1"), ("key2", "value2")])
            .unwrap();

        for time in [0, 60_000] {
            graph
                .add_edge(time, "node1", "node2", NO_PROPS, Some("fancy-layer"))
                .unwrap();
        }

        let template = DocumentTemplate {
            node_template: Some(DEFAULT_NODE_TEMPLATE.to_owned()),
            graph_template: Some(DEFAULT_GRAPH_TEMPLATE.to_owned()),
            edge_template: Some(DEFAULT_EDGE_TEMPLATE.to_owned()),
        };

        let mut docs = template.node(graph.node("node1").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            Node node1 has the following properties:
            key1: value1
            key2: value2
            temp_test:
             - changed to value_at_0 at Jan 1 1970 00:00
             - changed to value_at_1 at Jan 1 1970 00:00
        "};
        assert_eq!(&rendered, expected);

        let mut docs = template.edge(graph.edge("node1", "node2").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            There is an edge from node1 to node2 with events at:
            - Jan 1 1970 00:00
            - Jan 1 1970 00:01
        "};
        assert_eq!(&rendered, expected);

        let mut docs = template.graph(graph);
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            Graph with the following properties:
            name: test-name
        "};
        assert_eq!(&rendered, expected);
    }

    #[test]
    fn test_node_template() {
        let graph = Graph::new();

        let node1 = graph
            .add_node(0, "node1", [("temp_test", "value_at_0")], None)
            .unwrap();
        graph
            .add_node(1, "node1", [("temp_test", "value_at_1")], None)
            .unwrap();
        node1
            .add_constant_properties([("key1", "value1"), ("key2", "value2")])
            .unwrap();
        let node2 = graph
            .add_node(0, "node2", NO_PROPS, Some("person"))
            .unwrap();
        node2
            .add_constant_properties([("const_test", "const_test_value")])
            .unwrap();

        // I should be able to iterate over properties without doing properties|items, which would be solved by implementing Object for Properties
        let node_template = indoc! {"
            node {{ name }} is {% if node_type is none %}an unknown entity{% else %}a {{ node_type }}{% endif %} with the following properties:
            {% if properties.const_test is defined %}const_test: {{ properties.const_test }} {% endif %}
            {% if temporal_properties.temp_test is defined and temporal_properties.temp_test|length > 0 %}
            temp_test:
            {% for (time, value) in temporal_properties.temp_test %}
             - changed to {{ value }} at {{ time }}
            {% endfor %}
            {% endif %}
            {% for (key, value) in properties|items if key != \"temp_test\" and key != \"const_test\" %}
            {{ key }}: {{ value }}
            {% endfor %}
            {% for (key, value) in constant_properties|items if key != \"const_test\" %}
            {{ key }}: {{ value }}
            {% endfor %}
        "};
        let template = DocumentTemplate {
            node_template: Some(node_template.to_owned()),
            graph_template: None,
            edge_template: None,
        };

        let mut docs = template.node(graph.node("node1").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            node node1 is an unknown entity with the following properties:
            temp_test:
             - changed to value_at_0 at 0
             - changed to value_at_1 at 1
            key1: value1
            key2: value2
            key1: value1
            key2: value2
        "};
        assert_eq!(&rendered, expected);

        let mut docs = template.node(graph.node("node2").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            node node2 is a person with the following properties:
            const_test: const_test_value"};
        assert_eq!(&rendered, expected);
    }

    #[test]
    fn test_datetimes() {
        let graph = Graph::new();
        graph
            .add_node("2024-09-09T09:08:01", "node1", [("temp", "value")], None)
            .unwrap();

        // I should be able to iteate over properties without doing properties|items, which would be solved by implementing Object for Properties
        let node_template =
            "{{ (temporal_properties.temp|first).time|datetimeformat(format=\"long\") }}";
        let template = DocumentTemplate {
            node_template: Some(node_template.to_owned()),
            graph_template: None,
            edge_template: None,
        };

        let mut docs = template.node(graph.node("node1").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = "September 9 2024 09:08:01";
        assert_eq!(&rendered, expected);
    }
}
