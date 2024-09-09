use minijinja::{
    value::{Enumerator, Object},
    Environment, Template, Value,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::Serialize;
use std::sync::Arc;

use crate::{
    core::{DocumentInput, Prop},
    db::{
        api::{properties::TemporalPropertyView, view::StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
};

#[derive(Debug)]
struct PropUpdate {
    time: i64,
    value: Value,
}

impl<G: StaticGraphViewOps> From<TemporalPropertyView<NodeView<G>>> for Value {
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
impl<G: StaticGraphViewOps> From<TemporalPropertyView<G>> for Value {
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
    props: Value,
    constant_props: Value,
    temporal_props: Value,
}

impl<G: StaticGraphViewOps> From<&NodeView<G>> for NodeTemplateContext {
    fn from(value: &NodeView<G>) -> Self {
        Self {
            name: value.name(),
            node_type: value.node_type(),
            props: value
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            constant_props: value
                .properties()
                .constant()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            temporal_props: value
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
    // name: String, // TODO: add the name, I need some trait
    props: Value,
    constant_props: Value,
    temporal_props: Value,
}

// FIXME: boilerplate for the properties
impl<G: StaticGraphViewOps> From<&G> for GraphTemplateContext {
    fn from(value: &G) -> Self {
        Self {
            // name: value.name(),
            props: value
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            constant_props: value
                .properties()
                .constant()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            temporal_props: value
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
            Prop::DTime(value) => Value::from(value.to_string()), // TODO: review this, should return the epoch!!!!!! should be possible to be consumed by the datetime formatting functions
            Prop::NDTime(value) => Value::from(value.to_string()), // TODO: review this, should return the epoch!!!!!! should be possible to be consumed by the datetime formatting functions
            Prop::List(value) => value.iter().cloned().collect(),
            Prop::Map(value) => value
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            Prop::Document(value) => Value::from(value.content),
            Prop::Graph(value) => Value::from(value.to_string()), // TODO: review this
            Prop::PersistentGraph(value) => Value::from(value.to_string()), // TODO: review this
        }
    }
}

#[derive(Clone)]
pub struct DocumentTemplate {
    pub graph_template: Option<String>,
    pub node_template: Option<String>,
    pub edge_template: Option<String>,
}

fn empty_iter() -> Box<dyn Iterator<Item = DocumentInput>> {
    Box::new(std::iter::empty())
}

impl DocumentTemplate {
    pub(crate) fn graph<G: StaticGraphViewOps>(
        &self,
        graph: &G,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.graph_template {
            Some(template) => {
                let mut env = Environment::new();
                let template = build_template(&mut env, template);
                let document = template.render(GraphTemplateContext::from(graph)).unwrap(); // FIXME: this unwrap?
                Box::new(std::iter::once(document.into()))
            }
            None => empty_iter(),
        }
    }

    /// A function that translate a node into an iterator of documents
    pub(crate) fn node<G: StaticGraphViewOps>(
        &self,
        node: &NodeView<G>,
    ) -> impl Iterator<Item = DocumentInput> {
        match &self.node_template {
            Some(template) => {
                let mut env = Environment::new();
                let template = build_template(&mut env, template);
                let document = template.render(NodeTemplateContext::from(node)).unwrap(); // FIXME: this unwrap?
                Box::new(std::iter::once(document.into()))
            }
            None => empty_iter(),
        }
    }

    /// A function that translate an edge into an iterator of documents
    pub(crate) fn edge<G: StaticGraphViewOps>(
        &self,
        edge: &EdgeView<G, G>,
    ) -> impl Iterator<Item = DocumentInput> {
        match &self.edge_template {
            Some(template) => {
                let mut env = Environment::new();
                let template = build_template(&mut env, template);
                let document = template.render(EdgeTemplateContext::from(edge)).unwrap(); // FIXME: this unwrap?
                Box::new(std::iter::once(document.into()))
            }
            None => empty_iter(),
        }
    }
}

fn build_template<'a>(env: &'a mut Environment<'a>, template: &'a str) -> Template<'a, 'a> {
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
    props: Value,
}

impl<G: StaticGraphViewOps> From<&EdgeView<G>> for EdgeTemplateContext {
    fn from(value: &EdgeView<G>) -> Self {
        Self {
            src: (&value.src()).into(),
            dst: (&value.dst()).into(),
            history: value.history(),
            layers: value
                .layer_names()
                .into_iter()
                .map(|name| name.into()) // TODO: there has to be something easier than key.0.as_ref().to_owned()
                .collect(),
            props: value // FIXME: boilerplate
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone())) // TODO: there has to be something easier than key.0.as_ref().to_owned()
                .collect(),
        }
    }
}

#[cfg(test)]
mod template_tests {
    use indoc::indoc;

    use crate::prelude::{AdditionOps, Graph, NO_PROPS};

    use super::*;

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

        // I should be able to iteate over props without doing props|items, which would be solved by implementing Object for Properties
        let node_template = indoc! {"
            node {{ name }} is {% if node_type is none %}an unknown entity{% else %}a {{ node_type }}{% endif %} with the following props:
            {% if props.const_test is defined %}const_test: {{ props.const_test }} {% endif %}
            {% if temporal_props.temp_test is defined and temporal_props.temp_test|length > 0 %}
            temp_test:
            {% for (time, value) in temporal_props.temp_test %}
             - changed to {{ value }} at {{ time }}
            {% endfor %}
            {% endif %}
            {% for (key, value) in props|items if key != \"temp_test\" and key != \"const_test\" %}
            {{ key }}: {{ value }}
            {% endfor %}
            {% for (key, value) in constant_props|items if key != \"const_test\" %}
            {{ key }}: {{ value }}
            {% endfor %}
        "};
        let template = DocumentTemplate {
            node_template: Some(node_template.to_owned()),
            graph_template: None,
            edge_template: None,
        };

        let mut docs = template.node(&graph.node("node1").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            node node1 is an unknown entity with the following props:
            temp_test:
             - changed to value_at_0 at 0
             - changed to value_at_1 at 1
            key1: value1
            key2: value2
            key1: value1
            key2: value2
        "};
        assert_eq!(&rendered, expected);

        let mut docs = template.node(&graph.node("node2").unwrap());
        let rendered = docs.next().unwrap().content;
        let expected = indoc! {"
            node node2 is a person with the following props:
            const_test: const_test_value"};
        assert_eq!(&rendered, expected);
    }
}
