use indoc::indoc;
use minijinja::{
    value::{Enumerator, Object},
    Environment, Value,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::Serialize;
use std::sync::Arc;

use crate::{
    core::Prop,
    db::{
        api::properties::TemporalPropertyView,
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
};

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
    // test: Value,
    constant_props: Value,
    temporal_props: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<NodeView<G>> for NodeTemplateContext {
    fn from(value: NodeView<G>) -> Self {
        Self {
            name: value.name(),
            // node_type: value.node_type().unwrap_or(ArcStr::from("")),
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

// FIXME: this is eagerly allocating a lot of stuff eagerly, we should implement Object instead for Prop
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

// enum NodeTemplate {
//     Property(String), // I guess this is just a subcase of the other
//     Jinja(String),
// }

// impl NodeTemplate {
//     pub(crate) fn render<'graph, G: GraphViewOps<'graph>>(
//         &self,
//         node: NodeView<G>,
//     ) -> Option<String> {
//         match self {
//             Self::Property(name) => node.properties().get(name).map(|prop| prop.to_string()),
//             Self::Jinja(template) => {
//                 let mut env = Environment::new();
//                 env.add_template("template", template).unwrap();
//                 let tmpl = env.get_template("template").unwrap();
//                 Some(tmpl.render(NodeTemplateContext::from(node)).unwrap())
//             }
//         }
//     }
// }

struct NodeTemplate(String);

impl NodeTemplate {
    pub(crate) fn render<'graph, G: GraphViewOps<'graph>>(&self, node: NodeView<G>) -> String {
        let mut env = Environment::new();
        // it's important adding these settings
        env.set_trim_blocks(true);
        env.set_lstrip_blocks(true);
        // before adding any template
        env.add_template("template", &self.0).unwrap();
        println!("trimblocks ->{}", env.trim_blocks());
        let tmpl = env.get_template("template").unwrap();
        tmpl.render(NodeTemplateContext::from(node)).unwrap() // FIXME: this unwrap?
    }
}

#[derive(Serialize)]
struct EdgeTemplateContext {
    src: NodeTemplateContext,
    dst: NodeTemplateContext,
    layers: Vec<String>,
    props: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<EdgeView<G>> for EdgeTemplateContext {
    fn from(value: EdgeView<G>) -> Self {
        Self {
            src: value.src().into(),
            dst: value.dst().into(),
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

#[derive(Serialize)]
struct EdgeLayerTemplateContext {
    src: NodeTemplateContext,
    dst: NodeTemplateContext,
    layer: String,
    props: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<EdgeView<G>> for Vec<EdgeLayerTemplateContext> {
    fn from(value: EdgeView<G>) -> Self {
        value
            .explode_layers()
            .iter()
            .map(|edge| EdgeLayerTemplateContext {
                src: value.src().into(),
                dst: value.dst().into(),
                layer: edge.layer_name().unwrap().into(),
                props: edge // FIXME: boilerplate
                    .properties()
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.clone()))
                    .collect(),
            })
            .collect()
    }
}

#[cfg(test)]
mod template_tests {
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
        // node1.add_constant_properties([
        //     ("key1", "value1"),
        //     ("iter2", "value2"),
        //     ("iter3", "value3"),
        // ]);

        // I should be able to iteate over props without doing props|items, which would be solved by implementing Object for Properties
        let template_string = indoc! {"
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
        let template = NodeTemplate(template_string.to_owned());

        let rendered = template.render(graph.node("node1").unwrap());
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

        let rendered = template.render(graph.node("node2").unwrap());
        let expected = indoc! {"
            node node2 is a person with the following props:
            const_test: const_test_value"};
        assert_eq!(&rendered, expected);
    }
}
