use super::datetimeformat::datetimeformat;
use raphtory::{db::api::properties::{internal::InternalPropertiesOps, TemporalPropertyView}, db::graph::edge::EdgeView, db::graph::node::NodeView, prelude::*};
use minijinja::{
    value::{Enumerator, Object},
    Environment, Template, Value,
};
use raphtory_api::core::storage::{
    arc_str::{ArcStr, OptionAsStr},
    timeindex::EventTime,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

#[derive(Debug)]
struct PropUpdate {
    time: EventTime,
    value: Value,
}

fn temporal_prop_value<P: InternalPropertiesOps + Clone>(value: TemporalPropertyView<P>) -> Value {
    value
        .iter()
        .map(|(time, value)| PropUpdate {
            time,
            value: value.into(),
        })
        .map(Value::from_object)
        .collect()
}

impl Object for PropUpdate {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "time" => Some(Value::from(self.time.0)),
            "value" => Some(self.value.clone()),
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Values(vec![self.time.0.into(), self.value.clone()])
    }
}

#[derive(Serialize)]
struct NodeTemplateContext {
    name: String,
    node_type: Option<ArcStr>,
    properties: Value,
    metadata: Value,
    temporal_properties: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<NodeView<'graph, G>> for NodeTemplateContext {
    fn from(value: NodeView<'graph, G>) -> Self {
        Self {
            name: value.name(),
            node_type: value.node_type(),
            properties: value
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            metadata: value
                .metadata()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            temporal_properties: value
                .properties()
                .temporal()
                .iter()
                .map(|(key, prop)| (key.to_string(), temporal_prop_value(prop)))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DocumentTemplate {
    pub node_template: Option<String>,
    pub edge_template: Option<String>,
}

impl DocumentTemplate {
    /// A function that translate a node into an iterator of documents
    #[doc(hidden)] // pub for raphtory-tests
    pub fn node<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeView<'graph, G>,
    ) -> Option<String> {
        let template = self.node_template.as_str()?;
        let mut env = Environment::new();
        let template = build_template(&mut env, template);
        match template.render(NodeTemplateContext::from(node.clone())) {
            Ok(mut document) => {
                truncate(&mut document);
                Some(document)
            }
            Err(error) => {
                let node = node.name();
                error!("Template render failed for a node {node}, skipping: {error}");
                None
            }
        }
    }

    /// A function that translate an edge into an iterator of documents
    #[doc(hidden)] // pub for raphtory-tests
    pub fn edge<'graph, G: GraphViewOps<'graph>>(&self, edge: EdgeView<G>) -> Option<String> {
        let template = self.edge_template.as_str()?;
        let mut env = Environment::new();
        let template = build_template(&mut env, template);
        match template.render(EdgeTemplateContext::from(edge.clone())) {
            Ok(mut document) => {
                truncate(&mut document);
                Some(document)
            }
            Err(error) => {
                let src = edge.src().name();
                let dst = edge.dst().name();
                error!("Template render failed for edge {src}->{dst}, skipping: {error}");
                None
            }
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
    metadata: Value,
    temporal_properties: Value,
}

impl<'graph, G: GraphViewOps<'graph>> From<EdgeView<G>> for EdgeTemplateContext {
    fn from(value: EdgeView<G>) -> Self {
        Self {
            src: value.src().into(),
            dst: value.dst().into(),
            history: value.history().t().collect(),
            layers: value
                .layer_names()
                .into_iter()
                .map(|name| name.into())
                .collect(),
            properties: value // FIXME: boilerplate all over the place
                .properties()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            metadata: value
                .metadata()
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            temporal_properties: value
                .properties()
                .temporal()
                .iter()
                .map(|(key, prop)| (key.to_string(), temporal_prop_value(prop)))
                .collect(),
        }
    }
}

pub const DEFAULT_NODE_TEMPLATE: &str = "Node {{ name }}{% if node_type is none %} has the following properties:{% else %} is a {{ node_type }} with the following properties:{% endif %}

{% for (key, value) in metadata|items %}
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
