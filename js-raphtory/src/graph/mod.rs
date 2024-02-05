#![allow(dead_code)]
#[cfg(feature = "console_error_panic_hook")]
extern crate console_error_panic_hook;

use core::panic;
use js_sys::Object;
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::view::{BoxableGraphView, TimeOps},
        graph::{graph::Graph as TGraph, views::window_graph::WindowedGraph},
    },
    prelude::*,
};
use std::{convert::TryFrom, sync::Arc};
use wasm_bindgen::{prelude::*, JsCast};

use crate::{
    graph::{
        edge::Edge,
        misc::{JSError, JsObjectEntry},
        node::{JsNode, Node},
    },
    log,
    utils::set_panic_hook,
};

mod edge;
mod graph_view_impl;
mod misc;
mod node;

#[wasm_bindgen]
#[repr(transparent)]
#[derive(Clone)]
pub struct Graph(UnderGraph);

#[derive(Clone, Debug)]
enum UnderGraph {
    TGraph(Arc<TGraph>),
    WindowedGraph(Arc<WindowedGraph<TGraph>>),
}

impl UnderGraph {
    pub fn mutable_graph(&self) -> &TGraph {
        match self {
            UnderGraph::TGraph(g) => g,
            UnderGraph::WindowedGraph(g) => &g.graph,
        }
    }

    // a bit heavy but might work
    pub fn graph(&self) -> Box<Arc<dyn BoxableGraphView>> {
        match self {
            UnderGraph::TGraph(g) => Box::new(g.clone()),
            UnderGraph::WindowedGraph(g) => Box::new(g.clone()),
        }
    }
}

#[wasm_bindgen]
impl Graph {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        set_panic_hook();
        Graph(UnderGraph::TGraph(Arc::new(TGraph::new())))
    }

    #[wasm_bindgen(js_name = window)]
    pub fn window(&self, start: i64, end: i64) -> Self {
        match &self.0 {
            UnderGraph::TGraph(g) => {
                Graph(UnderGraph::WindowedGraph(Arc::new(g.window(start, end))))
            }
            UnderGraph::WindowedGraph(g) => {
                // take the largest of g.start() and start
                // and the smallest of g.end and end
                // and apply the window to the parent graph
                let start = std::cmp::max(g.start(), Some(start));
                let end = std::cmp::min(g.end(), Some(end));

                Graph(UnderGraph::WindowedGraph(Arc::new(
                    g.graph.window(start.unwrap(), end.unwrap()),
                )))
            }
        }
    }

    #[wasm_bindgen(js_name = getNode)]
    pub fn get_node(&self, id: JsValue) -> Option<Node> {
        let node_view = match JsNode::try_from(id).ok()? {
            JsNode::Str(node) => self.node(node),
            JsNode::Number(node) => self.node(node),
        };

        node_view.map(Node)
    }

    #[wasm_bindgen(js_name = addNode)]
    pub fn add_node_js(
        &self,
        t: i64,
        id: JsValue,
        js_props: Object,
        node_type: JsValue,
    ) -> Result<Node, JSError> {
        let rust_props = if js_props.is_string() {
            vec![("name".to_string(), Prop::str(js_props.as_string().unwrap()))]
        } else if js_props.is_object() {
            Object::entries(&js_props)
                .iter()
                .filter_map(|entry| {
                    let prop: Option<(String, Prop)> = JsObjectEntry(entry.clone()).into();
                    prop
                })
                .collect()
        } else {
            vec![]
        };

        let opt_string = node_type.as_string();
        let node_type_rust: Option<&str> = opt_string.as_deref().map_or(Some("_default"), Some);

        match JsNode::try_from(id)? {
            JsNode::Str(node) => self
                .mutable_graph()
                .add_node(t, node, rust_props, node_type_rust)
                .map(|v| v.into())
                .map_err(JSError),
            JsNode::Number(node) => self
                .mutable_graph()
                .add_node(t, node, rust_props, node_type_rust)
                .map(|v| v.into())
                .map_err(JSError),
        }
    }

    #[wasm_bindgen(js_name = addEdge)]
    pub fn add_edge(
        &self,
        t: i64,
        src: JsValue,
        dst: JsValue,
        js_props: Object,
    ) -> Result<Edge, JSError> {
        js_props.dyn_ref::<js_sys::BigInt>().map(|bigint| {
            log(&format!("bigint: {:?}", bigint));
        });

        let props = if js_props.is_bigint() {
            vec![("weight".to_string(), Prop::F64(js_props.as_f64().unwrap()))]
        } else if js_props.is_object() {
            Object::entries(&js_props)
                .iter()
                .filter_map(|entry| {
                    let prop: Option<(String, Prop)> = JsObjectEntry(entry.clone()).into();
                    prop
                })
                .collect()
        } else {
            vec![]
        };

        match (JsNode::try_from(src)?, JsNode::try_from(dst)?) {
            (JsNode::Str(src), JsNode::Str(dst)) => self
                .mutable_graph()
                .add_edge(t, src, dst, props, None)
                .map_err(JSError)
                .map(|e| e.into()),
            (JsNode::Number(src), JsNode::Number(dst)) => self
                .mutable_graph()
                .add_edge(t, src, dst, props, None)
                .map_err(JSError)
                .map(|e| e.into()),
            _ => Err(JSError(GraphError::NodeIdNotStringOrNumber)),
        }
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string(&self) -> String {
        format!("{:?}", self.mutable_graph())
    }

    fn mutable_graph(&self) -> &TGraph {
        self.0.mutable_graph()
    }
}

#[cfg(test)]
mod js_test {
    use super::*;
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn add_one_edge_get_neighbours() -> Result<(), super::JSError> {
        let graph = super::Graph::new();

        graph.add_node_js(2, "Bob".into(), Object::new(), JsValue::from("_default"))?;
        graph.add_node_js(3, "Alice".into(), Object::new(), JsValue::from("_default"))?;

        let js_weight = JsValue::from_f64(3.14);

        graph.add_edge(9, "Bob".into(), "Alice".into(), js_weight.into())?;
        Ok(())
    }
}
