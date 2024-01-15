use super::{misc::JSError, Graph};
use crate::graph::{edge::Edge, misc::JsProp, UnderGraph};
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::view::NodeViewOps,
        graph::{graph::Graph as TGraph, node::NodeView},
    },
};
use std::{convert::TryFrom, sync::Arc};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Node(pub(crate) NodeView<Graph>);

impl From<NodeView<TGraph>> for Node {
    fn from(value: NodeView<TGraph>) -> Self {
        let vid = value.node;
        let graph = value.graph;
        let js_graph = Graph(UnderGraph::TGraph(Arc::new(graph)));
        Node(NodeView::new_internal(js_graph, vid))
    }
}

#[wasm_bindgen]
impl Node {
    #[wasm_bindgen(js_name = id)]
    pub fn id(&self) -> u64 {
        self.0.id()
    }

    #[wasm_bindgen(js_name = name)]
    pub fn name(&self) -> String {
        self.0.name().to_string()
    }

    #[wasm_bindgen(js_name = outDegree)]
    pub fn out_degree(&self) -> usize {
        self.0.out_degree()
    }

    #[wasm_bindgen(js_name = inDegree)]
    pub fn in_degree(&self) -> usize {
        self.0.in_degree()
    }

    #[wasm_bindgen(js_name = degree)]
    pub fn degree(&self) -> usize {
        self.0.in_degree()
    }

    #[wasm_bindgen(js_name = neighbours)]
    pub fn neighbours(&self) -> js_sys::Array {
        self.0
            .neighbours()
            .iter()
            .map(Node)
            .map(JsValue::from)
            .collect()
    }

    #[wasm_bindgen(js_name = outNeighbours)]
    pub fn out_neighbours(&self) -> js_sys::Array {
        self.0
            .out_neighbours()
            .iter()
            .map(Node)
            .map(JsValue::from)
            .collect()
    }

    #[wasm_bindgen(js_name = inNeighbours)]
    pub fn in_neighbours(&self) -> js_sys::Array {
        self.0
            .in_neighbours()
            .iter()
            .map(Node)
            .map(JsValue::from)
            .collect()
    }

    #[wasm_bindgen(js_name = edges)]
    pub fn edges(&self) -> js_sys::Array {
        self.0.edges().iter().map(Edge).map(JsValue::from).collect()
    }

    // out_edges
    #[wasm_bindgen(js_name = outEdges)]
    pub fn out_edges(&self) -> js_sys::Array {
        self.0
            .out_edges()
            .iter()
            .map(Edge)
            .map(JsValue::from)
            .collect()
    }

    // in_edges
    #[wasm_bindgen(js_name = inEdges)]
    pub fn in_edges(&self) -> js_sys::Array {
        self.0
            .in_edges()
            .iter()
            .map(Edge)
            .map(JsValue::from)
            .collect()
    }

    #[wasm_bindgen(js_name = properties)]
    pub fn properties(&self) -> js_sys::Map {
        let obj = js_sys::Map::new();
        for (k, v) in self.0.properties() {
            obj.set(&k.to_string().into(), &JsProp(v).into());
        }
        obj
    }

    #[wasm_bindgen(js_name = getProperty)]
    pub fn get_property(&self, name: String) -> JsValue {
        self.0
            .properties()
            .get(&name)
            .map(|v| JsProp(v).into())
            .unwrap_or(JsValue::NULL)
    }
}

pub enum JsNode {
    Str(String),
    Number(u64),
}

impl TryFrom<JsValue> for JsNode {
    type Error = JSError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_string() {
            return Ok(JsNode::Str(value.as_string().unwrap()));
        } else {
            let num = js_sys::Number::from(value);
            if let Some(number) = num.as_f64() {
                if !number.is_nan() && number.fract() == 0.0 {
                    return Ok(JsNode::Number(number as u64));
                }
            }
        }
        Err(JSError(GraphError::NodeIdNotStringOrNumber))
    }
}
