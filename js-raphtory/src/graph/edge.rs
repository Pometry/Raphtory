use super::Graph;
use crate::graph::{misc::JsProp, vertex::Vertex, UnderGraph};
use raphtory::db::{
    api::view::*,
    graph::{edge::EdgeView, graph::Graph as TGraph},
};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Edge(pub(crate) EdgeView<Graph>);

impl From<EdgeView<TGraph>> for Edge {
    fn from(value: EdgeView<TGraph>) -> Self {
        let graph = value.graph;
        let eref = value.edge;
        let js_graph = Graph(UnderGraph::TGraph(Arc::new(graph)));
        Edge(EdgeView {
            graph: js_graph,
            edge: eref,
        })
    }
}

#[wasm_bindgen]
impl Edge {
    #[wasm_bindgen(js_name = source)]
    pub fn src(&self) -> Vertex {
        Vertex(self.0.src())
    }

    #[wasm_bindgen(js_name = destination)]
    pub fn dst(&self) -> Vertex {
        Vertex(self.0.dst())
    }

    #[wasm_bindgen(js_name = properties)]
    pub fn properties(&self) -> js_sys::Map {
        let t_props = self.0.properties();
        let obj = js_sys::Map::new();
        for (k, v) in t_props.iter() {
            obj.set(&k.to_string().into(), &JsProp(v).into());
        }
        obj
    }

    #[wasm_bindgen(js_name = getProperty)]
    pub fn get_property(&self, name: String) -> JsValue {
        if let Some(prop) = self.0.properties().get(&name).map(JsProp) {
            prop.into()
        } else {
            JsValue::NULL
        }
    }
}
