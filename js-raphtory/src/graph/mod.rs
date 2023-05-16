extern crate console_error_panic_hook;

use core::panic;
use std::convert::TryFrom;
use std::sync::Arc;

use js_sys::Array;
use js_sys::Object;
use raphtory::core::tgraph_shard::errors::GraphError;
use raphtory::core::Prop;
use raphtory::db::graph::Graph as TGraph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::view_api::internal::GraphViewInternalOps;
use raphtory::db::view_api::GraphViewOps;
use raphtory::db::view_api::TimeOps;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::graph::vertex::JsVertex;
use crate::graph::vertex::Vertex;
use crate::log;

mod edge;
mod graph_view_impl;
mod vertex;

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
    pub fn graph(&self) -> Box<Arc<dyn GraphViewInternalOps + Send + Sync + 'static>> {
        match self {
            UnderGraph::TGraph(g) => Box::new(g.clone()),
            UnderGraph::WindowedGraph(g) => Box::new(g.clone()),
        }
    }
}

#[wasm_bindgen]
#[repr(transparent)]
#[derive(Clone)]
pub struct Graph(UnderGraph);

#[wasm_bindgen]
#[derive(Debug)]
pub struct JSError(GraphError);

struct JsObjectEntry(JsValue);

impl From<JsObjectEntry> for Option<(String, Prop)> {
    fn from(entry: JsObjectEntry) -> Self {
        let arr: Array = entry.0.into();

        let key = arr.at(0).as_string().unwrap();
        let value = arr.at(1).as_string().unwrap();
        Some((key, Prop::Str(value)))
    }
}

#[wasm_bindgen]
impl Graph {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        Graph(UnderGraph::TGraph(Arc::new(TGraph::new(1))))
    }

    #[wasm_bindgen(js_name = window)]
    pub fn window(&self, t_start: i64, t_end: i64) -> Self {
        match &self.0 {
            UnderGraph::TGraph(g) => Graph(UnderGraph::WindowedGraph(Arc::new(
                g.window(t_start, t_end),
            ))),
            UnderGraph::WindowedGraph(g) => {
                // take the largest of g.start() and t_start
                // and the smallest of g.end and t_end
                // and apply the window to the parent graph
                let t_start = std::cmp::max(g.start(), Some(t_start));
                let t_end = std::cmp::min(g.end(), Some(t_end));

                Graph(UnderGraph::WindowedGraph(Arc::new(
                    g.graph.window(t_start.unwrap(), t_end.unwrap()),
                )))
            }
        }
    }

    #[wasm_bindgen(js_name = getVertex)]
    pub fn get_vertex(&self, id: JsValue) -> Option<Vertex> {
        let vertex_view = match JsVertex::try_from(id).ok()? {
            JsVertex::Str(vertex) => self.vertex(vertex),
            JsVertex::Number(vertex) => self.vertex(vertex),
        };

        vertex_view.map(Vertex)
    }

    #[wasm_bindgen(js_name = addVertex)]
    pub fn add_vertex_js(&self, t: i64, id: JsValue, js_props: Object) -> Result<(), JSError> {
        let rust_props = if js_props.is_string() {
            vec![("name".to_string(), Prop::Str(js_props.as_string().unwrap()))]
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

        match JsVertex::try_from(id)? {
            JsVertex::Str(vertex) => self
                .mutable_graph()
                .add_vertex(t, vertex, &rust_props)
                .map_err(JSError),
            JsVertex::Number(vertex) => self
                .mutable_graph()
                .add_vertex(t, vertex, &rust_props)
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
    ) -> Result<(), JSError> {
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

        match (JsVertex::try_from(src)?, JsVertex::try_from(dst)?) {
            (JsVertex::Str(src), JsVertex::Str(dst)) => self
                .mutable_graph()
                .add_edge(t, src, dst, &props, None)
                .map_err(JSError),
            (JsVertex::Number(src), JsVertex::Number(dst)) => self
                .mutable_graph()
                .add_edge(t, src, dst, &props, None)
                .map_err(JSError),
            _ => Err(JSError(GraphError::VertexIdNotStringOrNumber)),
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
    fn one() {
        assert_eq!(1, 2)
    }
    #[wasm_bindgen_test]
    fn add_one_edge_get_neighbours() -> Result<(), super::JSError> {
        let graph = super::Graph::new();

        graph.add_vertex_js(2, "Bob".into(), Object::new())?;
        graph.add_vertex_js(3, "Alice".into(), Object::new())?;

        let js_weight = JsValue::from_f64(3.14);

        graph.add_edge(9, "Bob".into(), "Alice".into(), js_weight.into())?;
        Ok(())
    }
}
