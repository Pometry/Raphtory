use std::collections::HashMap;

use js_sys::Array;
use js_sys::Object;
use raphtory::core::tgraph_shard::errors::GraphError;
use raphtory::core::Prop;
use raphtory::db::graph::Graph as TGraph;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen]
#[repr(transparent)]
pub struct Graph {
    g: TGraph,
}

impl From<TGraph> for Graph {
    fn from(g: TGraph) -> Self {
        Graph { g }
    }
}

#[wasm_bindgen]
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
        Graph { g: TGraph::new(1) }
    }

    #[wasm_bindgen(js_name = addVertex)]
    pub fn add_vertex(&self, t: i64, id: u64, js_props: Object) -> Result<(), JSError> {
        if js_props.is_string() {
            let props: Vec<(String, Prop)> =
                vec![("name".to_string(), Prop::Str(js_props.as_string().unwrap()))];
            return self.g.add_vertex(t, id, &props).map_err(JSError);
        } else if js_props.is_object() {
            let props: Vec<(String, Prop)> = Object::entries(&js_props)
                .iter()
                .filter_map(|entry| {
                    let prop: Option<(String, Prop)> = JsObjectEntry(entry.clone()).into();
                    prop
                })
                .collect();

            self.g.add_vertex(t, id, &props).map_err(JSError)
        } else {
            self.g.add_vertex(t, id, &vec![]).map_err(JSError)
        }
    }

    #[wasm_bindgen(js_name = addEdge)]
    pub fn add_edge(&self, t: i64, src: u64, dst: u64, js_props: Object) -> Result<(), JSError> {
        if js_props.is_bigint() {
            let props: Vec<(String, Prop)> =
                vec![("weight".to_string(), Prop::F64(js_props.as_f64().unwrap()))];
            return self.g.add_edge(t, src, dst, &props, None).map_err(JSError);
        } else if js_props.is_object() {
            let props: Vec<(String, Prop)> = Object::entries(&js_props)
                .iter()
                .filter_map(|entry| {
                    let prop: Option<(String, Prop)> = JsObjectEntry(entry.clone()).into();
                    prop
                })
                .collect();

            self.g.add_edge(t, src, dst, &props, None).map_err(JSError)
        } else {
            self.g.add_edge(t, src, dst, &vec![], None).map_err(JSError)
        }
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string(&self) -> String {
        format!("{:?}", self.g)
    }

}
