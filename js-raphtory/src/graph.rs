use std::collections::HashMap;
use std::convert::TryFrom;

use js_sys::Array;
use js_sys::Object;
use raphtory::core::tgraph_shard::errors::GraphError;
use raphtory::core::Prop;
use raphtory::db::graph::Graph as TGraph;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::log;

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

enum JsVertex {
    Str(String),
    Number(u64),
}

impl TryFrom<JsValue> for JsVertex {
    type Error = JSError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_string() {
            return Ok(JsVertex::Str(value.as_string().unwrap()));
        } else {
            let num = js_sys::Number::from(value);
            if let Some(number) = num.as_f64() {
                if !number.is_nan() && number.fract() == 0.0 {
                    return Ok(JsVertex::Number(number as u64));
                }
            }
        }
        Err(JSError(GraphError::VertexIdNotStringOrNumber))
    }
}

#[wasm_bindgen]
impl Graph {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Graph { g: TGraph::new(1) }
    }

    #[wasm_bindgen(js_name = addVertex)]
    pub fn add_vertex(&self, t: i64, id: JsValue, js_props: Object) -> Result<(), JSError> {
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
            JsVertex::Str(vertex) => self.g.add_vertex(t, vertex, &rust_props).map_err(JSError),
            JsVertex::Number(vertex) => self.g.add_vertex(t, vertex, &rust_props).map_err(JSError),
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
            (JsVertex::Str(src), JsVertex::Str(dst)) => {
                self.g.add_edge(t, src, dst, &props, None).map_err(JSError)
            }
            (JsVertex::Number(src), JsVertex::Number(dst)) => {
                self.g.add_edge(t, src, dst, &props, None).map_err(JSError)
            }
            _ => Err(JSError(GraphError::VertexIdNotStringOrNumber)),
        }
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string(&self) -> String {
        format!("{:?}", self.g)
    }
}
