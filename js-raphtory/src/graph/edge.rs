use super::Graph;
use crate::graph::{misc::JsProp, vertex::Vertex};
use raphtory::db::{api::view::*, graph::edge::EdgeView};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Edge(pub(crate) EdgeView<Graph>);

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
        let s_props = self.0.static_properties();
        let iter = t_props
            .iter()
            .flat_map(|(k, v)| v.latest().or_else(|| s_props.get(&k)).map(|v| (k, v)))
            .chain(s_props.iter().filter(|(k, _)| t_props.get(k).is_none()));
        let obj = js_sys::Map::new();
        for (k, v) in iter {
            obj.set(&k.into(), &JsProp(v).into());
        }
        obj
    }

    #[wasm_bindgen(js_name = getProperty)]
    pub fn get_property(&self, name: String) -> JsValue {
        if let Some(prop) = self
            .0
            .properties()
            .get(&name)
            .and_then(|v| v.latest())
            .or_else(|| self.0.static_properties().get(name))
            .map(JsProp)
        {
            prop.into()
        } else {
            JsValue::NULL
        }
    }
}
