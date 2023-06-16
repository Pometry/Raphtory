use raphtory::db::edge::EdgeView;
use raphtory::db::view_api::*;
use wasm_bindgen::prelude::*;

use super::Graph;
use crate::graph::{misc::JsProp, vertex::Vertex};

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
        let obj = js_sys::Map::new();
        for (k, v) in self.0.properties(true) {
            obj.set(&k.into(), &JsProp(v).into());
        }
        obj
    }

    #[wasm_bindgen(js_name = getProperty)]
    pub fn get_property(&self, name: String) -> JsValue {
        if let Some(prop) = self.0.property(&name, true).map(JsProp) {
            prop.into()
        } else {
            JsValue::NULL
        }
    }
}
