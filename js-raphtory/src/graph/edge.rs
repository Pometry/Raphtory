use raphtory::db::edge::EdgeView;
use wasm_bindgen::prelude::*;

use super::Graph;


#[wasm_bindgen]
pub struct Edge(pub(crate) EdgeView<Graph>);

