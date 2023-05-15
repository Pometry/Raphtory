use core::panic;
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Range;
use std::sync::Arc;

use js_sys::Array;
use js_sys::Object;
use raphtory::core::tgraph::EdgeRef;
use raphtory::core::tgraph::VertexRef;
use raphtory::core::tgraph_shard::errors::GraphError;
use raphtory::core::Prop;
use raphtory::db::graph::Graph as TGraph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::GraphViewInternalOps;
use raphtory::db::view_api::GraphViewOps;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::log;

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

impl From<TGraph> for Graph {
    fn from(g: TGraph) -> Self {
        Graph(UnderGraph::TGraph(Arc::new(g)))
    }
}

impl From<WindowedGraph<TGraph>> for Graph {
    fn from(g: WindowedGraph<TGraph>) -> Self {
        Graph(UnderGraph::WindowedGraph(Arc::new(g)))
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
pub struct Vertex(VertexView<Graph>);

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
pub enum Direction {
    Out = 0,
    In = 1,
    Both = 2,
}

#[wasm_bindgen]
impl Graph {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Graph(UnderGraph::TGraph(Arc::new(TGraph::new(1))))
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

impl GraphViewInternalOps for Graph {
    fn get_unique_layers_internal(&self) -> Vec<String> {
        self.0.graph().get_unique_layers_internal()
    }

    fn get_layer(&self, key: Option<&str>) -> Option<usize> {
        self.0.graph().get_layer(key)
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.0.graph().get_layer_name_by_id(layer_id)
    }

    fn view_start(&self) -> Option<i64> {
        self.0.graph().view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.0.graph().view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.0.graph().earliest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.graph().earliest_time_window(t_start, t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.0.graph().latest_time_global()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.graph().latest_time_window(t_start, t_end)
    }

    fn vertices_len(&self) -> usize {
        self.0.graph().vertices_len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.0.graph().vertices_len_window(t_start, t_end)
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.0.graph().edges_len(layer)
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        self.0.graph().edges_len_window(t_start, t_end, layer)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.0.graph().has_edge_ref(src, dst, layer)
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool {
        self.0
            .graph()
            .has_edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.0.graph().has_vertex_ref(v)
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.0.graph().has_vertex_ref_window(v, t_start, t_end)
    }

    fn degree(&self, v: VertexRef, d: raphtory::core::Direction, layer: Option<usize>) -> usize {
        self.0.graph().degree(v, d, layer)
    }

    fn vertex_timestamps(&self, v: VertexRef) -> Vec<i64> {
        self.0.graph().vertex_timestamps(v)
    }

    fn vertex_timestamps_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Vec<i64> {
        self.0.graph().vertex_timestamps_window(v, t_start, t_end)
    }

    fn edge_timestamps(&self, e: EdgeRef, window: Option<Range<i64>>) -> Vec<i64> {
        self.0.graph().edge_timestamps(e, window)
    }

    fn degree_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> usize {
        self.0.graph().degree_window(v, t_start, t_end, d, layer)
    }

    fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
        self.0.graph().vertex_ref(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
        self.0.graph().vertex_ref_window(v, t_start, t_end)
    }

    fn vertex_earliest_time(&self, v: VertexRef) -> Option<i64> {
        self.0.graph().vertex_earliest_time(v)
    }

    fn vertex_earliest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.0
            .graph()
            .vertex_earliest_time_window(v, t_start, t_end)
    }

    fn vertex_latest_time(&self, v: VertexRef) -> Option<i64> {
        self.0.graph().vertex_latest_time(v)
    }

    fn vertex_latest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.graph().vertex_latest_time_window(v, t_start, t_end)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.graph().vertex_refs()
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.graph().vertex_refs_window(t_start, t_end)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.graph().vertex_refs_shard(shard)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0
            .graph()
            .vertex_refs_window_shard(shard, t_start, t_end)
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        self.0.graph().edge_ref(src, dst, layer)
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef> {
        self.0
            .graph()
            .edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().edge_refs(layer)
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().edge_refs_window(t_start, t_end, layer)
    }

    fn vertex_edges_t(
        &self,
        v: VertexRef,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().vertex_edges_t(v, d, layer)
    }

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0
            .graph()
            .vertex_edges_window(v, t_start, t_end, d, layer)
    }

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0
            .graph()
            .vertex_edges_window_t(v, t_start, t_end, d, layer)
    }

    fn neighbours(
        &self,
        v: VertexRef,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.graph().neighbours(v, d, layer)
    }

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0
            .graph()
            .neighbours_window(v, t_start, t_end, d, layer)
    }

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop> {
        self.0.graph().static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.0.graph().static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.0.graph().temporal_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)> {
        self.0.graph().temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.0
            .graph()
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.graph().temporal_vertex_props(v)
    }

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0
            .graph()
            .temporal_vertex_props_window(v, t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.0.graph().static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.0.graph().static_edge_prop_names(e)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.0.graph().temporal_edge_prop_names(e)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.0.graph().temporal_edge_props_vec(e, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.0
            .graph()
            .temporal_edge_props_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.graph().temporal_edge_props(e)
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.graph().temporal_edge_props_window(e, t_start, t_end)
    }

    fn num_shards(&self) -> usize {
        self.0.graph().num_shards()
    }

    fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.graph().vertices_shard(shard_id)
    }

    fn vertices_shard_window(
        &self,
        shard_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0
            .graph()
            .vertices_shard_window(shard_id, t_start, t_end)
    }

    fn vertex_edges(
        &self,
        v: VertexRef,
        d: raphtory::core::Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().vertex_edges(v, d, layer)
    }

    fn lookup_by_pid_and_shard(&self, pid: usize, shard: usize) -> Option<VertexRef> {
        self.0.graph().lookup_by_pid_and_shard(pid, shard)
    }
}
