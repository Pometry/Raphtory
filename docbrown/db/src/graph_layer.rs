use crate::view_api::internal::GraphViewInternalOps;
use docbrown_core::{
    tgraph::{EdgeRef, VertexRef},
    tgraph_shard::errors::GraphError,
    Direction, Prop,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct LayeredGraph<G: GraphViewInternalOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layer: usize,
}

impl<G: GraphViewInternalOps> LayeredGraph<G> {
    pub fn new(graph: G, layer: usize) -> Self {
        Self { graph, layer }
    }

    /// Return None if the intersection between the previously requested layers and the layer of
    /// this view is null
    fn constrain(&self, layer: Option<usize>) -> Option<usize> {
        match layer {
            None => Some(self.layer),
            Some(layer) if layer == self.layer => Some(layer),
            _ => None,
        }
    }
}

impl<G: GraphViewInternalOps> GraphViewInternalOps for LayeredGraph<G> {
    fn get_layer(&self, key: Option<&str>) -> Option<usize> {
        self.graph.get_layer(key)
    }

    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.earliest_time_window(t_start, t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph.latest_time_global()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.latest_time_window(t_start, t_end)
    }

    fn vertices_len(&self) -> usize {
        self.graph.vertices_len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph.vertices_len_window(t_start, t_end)
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.edges_len(Some(layer)))
            .unwrap_or(0)
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.edges_len_window(t_start, t_end, Some(layer)))
            .unwrap_or(0)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        // FIXME: there is something wrong here, the layer should be able to be None, which would mean, whatever layer this is
        layer == self.layer && self.graph.has_edge_ref(src, dst, layer)
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool {
        layer == self.layer
            && self
                .graph
                .has_edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph.has_vertex_ref(v)
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.graph.has_vertex_ref_window(v, t_start, t_end)
    }

    fn degree(&self, v: VertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.degree(v, d, Some(layer)))
            .unwrap_or(0)
    }

    fn degree_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.degree_window(v, t_start, t_end, d, Some(layer)))
            .unwrap_or(0)
    }

    fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
        self.graph.vertex_ref(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
        self.graph.vertex_ref_window(v, t_start, t_end)
    }

    fn vertex_earliest_time(&self, v: VertexRef) -> Option<i64> {
        self.graph.vertex_earliest_time(v)
    }

    fn vertex_earliest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.vertex_earliest_time_window(v, t_start, t_end)
    }

    fn vertex_latest_time(&self, v: VertexRef) -> Option<i64> {
        self.graph.vertex_latest_time(v)
    }

    fn vertex_latest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.vertex_latest_time_window(v, t_start, t_end)
    }

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids()
    }

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(t_start, t_end)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs()
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_window(t_start, t_end)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_shard(shard)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_window_shard(shard, t_start, t_end)
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        (layer == self.layer)
            .then(|| self.graph.edge_ref(src, dst, layer))
            .flatten()
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef> {
        (layer == self.layer)
            .then(|| self.graph.edge_ref_window(src, dst, t_start, t_end, layer))
            .flatten()
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        // TODO: create a function empty_iter which returns a boxed empty iterator so we use it in all these functions
        self.constrain(layer)
            .map(|layer| self.graph.edge_refs(Some(layer)))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.edge_refs_window(t_start, t_end, Some(layer)))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn vertex_edges_all_layers(
        &self,
        v: VertexRef,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.vertex_edges_single_layer(v, d, self.layer)
    }

    fn vertex_edges_single_layer(
        &self,
        v: VertexRef,
        d: Direction,
        layer: usize,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        if layer == self.layer {
            self.graph.vertex_edges_single_layer(v, d, layer)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn vertex_edges_t(
        &self,
        v: VertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.vertex_edges_t(v, d, Some(layer)))
            .unwrap_or(Box::new(std::iter::empty()))
    }

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.constrain(layer)
            .map(|layer| {
                self.graph
                    .vertex_edges_window(v, t_start, t_end, d, Some(layer))
            })
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.constrain(layer)
            .map(|layer| {
                self.graph
                    .vertex_edges_window_t(v, t_start, t_end, d, Some(layer))
            })
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn neighbours(
        &self,
        v: VertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.neighbours(v, d, Some(layer)))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.constrain(layer)
            .map(|layer| {
                self.graph
                    .neighbours_window(v, t_start, t_end, d, Some(layer))
            })
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn neighbours_ids(
        &self,
        v: VertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.neighbours_ids(v, d, Some(layer)))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.constrain(layer)
            .map(|layer| {
                self.graph
                    .neighbours_ids_window(v, t_start, t_end, d, Some(layer))
            })
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.graph.static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.graph.temporal_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props(v)
    }

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props_window(v, t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.graph.static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.static_edge_prop_names(e)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.temporal_edge_prop_names(e)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_props_vec(e, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_props_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_edge_props(e)
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_edge_props_window(e, t_start, t_end)
    }

    fn num_shards(&self) -> usize {
        self.graph.num_shards()
    }

    fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertices_shard(shard_id)
    }

    fn vertices_shard_window(
        &self,
        shard_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertices_shard_window(shard_id, t_start, t_end)
    }
}
