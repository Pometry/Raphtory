//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::core::entities::properties::props::DictMapper;
use crate::core::entities::properties::tprop::TProp;
use crate::db::api::properties::internal::{
    CorePropertiesOps, StaticProperties, StaticPropertiesOps, TemporalProperties,
    TemporalPropertiesOps, TemporalPropertyView, TemporalPropertyViewOps,
};
use crate::db::api::view::internal::Static;
use crate::{
    core::{
        entities::{vertices::vertex_ref::VertexRef, VID},
        utils::time::IntoTime,
        Direction,
    },
    db::{
        api::view::{internal::GraphPropertiesOps, BoxedIter, LayerOps},
        graph::{
            edge::{EdgeList, EdgeView},
            path::{Operations, PathFromVertex},
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct VertexView<G: GraphViewOps> {
    pub graph: G,
    pub vertex: VID,
}

impl<G: GraphViewOps> From<VertexView<G>> for VertexRef {
    fn from(value: VertexView<G>) -> Self {
        VertexRef::Local(value.vertex)
    }
}

impl<G: GraphViewOps> From<&VertexView<G>> for VertexRef {
    fn from(value: &VertexView<G>) -> Self {
        VertexRef::Local(value.vertex)
    }
}

impl<G: GraphViewOps> VertexView<G> {
    /// Creates a new `VertexView` wrapping a vertex reference and a graph, localising any remote vertices to the correct shard.
    pub fn new(graph: G, vertex: VertexRef) -> VertexView<G> {
        match vertex {
            VertexRef::Local(local) => Self::new_local(graph, local),
            _ => {
                let v = graph.localise_vertex_unchecked(vertex);
                VertexView { graph, vertex: v }
            }
        }
    }

    /// Creates a new `VertexView` wrapping a local vertex reference and a graph
    pub fn new_local(graph: G, vertex: VID) -> VertexView<G> {
        VertexView { graph, vertex }
    }
}

impl<G: GraphViewOps> TemporalPropertiesOps for VertexView<G> {
    fn temporal_property_keys(&self) -> Vec<String> {
        self.graph.temporal_vertex_prop_names(self.vertex)
    }

    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = String> + '_> {
        Box::new(self.temporal_property_keys().into_iter())
    }

    fn get_temporal_property(&self, key: &str) -> Option<String> {
        (!self
            .graph
            .temporal_vertex_prop_vec(self.vertex, key)
            .is_empty())
        .then(|| key.to_owned())
    }
}

impl<G: GraphViewOps> TemporalPropertyViewOps for VertexView<G> {
    fn temporal_value(&self, id: &String) -> Option<Prop> {
        self.graph
            .temporal_vertex_prop_vec(self.vertex, id)
            .last()
            .map(|(_, v)| v.to_owned())
    }

    fn temporal_history(&self, id: &String) -> Vec<i64> {
        self.graph
            .temporal_vertex_prop_vec(self.vertex, id)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: &String) -> Vec<Prop> {
        self.graph
            .temporal_vertex_prop_vec(self.vertex, id)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn temporal_value_at(&self, id: &String, t: i64) -> Option<Prop> {
        let history = self.temporal_history(id);
        match history.binary_search(&t) {
            Ok(index) => Some(self.temporal_values(id)[index].clone()),
            Err(index) => (index > 0).then(|| self.temporal_values(id)[index - 1].clone()),
        }
    }
}

impl<G: GraphViewOps> StaticPropertiesOps for VertexView<G> {
    fn static_property_keys(&self) -> Vec<String> {
        self.graph.static_vertex_prop_names(self.vertex)
    }

    fn static_property_values(&self) -> Vec<Prop> {
        self.static_property_keys()
            .iter()
            .flat_map(|prop_name| self.graph.static_vertex_prop(self.vertex, prop_name))
            .collect()
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.graph.static_vertex_prop(self.vertex, key)
    }
}

impl<G: GraphViewOps> Static for VertexView<G> {}

/// View of a Vertex in a Graph
impl<G: GraphViewOps> VertexViewOps for VertexView<G> {
    type Graph = G;
    type ValueType<T> = T;
    type PathType<'a> = PathFromVertex<G> where Self: 'a;
    type EList = BoxedIter<EdgeView<G>>;

    fn id(&self) -> u64 {
        self.graph.vertex_id(self.vertex)
    }

    fn name(&self) -> String {
        self.graph.vertex_name(self.vertex)
    }

    fn earliest_time(&self) -> Option<i64> {
        self.graph.vertex_earliest_time(self.vertex)
    }

    fn latest_time(&self) -> Option<i64> {
        self.graph.vertex_latest_time(self.vertex)
    }

    fn history(&self) -> Vec<i64> {
        self.graph.vertex_history(self.vertex)
    }

    fn properties(&self) -> TemporalProperties<Self> {
        TemporalProperties::new(self.clone())
    }

    fn static_properties(&self) -> StaticProperties<Self> {
        StaticProperties::new(self.clone())
    }

    fn degree(&self) -> usize {
        let dir = Direction::BOTH;
        self.graph.degree(self.vertex, dir, None)
    }

    fn in_degree(&self) -> usize {
        let dir = Direction::IN;
        self.graph.degree(self.vertex, dir, None)
    }

    fn out_degree(&self) -> usize {
        let dir = Direction::OUT;
        self.graph.degree(self.vertex, dir, None)
    }

    fn edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        let dir = Direction::BOTH;
        Box::new(
            g.vertex_edges(self.vertex, dir, None)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    fn in_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        let dir = Direction::IN;
        Box::new(
            g.vertex_edges(self.vertex, dir, None)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    fn out_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        let dir = Direction::OUT;
        Box::new(
            g.vertex_edges(self.vertex, dir, None)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    fn neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        let dir = Direction::BOTH;
        PathFromVertex::new(g, self, Operations::Neighbours { dir })
    }

    fn in_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        let dir = Direction::IN;
        PathFromVertex::new(g, self, Operations::Neighbours { dir })
    }

    fn out_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        let dir = Direction::OUT;
        PathFromVertex::new(g, self, Operations::Neighbours { dir })
    }
}

impl<G: GraphViewOps> TimeOps for VertexView<G> {
    type WindowedViewType = VertexView<WindowedGraph<G>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        VertexView {
            graph: self.graph.window(t_start, t_end),
            vertex: self.vertex,
        }
    }
}

impl<G: GraphViewOps> LayerOps for VertexView<G> {
    type LayeredViewType = VertexView<LayeredGraph<G>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        VertexView {
            graph: self.graph.default_layer(),
            vertex: self.vertex,
        }
    }

    fn layer(&self, name: &str) -> Option<Self::LayeredViewType> {
        Some(VertexView {
            graph: self.graph.layer(name)?,
            vertex: self.vertex,
        })
    }
}

/// Implementation of the VertexListOps trait for an iterator of VertexView objects.
///
impl<G: GraphViewOps> VertexListOps for Box<dyn Iterator<Item = VertexView<G>> + Send> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type IterType<T> = Box<dyn Iterator<Item = T> + Send>;
    type EList = Box<dyn Iterator<Item = EdgeView<Self::Graph>> + Send>;
    type ValueType<T> = T;

    fn earliest_time(self) -> BoxedIter<Option<i64>> {
        Box::new(self.map(|v| v.start()))
    }

    fn latest_time(self) -> BoxedIter<Option<i64>> {
        Box::new(self.map(|v| v.end().map(|t| t - 1)))
    }

    fn window(self, t_start: i64, t_end: i64) -> BoxedIter<VertexView<WindowedGraph<G>>> {
        Box::new(self.map(move |v| v.window(t_start, t_end)))
    }

    fn id(self) -> BoxedIter<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn name(self) -> BoxedIter<String> {
        Box::new(self.map(|v| v.name()))
    }

    fn properties(self) -> BoxedIter<TemporalProperties<VertexView<G>>> {
        Box::new(self.map(move |v| v.properties()))
    }

    fn history(self) -> BoxedIter<Vec<i64>> {
        Box::new(self.map(|v| v.history()))
    }

    fn static_properties(self) -> BoxedIter<StaticProperties<VertexView<G>>> {
        Box::new(self.map(move |v| v.static_properties()))
    }

    fn degree(self) -> BoxedIter<usize> {
        Box::new(self.map(|v| v.degree()))
    }

    fn in_degree(self) -> BoxedIter<usize> {
        Box::new(self.map(|v| v.in_degree()))
    }

    fn out_degree(self) -> BoxedIter<usize> {
        Box::new(self.map(|v| v.out_degree()))
    }

    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}

impl<G: GraphViewOps> VertexListOps for BoxedIter<BoxedIter<VertexView<G>>> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type IterType<T> = BoxedIter<BoxedIter<T>>;
    type EList = BoxedIter<BoxedIter<EdgeView<G>>>;
    type ValueType<T> = BoxedIter<T>;

    fn earliest_time(self) -> BoxedIter<Self::ValueType<Option<i64>>> {
        Box::new(self.map(|it| it.earliest_time()))
    }

    fn latest_time(self) -> BoxedIter<Self::ValueType<Option<i64>>> {
        Box::new(self.map(|it| it.latest_time()))
    }

    fn window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> BoxedIter<Self::ValueType<VertexView<WindowedGraph<Self::Graph>>>> {
        Box::new(self.map(move |it| it.window(t_start, t_end)))
    }

    fn id(self) -> BoxedIter<Self::ValueType<u64>> {
        Box::new(self.map(|it| it.id()))
    }

    fn name(self) -> BoxedIter<Self::ValueType<String>> {
        Box::new(self.map(|it| it.name()))
    }

    fn properties(self) -> BoxedIter<Self::ValueType<TemporalProperties<VertexView<G>>>> {
        Box::new(self.map(move |it| it.properties()))
    }

    fn history(self) -> BoxedIter<Self::ValueType<Vec<i64>>> {
        Box::new(self.map(move |it| it.history()))
    }

    fn static_properties(self) -> BoxedIter<Self::ValueType<StaticProperties<VertexView<G>>>> {
        Box::new(self.map(move |it| it.static_properties()))
    }

    fn degree(self) -> BoxedIter<Self::ValueType<usize>> {
        Box::new(self.map(|it| it.degree()))
    }

    fn in_degree(self) -> BoxedIter<Self::ValueType<usize>> {
        Box::new(self.map(|it| it.in_degree()))
    }

    fn out_degree(self) -> BoxedIter<Self::ValueType<usize>> {
        Box::new(self.map(|it| it.out_degree()))
    }

    fn edges(self) -> Self::EList {
        Box::new(self.map(|it| it.edges()))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.map(|it| it.in_edges()))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.map(|it| it.out_edges()))
    }

    fn neighbours(self) -> Self {
        Box::new(self.map(|it| it.neighbours()))
    }

    fn in_neighbours(self) -> Self {
        Box::new(self.map(|it| it.in_neighbours()))
    }

    fn out_neighbours(self) -> Self {
        Box::new(self.map(|it| it.out_neighbours()))
    }
}

#[cfg(test)]
mod vertex_test {
    use crate::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_earliest_time() {
        let g = Graph::new();
        g.add_vertex(0, 1, []).unwrap();
        g.add_vertex(1, 1, []).unwrap();
        g.add_vertex(2, 1, []).unwrap();
        let mut view = g.at(1);
        assert_eq!(view.vertex(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.vertex(1).expect("v").latest_time().unwrap(), 1);

        view = g.at(3);
        assert_eq!(view.vertex(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.vertex(1).expect("v").latest_time().unwrap(), 2);
    }

    #[test]
    fn test_properties() {
        let g = Graph::new();
        let props = [("test".to_string(), Prop::Str("test".to_string()))];
        g.add_vertex(0, 1, []).unwrap();
        g.add_vertex(2, 1, props.clone()).unwrap();

        let v1 = g.vertex(1).unwrap();
        let v1_w = g.window(0, 1).vertex(1).unwrap();
        assert_eq!(
            v1.properties()
                .pairs()
                .map(|(k, v)| (k, v.value().unwrap()))
                .collect::<HashMap<_, _>>(),
            props.into()
        );
        assert_eq!(
            v1_w.properties()
                .pairs()
                .map(|(k, v)| (k, v.value().unwrap()))
                .collect::<HashMap<_, _>>(),
            HashMap::default()
        )
    }
}
