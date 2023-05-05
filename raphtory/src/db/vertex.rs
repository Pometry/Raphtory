//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::edge::{EdgeList, EdgeView};
use crate::db::graph_layer::LayeredGraph;
use crate::db::graph_window::WindowedGraph;
use crate::db::path::{Operations, PathFromVertex};
use crate::db::view_api::layer::LayerOps;
use crate::db::view_api::vertex::VertexViewOps;
use crate::db::view_api::{BoxedIter, GraphViewOps, TimeOps, VertexListOps};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct VertexView<G: GraphViewOps> {
    pub graph: Arc<G>,
    pub vertex: LocalVertexRef,
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
    pub(crate) fn new(graph: Arc<G>, vertex: VertexRef) -> VertexView<G> {
        let v = graph.localise_vertex_unchecked(vertex);
        VertexView { graph, vertex: v }
    }

    /// Creates a new `VertexView` wrapping a local vertex reference and a graph
    pub(crate) fn new_local(graph: Arc<G>, vertex: LocalVertexRef) -> VertexView<G> {
        VertexView { graph, vertex }
    }
}

/// View of a Vertex in a Graph
impl<G: GraphViewOps> VertexViewOps for VertexView<G> {
    type Graph = G;
    type ValueType<T> = T;
    type PathType = PathFromVertex<G>;
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

    fn property(&self, name: String, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name.clone());
        match props.last() {
            None => {
                if include_static {
                    self.graph.static_vertex_prop(self.vertex, name)
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    fn history(&self) -> Vec<i64> {
        self.graph.vertex_timestamps(self.vertex)
    }

    fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(self.vertex, name)
    }

    fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph.static_vertex_prop_names(self.vertex) {
                if let Some(prop) = self
                    .graph
                    .static_vertex_prop(self.vertex, prop_name.clone())
                {
                    props.insert(prop_name, prop);
                }
            }
        }
        props
    }

    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props(self.vertex)
    }

    fn property_names(&self, include_static: bool) -> Vec<String> {
        let mut names: Vec<String> = self.graph.temporal_vertex_prop_names(self.vertex);
        if include_static {
            names.extend(self.graph.static_vertex_prop_names(self.vertex))
        }
        names
    }

    fn has_property(&self, name: String, include_static: bool) -> bool {
        (!self.property_history(name.clone()).is_empty())
            || (include_static
                && self
                    .graph
                    .static_vertex_prop_names(self.vertex)
                    .contains(&name))
    }

    fn has_static_property(&self, name: String) -> bool {
        self.graph
            .static_vertex_prop_names(self.vertex)
            .contains(&name)
    }

    fn static_property(&self, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(self.vertex, name)
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

    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType {
        VertexView {
            graph: Arc::new(self.graph.window(t_start, t_end)),
            vertex: self.vertex,
        }
    }
}

impl<G: GraphViewOps> LayerOps for VertexView<G> {
    type LayeredViewType = VertexView<LayeredGraph<G>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        VertexView {
            graph: self.graph.default_layer().as_arc(),
            vertex: self.vertex,
        }
    }

    fn layer(&self, name: &str) -> Option<Self::LayeredViewType> {
        Some(VertexView {
            graph: self.graph.layer(name)?.as_arc(),
            vertex: self.vertex,
        })
    }
}

/// Implementation of the VertexListOps trait for an iterator of VertexView objects.
///
impl<G: GraphViewOps> VertexListOps for Box<dyn Iterator<Item = VertexView<G>> + Send> {
    type Graph = G;
    type IterType = Box<dyn Iterator<Item = VertexView<G>> + Send>;
    type EList = Box<dyn Iterator<Item = EdgeView<Self::Graph>> + Send>;
    type VList = Box<dyn Iterator<Item = VertexView<Self::Graph>> + Send>;
    type ValueType<T: Send> = T;

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

    fn property(self, name: String, include_static: bool) -> BoxedIter<Option<Prop>> {
        Box::new(self.map(move |v| v.property(name.clone(), include_static)))
    }

    fn property_history(self, name: String) -> BoxedIter<Vec<(i64, Prop)>> {
        Box::new(self.map(move |v| v.property_history(name.clone())))
    }

    fn properties(self, include_static: bool) -> BoxedIter<HashMap<String, Prop>> {
        Box::new(self.map(move |v| v.properties(include_static)))
    }

    fn history(self) -> BoxedIter<Vec<i64>> {
        Box::new(self.map(|v| v.history()))
    }

    fn property_histories(self) -> BoxedIter<HashMap<String, Vec<(i64, Prop)>>> {
        Box::new(self.map(|v| v.property_histories()))
    }

    fn property_names(self, include_static: bool) -> BoxedIter<Vec<String>> {
        Box::new(self.map(move |v| v.property_names(include_static)))
    }

    fn has_property(self, name: String, include_static: bool) -> BoxedIter<bool> {
        Box::new(self.map(move |v| v.has_property(name.clone(), include_static)))
    }

    fn has_static_property(self, name: String) -> BoxedIter<bool> {
        Box::new(self.map(move |v| v.has_static_property(name.clone())))
    }

    fn static_property(self, name: String) -> BoxedIter<Option<Prop>> {
        Box::new(self.map(move |v| v.static_property(name.clone())))
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

    fn neighbours(self) -> Self::VList {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self::VList {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self::VList {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}

impl<G: GraphViewOps> VertexListOps for BoxedIter<BoxedIter<VertexView<G>>> {
    type Graph = G;
    type IterType = Self;
    type EList = BoxedIter<BoxedIter<EdgeView<G>>>;
    type VList = Self;
    type ValueType<T: Send> = BoxedIter<T>;

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

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> BoxedIter<Self::ValueType<Option<Prop>>> {
        Box::new(self.map(move |it| it.property(name.clone(), include_static)))
    }

    fn property_history(self, name: String) -> BoxedIter<Self::ValueType<Vec<(i64, Prop)>>> {
        Box::new(self.map(move |it| it.property_history(name.clone())))
    }

    fn properties(self, include_static: bool) -> BoxedIter<Self::ValueType<HashMap<String, Prop>>> {
        Box::new(self.map(move |it| it.properties(include_static)))
    }

    fn history(self) -> BoxedIter<Self::ValueType<Vec<i64>>> {
        Box::new(self.map(move |it| it.history()))
    }

    fn property_histories(self) -> BoxedIter<Self::ValueType<HashMap<String, Vec<(i64, Prop)>>>> {
        Box::new(self.map(|it| it.property_histories()))
    }

    fn property_names(self, include_static: bool) -> BoxedIter<Self::ValueType<Vec<String>>> {
        Box::new(self.map(move |it| it.property_names(include_static)))
    }

    fn has_property(self, name: String, include_static: bool) -> BoxedIter<Self::ValueType<bool>> {
        Box::new(self.map(move |it| it.has_property(name.clone(), include_static)))
    }

    fn has_static_property(self, name: String) -> BoxedIter<Self::ValueType<bool>> {
        Box::new(self.map(move |it| it.has_static_property(name.clone())))
    }

    fn static_property(self, name: String) -> BoxedIter<Self::ValueType<Option<Prop>>> {
        Box::new(self.map(move |it| it.static_property(name.clone())))
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

    fn neighbours(self) -> Self::VList {
        Box::new(self.map(|it| it.neighbours()))
    }

    fn in_neighbours(self) -> Self::VList {
        Box::new(self.map(|it| it.in_neighbours()))
    }

    fn out_neighbours(self) -> Self::VList {
        Box::new(self.map(|it| it.out_neighbours()))
    }
}

#[cfg(test)]
mod vertex_test {
    use crate::db::view_api::*;

    #[test]
    fn test_all_degrees_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().degree(), 49);
        assert_eq!(
            g.vertex("Gandalf").unwrap().window(1356, 24792).degree(),
            34
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_degree(), 24);
        assert_eq!(
            g.vertex("Gandalf").unwrap().window(1356, 24792).in_degree(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_degree(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_degree(),
            20
        );
    }

    #[test]
    fn test_all_neighbours_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().neighbours().iter().count(), 49);

        for v in g
            .vertex("Gandalf")
            .unwrap()
            .window(1356, 24792)
            .neighbours()
            .iter()
        {
            println!("{:?}", v.id())
        }
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .neighbours()
                .iter()
                .count(),
            34
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().in_neighbours().iter().count(),
            24
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .in_neighbours()
                .iter()
                .count(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().out_neighbours().iter().count(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_neighbours()
                .iter()
                .count(),
            20
        );
    }

    #[test]
    fn test_all_edges_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().edges().count(), 59);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .edges()
                .count(),
            36
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_edges().count(), 24);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .in_edges()
                .count(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_edges().count(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_edges()
                .count(),
            20
        );
    }
}
