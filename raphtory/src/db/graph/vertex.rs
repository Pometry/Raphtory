//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::{
    core::entities::{edges::edge_ref::EdgeRef, vertices::vertex::Vertex},
    db::{
        api::view::{BaseVertexViewOps, IntoDynBoxed},
        graph::path::{PathFromGraph, PathFromVertex},
    },
};
use crate::{
    core::{
        entities::{vertices::vertex_ref::VertexRef, VID},
        storage::timeindex::TimeIndexEntry,
        utils::{errors::GraphError, time::IntoTime},
        ArcStr, Direction,
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                CollectProperties, TryIntoInputTime,
            },
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            view::{internal::Static, BoxedIter, Layer, LayerOps},
        },
        graph::{
            edge::{EdgeList, EdgeView},
            // path::{Operations, PathFromVertex},
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
    python::packages::algorithms::one_path_vertex,
};
use std::{
    fmt,
    hash::{Hash, Hasher},
};

/// View of a Vertex in a Graph
#[derive(Clone)]
pub struct VertexView<G: GraphViewOps, GH: GraphViewOps = G> {
    pub base_graph: G,
    pub graph: GH,
    pub vertex: VID,
}

impl<G1: GraphViewOps, G1H: GraphViewOps, G2: GraphViewOps, G2H: GraphViewOps>
    PartialEq<VertexView<G2, G2H>> for VertexView<G1, G1H>
{
    fn eq(&self, other: &VertexView<G2, G2H>) -> bool {
        self.id() == other.id()
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> From<VertexView<G, GH>> for VertexRef {
    fn from(value: VertexView<G, GH>) -> Self {
        VertexRef::Internal(value.vertex)
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> From<&VertexView<G, GH>> for VertexRef {
    fn from(value: &VertexView<G, GH>) -> Self {
        VertexRef::Internal(value.vertex)
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> fmt::Debug for VertexView<G, GH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VertexView {{ graph: {}{}, vertex: {} }}",
            self.graph.count_vertices(),
            self.graph.count_edges(),
            self.vertex.0
        )
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> fmt::Display for VertexView<G, GH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VertexView {{ graph: {}{}, vertex: {} }}",
            self.graph.count_vertices(),
            self.graph.count_edges(),
            self.vertex.0
        )
    }
}

impl<G1: GraphViewOps, G1H: GraphViewOps, G2: GraphViewOps, G2H: GraphViewOps>
    PartialOrd<VertexView<G2, G2H>> for VertexView<G1, G1H>
{
    fn partial_cmp(&self, other: &VertexView<G2, G2H>) -> Option<std::cmp::Ordering> {
        self.vertex.0.partial_cmp(&other.vertex.0)
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> Ord for VertexView<G, GH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vertex.0.cmp(&other.vertex.0)
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> Eq for VertexView<G, GH> {}

impl<G: GraphViewOps> VertexView<G> {
    /// Creates a new `VertexView` wrapping an internal vertex reference and a graph
    pub fn new_internal(graph: G, vertex: VID) -> VertexView<G> {
        VertexView {
            base_graph: graph.clone(),
            graph,
            vertex,
        }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> VertexView<G, GH> {
    /// Creates a new `VertexView` wrapping an internal vertex reference and a graph, internalising any global vertex ids.
    pub fn new(graph: G, vertex: VertexRef) -> VertexView<G> {
        match vertex {
            VertexRef::Internal(local) => VertexView::new_internal(graph, local),
            _ => {
                let v = graph.internalise_vertex_unchecked(vertex);
                VertexView::new_internal(graph, v)
            }
        }
    }

    pub fn new_one_hop_filtered(base_graph: G, graph: GH, vertex: VID) -> Self {
        VertexView {
            base_graph,
            graph,
            vertex,
        }
    }

    fn one_hop_filtered<GHH: GraphViewOps>(&self, one_hop_graph: GHH) -> VertexView<G, GHH> {
        let base_graph = self.base_graph.clone();
        let vertex = self.vertex;
        VertexView {
            base_graph,
            graph: one_hop_graph,
            vertex,
        }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> Hash for VertexView<G, GH> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the graph
        "1".to_string().hash(state);
        // Hash the vertex ID
        self.id().hash(state);
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> TemporalPropertiesOps for VertexView<G, GH> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph
            .vertex_meta()
            .temporal_prop_meta()
            .get_id(name)
            .filter(|id| self.graph.has_temporal_vertex_prop(self.vertex, *id))
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.vertex_meta().temporal_prop_meta().get_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(
            self.graph
                .temporal_vertex_prop_ids(self.vertex)
                .filter(|id| self.graph.has_temporal_vertex_prop(self.vertex, *id)),
        )
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> TemporalPropertyViewOps for VertexView<G, GH> {
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph
            .temporal_vertex_prop_vec(self.vertex, id)
            .last()
            .map(|(_, v)| v.to_owned())
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph
            .temporal_vertex_prop_vec(self.vertex, id)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph
            .temporal_vertex_prop_vec(self.vertex, id)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        let history = self.temporal_history(id);
        match history.binary_search(&t) {
            Ok(index) => Some(self.temporal_values(id)[index].clone()),
            Err(index) => (index > 0).then(|| self.temporal_values(id)[index - 1].clone()),
        }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> ConstPropertiesOps for VertexView<G, GH> {
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.vertex_meta().const_prop_meta().get_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph.vertex_meta().const_prop_meta().get_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph.constant_vertex_prop_ids(self.vertex)
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph.constant_vertex_prop(self.vertex, id)
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> Static for VertexView<G, GH> {}

impl<G: GraphViewOps, GH: GraphViewOps> BaseVertexViewOps for VertexView<G, GH> {
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = T;
    type PropType = Self;
    type PathType = PathFromVertex<G, G>;
    type Edge = EdgeView<G>;
    type EList = BoxedIter<EdgeView<G>>;

    fn map<O, F: for<'a> Fn(&'a Self::Graph, VID) -> O>(&self, op: F) -> Self::ValueType<O> {
        op(&self.graph, self.vertex)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        Properties::new(self.clone())
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let graph = self.base_graph.clone();
        op(&self.graph, self.vertex)
            .map(move |edge| EdgeView::new(graph.clone(), edge))
            .into_dyn_boxed()
    }

    fn hop<
        I: Iterator<Item = VID> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        PathFromVertex::new(self.base_graph.clone(), self.vertex, move |vid| {
            op(&graph, vid).into_dyn_boxed()
        })
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> TimeOps for VertexView<G, GH> {
    type WindowedViewType = VertexView<G, WindowedGraph<GH>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        let one_hop_graph = self.graph.window(start, end);
        self.one_hop_filtered(one_hop_graph)
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> LayerOps for VertexView<G, GH> {
    type LayeredViewType = VertexView<G, LayeredGraph<GH>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        let one_hop_graph = self.graph.default_layer();
        self.one_hop_filtered(one_hop_graph)
    }

    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType> {
        let one_hop_graph = self.graph.layer(name)?;
        Some(self.one_hop_filtered(one_hop_graph))
    }
}

impl<G: GraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> VertexView<G, G> {
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        props: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_vertex_property(name, dtype, true),
            |prop| self.graph.process_prop_value(prop),
        )?;
        self.graph
            .internal_add_constant_vertex_properties(self.vertex, properties)
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_vertex_property(name, dtype, true),
            |prop| self.graph.process_prop_value(prop),
        )?;
        self.graph
            .internal_update_constant_vertex_properties(self.vertex, properties)
    }

    pub fn add_updates<C: CollectProperties, T: TryIntoInputTime>(
        &self,
        time: T,
        props: C,
    ) -> Result<(), GraphError> {
        let t = TimeIndexEntry::from_input(&self.graph, time)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_vertex_property(name, dtype, false),
            |prop| self.graph.process_prop_value(prop),
        )?;
        self.graph.internal_add_vertex(t, self.vertex, properties)
    }
}

impl<V: VertexViewOps + TimeOps> VertexListOps for BoxedIter<V> {
    type Vertex = V;
    type Edge = V::Edge;
    type IterType<T> = BoxedIter<V::ValueType<T>>;
    type ValueType<T> = T;

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.into_iter().map(|v| v.earliest_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }

    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        todo!()
    }

    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        todo!()
    }

    fn id(self) -> Self::IterType<u64> {
        todo!()
    }

    fn name(self) -> Self::IterType<String> {
        todo!()
    }

    fn properties(self) -> Self::IterType<Properties<<Self::Vertex as VertexViewOps>::PropType>> {
        todo!()
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        todo!()
    }

    fn degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn in_degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn out_degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn in_edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn out_edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }

    fn in_neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }

    fn out_neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }
}

#[cfg(test)]
mod vertex_test {
    use crate::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_earliest_time() {
        let g = Graph::new();
        g.add_vertex(0, 1, NO_PROPS).unwrap();
        g.add_vertex(1, 1, NO_PROPS).unwrap();
        g.add_vertex(2, 1, NO_PROPS).unwrap();
        let view = g.before(2);
        assert_eq!(view.vertex(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.vertex(1).expect("v").latest_time().unwrap(), 1);

        let view = g.before(3);
        assert_eq!(view.vertex(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.vertex(1).expect("v").latest_time().unwrap(), 2);

        let view = g.after(0);
        assert_eq!(view.vertex(1).expect("v").earliest_time().unwrap(), 1);
        assert_eq!(view.vertex(1).expect("v").latest_time().unwrap(), 2);

        let view = g.after(2);
        assert_eq!(view.vertex(1), None);
        assert_eq!(view.vertex(1), None);

        let view = g.at(1);
        assert_eq!(view.vertex(1).expect("v").earliest_time().unwrap(), 1);
        assert_eq!(view.vertex(1).expect("v").latest_time().unwrap(), 1);
    }

    #[test]
    fn test_properties() {
        let g = Graph::new();
        let props = [("test", "test")];
        g.add_vertex(0, 1, NO_PROPS).unwrap();
        g.add_vertex(2, 1, props).unwrap();

        let v1 = g.vertex(1).unwrap();
        let v1_w = g.window(0, 1).vertex(1).unwrap();
        assert_eq!(
            v1.properties().as_map(),
            props
                .into_iter()
                .map(|(k, v)| (k.into(), v.into_prop()))
                .collect()
        );
        assert_eq!(v1_w.properties().as_map(), HashMap::default())
    }

    #[test]
    fn test_property_additions() {
        let g = Graph::new();
        let props = [("test", "test")];
        let v1 = g.add_vertex(0, 1, NO_PROPS).unwrap();
        v1.add_updates(2, props).unwrap();
        let v1_w = v1.window(0, 1);
        assert_eq!(
            v1.properties().as_map(),
            props
                .into_iter()
                .map(|(k, v)| (k.into(), v.into_prop()))
                .collect()
        );
        assert_eq!(v1_w.properties().as_map(), HashMap::default())
    }

    #[test]
    fn test_constant_property_additions() {
        let g = Graph::new();
        let v1 = g.add_vertex(0, 1, NO_PROPS).unwrap();
        v1.add_constant_properties([("test", "test")]).unwrap();
        assert_eq!(v1.properties().get("test"), Some("test".into()))
    }

    #[test]
    fn test_constant_property_updates() {
        let g = Graph::new();
        let v1 = g.add_vertex(0, 1, NO_PROPS).unwrap();
        v1.add_constant_properties([("test", "test")]).unwrap();
        v1.update_constant_properties([("test", "test2")]).unwrap();
        assert_eq!(v1.properties().get("test"), Some("test2".into()))
    }

    #[test]
    fn test_string_deduplication() {
        let g = Graph::new();
        let v1 = g
            .add_vertex(0, 1, [("test1", "test"), ("test2", "test")])
            .unwrap();
        let s1 = v1.properties().get("test1").unwrap_str();
        let s2 = v1.properties().get("test2").unwrap_str();

        assert_eq!(s1.as_ptr(), s2.as_ptr())
    }
}
