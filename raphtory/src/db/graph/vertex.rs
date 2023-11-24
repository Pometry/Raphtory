//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::{
    core::entities::{edges::edge_ref::EdgeRef, vertices::vertex::Vertex, LayerIds},
    db::{
        api::view::{
            internal::{CoreGraphOps, GraphOps, InternalLayerOps, OneHopFilter, TimeSemantics},
            BaseVertexViewOps, BoxedLIter, IntoDynBoxed, StaticGraphViewOps,
        },
        graph::{
            graph::InternalGraph,
            path::{PathFromGraph, PathFromVertex},
        },
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
            edge::EdgeView,
            // path::{Operations, PathFromVertex},
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};
use chrono::format::format_item;
use std::{
    fmt,
    hash::{Hash, Hasher},
};

/// View of a Vertex in a Graph
#[derive(Clone)]
pub struct VertexView<G, GH = G> {
    pub base_graph: G,
    pub graph: GH,
    pub vertex: VID,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalLayerOps
    for VertexView<G, GH>
{
    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph.layer_ids_from_names(key)
    }
}

impl<G1: CoreGraphOps, G1H, G2: CoreGraphOps, G2H> PartialEq<VertexView<G2, G2H>>
    for VertexView<G1, G1H>
{
    fn eq(&self, other: &VertexView<G2, G2H>) -> bool {
        self.base_graph.vertex_id(self.vertex) == other.base_graph.vertex_id(other.vertex)
    }
}

impl<G, GH> From<VertexView<G, GH>> for VertexRef {
    fn from(value: VertexView<G, GH>) -> Self {
        VertexRef::Internal(value.vertex)
    }
}

impl<G, GH> From<&VertexView<G, GH>> for VertexRef {
    fn from(value: &VertexView<G, GH>) -> Self {
        VertexRef::Internal(value.vertex)
    }
}

impl<'graph, G, GH: GraphViewOps<'graph>> fmt::Debug for VertexView<G, GH> {
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

impl<'graph, G, GH: GraphViewOps<'graph>> fmt::Display for VertexView<G, GH> {
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

impl<
        'graph,
        G1: GraphViewOps<'graph>,
        G1H: GraphViewOps<'graph>,
        G2: GraphViewOps<'graph>,
        G2H: GraphViewOps<'graph>,
    > PartialOrd<VertexView<G2, G2H>> for VertexView<G1, G1H>
{
    fn partial_cmp(&self, other: &VertexView<G2, G2H>) -> Option<std::cmp::Ordering> {
        self.vertex.0.partial_cmp(&other.vertex.0)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Ord for VertexView<G, GH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vertex.0.cmp(&other.vertex.0)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Eq for VertexView<G, GH> {}

impl<'graph, G: GraphViewOps<'graph>> VertexView<G> {
    /// Creates a new `VertexView` wrapping an internal vertex reference and a graph
    pub fn new_internal(graph: G, vertex: VID) -> VertexView<G> {
        VertexView {
            base_graph: graph.clone(),
            graph,
            vertex,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> VertexView<G, GH> {
    pub fn new_one_hop_filtered(base_graph: G, graph: GH, vertex: VID) -> Self {
        VertexView {
            base_graph,
            graph,
            vertex,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for VertexView<G, GH>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = VertexView<G, GHH>;

    fn current_filter(&self) -> &Self::Graph {
        &self.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        let vertex = self.vertex;
        VertexView {
            base_graph,
            graph: filtered_graph,
            vertex,
        }
    }
}

impl<G, GH: CoreGraphOps> Hash for VertexView<G, GH> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the graph
        "1".to_string().hash(state);
        let id = self.graph.vertex_id(self.vertex);
        // Hash the vertex ID
        id.hash(state);
    }
}

impl<G, GH: CoreGraphOps + TimeSemantics> TemporalPropertiesOps for VertexView<G, GH> {
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

impl<G, GH: TimeSemantics> TemporalPropertyViewOps for VertexView<G, GH> {
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

impl<G, GH: CoreGraphOps> ConstPropertiesOps for VertexView<G, GH> {
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

impl<G: StaticGraphViewOps, GH: StaticGraphViewOps> Static for VertexView<G, GH> {}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseVertexViewOps<'graph>
    for VertexView<G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = T where T: 'graph;
    type PropType = Self;
    type PathType = PathFromVertex<'graph, G, G>;
    type Edge = EdgeView<G, GH>;
    type EList = BoxedLIter<'graph, EdgeView<G, GH>>;

    fn map<O: 'graph, F: for<'a> Fn(&'a Self::Graph, VID) -> O>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        op(&self.graph, self.vertex)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        Properties::new(self.clone())
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.clone();
        op(&self.graph, self.vertex)
            .map(move |edge| EdgeView::new_filtered(base_graph.clone(), graph.clone(), edge))
            .into_dyn_boxed()
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
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

impl<G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> VertexView<G, G> {
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> VertexListOps<'graph>
    for BoxedLIter<'graph, VertexView<G, GH>>
{
    type Vertex = VertexView<G, GH>;

    type Neighbour = VertexView<G, G>;
    type Edge = EdgeView<G, GH>;
    type IterType<T> = BoxedLIter<'graph, T> where T: 'graph;
    type ValueType<T> = T where T: 'graph;
    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|v| v.earliest_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        self.map(|v| v.latest_time()).into_dyn_boxed()
    }

    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps<'graph>>::WindowedViewType> {
        self.map(move |v| v.window(start, end)).into_dyn_boxed()
    }

    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps<'graph>>::WindowedViewType> {
        self.map(move |v| v.at(end)).into_dyn_boxed()
    }

    fn id(self) -> Self::IterType<u64> {
        self.map(|v| v.id()).into_dyn_boxed()
    }

    fn name(self) -> Self::IterType<String> {
        self.map(|v| v.name()).into_dyn_boxed()
    }

    fn properties(
        self,
    ) -> Self::IterType<Properties<<Self::Vertex as VertexViewOps<'graph>>::PropType>> {
        self.map(|v| v.properties()).into_dyn_boxed()
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        self.map(|v| v.history()).into_dyn_boxed()
    }

    fn degree(self) -> Self::IterType<usize> {
        self.map(|v| v.degree()).into_dyn_boxed()
    }

    fn in_degree(self) -> Self::IterType<usize> {
        self.map(|v| v.in_degree()).into_dyn_boxed()
    }

    fn out_degree(self) -> Self::IterType<usize> {
        self.map(|v| v.out_degree()).into_dyn_boxed()
    }

    fn edges(self) -> Self::IterType<Self::Edge> {
        self.flat_map(|v| v.edges()).into_dyn_boxed()
    }

    fn in_edges(self) -> Self::IterType<Self::Edge> {
        self.flat_map(|v| v.in_edges()).into_dyn_boxed()
    }

    fn out_edges(self) -> Self::IterType<Self::Edge> {
        self.flat_map(|v| v.out_edges()).into_dyn_boxed()
    }

    fn neighbours(self) -> Self::IterType<Self::Neighbour> {
        self.flat_map(|v| v.neighbours()).into_dyn_boxed()
    }

    fn in_neighbours(self) -> Self::IterType<Self::Neighbour> {
        self.flat_map(|v| v.in_neighbours()).into_dyn_boxed()
    }

    fn out_neighbours(self) -> Self::IterType<Self::Neighbour> {
        self.flat_map(|v| v.out_neighbours()).into_dyn_boxed()
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
