//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use chrono::NaiveDateTime;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::TimeIndexEntry,
        utils::{errors::GraphError, time::IntoTime},
        ArcStr,
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps},
                CollectProperties, TryIntoInputTime,
            },
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            view::{
                internal::{InternalLayerOps, OneHopFilter, Static},
                BoxedIter, BoxedLIter, EdgeViewInternalOps, StaticGraphViewOps,
            },
        },
        graph::{node::NodeView, views::window_graph::WindowedGraph},
    },
    prelude::*,
};
use std::{
    fmt::{Debug, Formatter},
    iter,
};

/// A view of an edge in the graph.
#[derive(Clone)]
pub struct EdgeView<G, GH = G> {
    pub base_graph: G,
    /// A view of an edge in the graph.
    pub graph: GH,
    /// A reference to the edge.
    pub edge: EdgeRef,
}

impl<G, GH> Static for EdgeView<G, GH> {}

impl<'graph, G: GraphViewOps<'graph>> EdgeView<G, G> {
    pub fn new(graph: G, edge: EdgeRef) -> Self {
        let base_graph = graph.clone();
        Self {
            base_graph,
            graph,
            edge,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgeView<G, GH> {
    pub fn new_filtered(base_graph: G, graph: GH, edge: EdgeRef) -> Self {
        Self {
            base_graph,
            graph,
            edge,
        }
    }
}

impl<
        G: StaticGraphViewOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + InternalDeletionOps,
    > EdgeView<G, G>
{
    pub fn delete<T: IntoTime>(&self, t: T, layer: Option<&str>) -> Result<(), GraphError> {
        let t = TimeIndexEntry::from_input(&self.graph, t)?;
        let layer = self.resolve_layer(layer)?;
        self.graph
            .internal_delete_edge(t, self.edge.src(), self.edge.dst(), layer)
    }
}

impl<
        'graph_1,
        'graph_2,
        G1: GraphViewOps<'graph_1>,
        GH1: GraphViewOps<'graph_1>,
        G2: GraphViewOps<'graph_2>,
        GH2: GraphViewOps<'graph_2>,
    > PartialEq<EdgeView<G2, GH2>> for EdgeView<G1, GH1>
{
    fn eq(&self, other: &EdgeView<G2, GH2>) -> bool {
        self.id() == other.id()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgeViewInternalOps<'graph>
    for EdgeView<G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type EList = BoxedLIter<'graph, Self>;
    type Neighbour = NodeView<G, G>;

    fn graph(&self) -> &GH {
        &self.graph
    }

    fn eref(&self) -> EdgeRef {
        self.edge
    }

    fn new_node(&self, v: VID) -> NodeView<G, G> {
        NodeView::new_internal(self.base_graph.clone(), v)
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        Self {
            graph: self.graph.clone(),
            base_graph: self.base_graph.clone(),
            edge: e,
        }
    }

    fn internal_explode(&self) -> Self::EList {
        let ev = self.clone();
        match self.edge.time() {
            Some(_) => Box::new(iter::once(ev)),
            None => {
                let layer_ids = self.layer_ids();
                let e = self.edge;
                let ex_iter = self.graph.edge_exploded(e, layer_ids);
                // FIXME: use duration
                Box::new(ex_iter.map(move |ex| ev.new_edge(ex)))
            }
        }
    }

    fn internal_explode_layers(&self) -> Self::EList {
        let ev = self.clone();
        match self.edge.layer() {
            Some(_) => Box::new(iter::once(ev)),
            None => {
                let e = self.edge;
                let ex_iter = self.graph.edge_layers(e, self.graph.layer_ids());
                Box::new(ex_iter.map(move |ex| ev.new_edge(ex)))
            }
        }
    }
}

impl<G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> EdgeView<G, G> {
    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError> {
        match layer {
            Some(name) => match self.edge.layer() {
                Some(l_id) => self
                    .graph
                    .get_layer_id(name)
                    .filter(|id| id == l_id)
                    .ok_or_else(|| GraphError::InvalidLayer(name.to_owned())),
                None => Ok(self.graph.resolve_layer(layer)),
            },
            None => Ok(self.edge.layer().copied().unwrap_or(0)),
        }
    }

    /// Add constant properties for the edge
    ///
    /// Returns a person with the name given them
    ///
    /// # Arguments
    ///
    /// * `props` - Property key-value pairs to add
    /// * `layer` - The layer to which properties should be added. If the edge view is restricted to a
    ///             single layer, 'None' will add the properties to that layer and 'Some("name")'
    ///             fails unless the layer matches the edge view. If the edge view is not restricted
    ///             to a single layer, 'None' sets the properties on the default layer and 'Some("name")'
    ///             sets the properties on layer '"name"' and fails if that layer doesn't exist.
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_edge_property(name, dtype, true),
            |prop| self.graph.process_prop_value(prop),
        )?;
        let input_layer_id = self.resolve_layer(layer)?;

        self.graph.internal_add_constant_edge_properties(
            self.edge.pid(),
            input_layer_id,
            properties,
        )
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_edge_property(name, dtype, true),
            |prop| self.graph.process_prop_value(prop),
        )?;
        let input_layer_id = self.resolve_layer(layer)?;

        self.graph.internal_update_constant_edge_properties(
            self.edge.pid(),
            input_layer_id,
            properties,
        )
    }

    pub fn add_updates<C: CollectProperties, T: TryIntoInputTime>(
        &self,
        time: T,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let t = TimeIndexEntry::from_input(&self.graph, time)?;
        let layer_id = self.resolve_layer(layer)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_edge_property(name, dtype, false),
            |prop| self.graph.process_prop_value(prop),
        )?;

        self.graph
            .internal_add_edge(t, self.edge.src(), self.edge.dst(), properties, layer_id)?;
        Ok(())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> ConstPropertiesOps
    for EdgeView<G, GH>
{
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.edge_meta().const_prop_meta().get_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph.edge_meta().const_prop_meta().get_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph
            .const_edge_prop_ids(self.edge, self.graph.layer_ids())
    }

    fn const_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        let reverse_map = self.graph.edge_meta().const_prop_meta().get_keys();
        Box::new(self.const_prop_ids().map(move |id| reverse_map[id].clone()))
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph
            .get_const_edge_prop(self.edge, id, self.graph.layer_ids())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> TemporalPropertyViewOps
    for EdgeView<G, GH>
{
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph
            .temporal_edge_prop_vec(self.edge, id, self.graph.layer_ids())
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        let layer_ids = self.graph.layer_ids().constrain_from_edge(self.edge);
        self.graph
            .temporal_edge_prop_vec(self.edge, id, layer_ids)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> TemporalPropertiesOps
    for EdgeView<G, GH>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_id(name)
            .filter(|id| {
                self.graph
                    .has_temporal_edge_prop(self.edge, *id, self.layer_ids())
            })
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.edge_meta().temporal_prop_meta().get_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(
            self.graph
                .temporal_edge_prop_ids(self.edge, self.layer_ids())
                .filter(|id| {
                    self.graph
                        .has_temporal_edge_prop(self.edge, *id, self.layer_ids())
                }),
        )
    }

    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        let reverse_map = self.graph.edge_meta().temporal_prop_meta().get_keys();
        Box::new(
            self.temporal_prop_ids()
                .map(move |id| reverse_map[id].clone()),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Debug for EdgeView<G, GH> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EdgeView({}, {})", self.src().id(), self.dst().id())
    }
}

impl<G, GH> From<EdgeView<G, GH>> for EdgeRef {
    fn from(value: EdgeView<G, GH>) -> Self {
        value.edge
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for EdgeView<G, GH>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EdgeView<G, GHH>;

    fn current_filter(&self) -> &Self::Graph {
        &self.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        EdgeView::new_filtered(self.base_graph.clone(), filtered_graph, self.edge)
    }
}

/// Implement `EdgeListOps` trait for an iterator of `EdgeView` objects.
///
/// This implementation enables the use of the `src` and `dst` methods to retrieve the nodes
/// connected to the edges inside the iterator.
impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgeListOps<'graph>
    for BoxedLIter<'graph, EdgeView<G, GH>>
{
    type Edge = EdgeView<G, GH>;
    type ValueType<T> = T;

    /// Specifies the associated type for an iterator over nodes.
    type VList = BoxedLIter<'graph, NodeView<G, G>>;

    /// Specifies the associated type for the iterator over edges.
    type IterType<T> = BoxedLIter<'graph, T>;

    fn properties(self) -> Self::IterType<Properties<Self::Edge>> {
        Box::new(self.map(move |e| e.properties()))
    }

    /// Returns an iterator over the source nodes of the edges in the iterator.
    fn src(self) -> Self::VList {
        Box::new(self.map(|e| e.src()))
    }

    /// Returns an iterator over the destination nodes of the edges in the iterator.
    fn dst(self) -> Self::VList {
        Box::new(self.map(|e| e.dst()))
    }

    fn id(self) -> Self::IterType<(u64, u64)> {
        Box::new(self.map(|e| e.id()))
    }

    /// returns an iterator of exploded edges that include an edge at each point in time
    fn explode(self) -> Self {
        Box::new(self.flat_map(move |e| e.explode()))
    }

    /// Gets the earliest times of a list of edges
    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    fn earliest_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.earliest_date_time()))
    }

    /// Gets the latest times of a list of edges
    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }

    fn latest_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.latest_date_time()))
    }

    fn date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.date_time()))
    }

    fn time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.time()))
    }

    fn layer_name(self) -> Self::IterType<Option<ArcStr>> {
        Box::new(self.map(|e| e.layer_name().map(|v| v.clone())))
    }

    fn layer_names(self) -> Self::IterType<BoxedIter<ArcStr>> {
        Box::new(self.map(|e| e.layer_names()))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|e| e.history()))
    }

    fn start(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.start()))
    }

    fn start_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.start_date_time()))
    }

    fn end(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.end()))
    }

    fn end_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.end_date_time()))
    }

    fn at<T: IntoTime>(self, time: T) -> Self::IterType<EdgeView<G, WindowedGraph<GH>>> {
        let new_time = time.into_time();
        Box::new(self.map(move |e| e.at(new_time)))
    }

    fn window<T: IntoTime>(
        self,
        start: T,
        end: T,
    ) -> Self::IterType<EdgeView<G, WindowedGraph<GH>>> {
        let start = start.into_time();
        let end = end.into_time();
        Box::new(self.map(move |e| e.window(start, end)))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgeListOps<'graph>
    for BoxedLIter<'graph, BoxedLIter<'graph, EdgeView<G, GH>>>
{
    type Edge = EdgeView<G, GH>;
    type ValueType<T> = BoxedLIter<'graph, T>;
    type VList = BoxedLIter<'graph, BoxedLIter<'graph, NodeView<G, G>>>;
    type IterType<T> = BoxedLIter<'graph, BoxedLIter<'graph, T>>;

    fn properties(self) -> Self::IterType<Properties<Self::Edge>> {
        Box::new(self.map(move |it| it.properties()))
    }

    fn src(self) -> Self::VList {
        Box::new(self.map(|it| it.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.map(|it| it.dst()))
    }

    fn id(self) -> Self::IterType<(u64, u64)> {
        Box::new(self.map(|it| it.id()))
    }

    fn explode(self) -> Self {
        Box::new(self.map(move |it| it.explode()))
    }

    /// Gets the earliest times of a list of edges
    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    fn earliest_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.earliest_date_time()))
    }

    fn latest_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|e| e.latest_date_time()))
    }

    /// Gets the latest times of a list of edges
    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }

    fn time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|it| it.time()))
    }

    fn layer_name(self) -> Self::IterType<Option<ArcStr>> {
        Box::new(self.map(|it| it.layer_name()))
    }

    fn layer_names(self) -> Self::IterType<BoxedIter<ArcStr>> {
        Box::new(self.map(|it| it.layer_names()))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|it| it.history()))
    }

    fn start(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|it| it.start()))
    }

    fn start_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|it| it.start_date_time()))
    }

    fn end(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|it| it.end()))
    }

    fn end_date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|it| it.end_date_time()))
    }

    fn date_time(self) -> Self::IterType<Option<NaiveDateTime>> {
        Box::new(self.map(|it| it.date_time()))
    }

    fn at<T: IntoTime>(self, time: T) -> Self::IterType<EdgeView<G, WindowedGraph<GH>>> {
        let new_time = time.into_time();
        Box::new(self.map(move |e| e.at(new_time)))
    }

    fn window<T: IntoTime>(
        self,
        start: T,
        end: T,
    ) -> Self::IterType<EdgeView<G, WindowedGraph<GH>>> {
        let start = start.into_time();
        let end = end.into_time();
        Box::new(self.map(move |e| e.window(start, end)))
    }
}

#[cfg(test)]
mod test_edge {
    use crate::{
        core::{ArcStr, IntoPropMap},
        prelude::*,
    };
    use itertools::Itertools;
    use std::collections::HashMap;

    #[test]
    fn test_properties() {
        let g = Graph::new();
        let props = [(ArcStr::from("test"), "test".into_prop())];
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, props.clone(), None).unwrap();

        let e1 = g.edge(1, 2).unwrap();
        let e1_w = g.window(0, 1).edge(1, 2).unwrap();
        assert_eq!(HashMap::from_iter(e1.properties().as_vec()), props.into());
        assert!(e1_w.properties().as_vec().is_empty())
    }

    #[test]
    fn test_constant_properties() {
        let g = Graph::new();
        g.add_edge(1, 1, 2, NO_PROPS, Some("layer 1"))
            .unwrap()
            .add_constant_properties([("test_prop", "test_val")], Some("layer 1"))
            .unwrap();
        g.add_edge(1, 2, 3, NO_PROPS, Some("layer 2"))
            .unwrap()
            .add_constant_properties([("test_prop", "test_val")], Some("layer 2"))
            .unwrap();

        assert_eq!(
            g.edge(1, 2)
                .unwrap()
                .properties()
                .constant()
                .get("test_prop"),
            Some([("layer 1", "test_val")].into_prop_map())
        );
        assert_eq!(
            g.edge(2, 3)
                .unwrap()
                .properties()
                .constant()
                .get("test_prop"),
            Some([("layer 2", "test_val")].into_prop_map())
        );
        for e in g.edges() {
            for ee in e.explode() {
                assert_eq!(
                    ee.properties().constant().get("test_prop"),
                    Some("test_val".into())
                )
            }
        }
    }

    #[test]
    fn test_property_additions() {
        let g = Graph::new();
        let props = [("test", "test")];
        let e1 = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e1.add_updates(2, props, None).unwrap(); // same layer works
        assert!(e1.add_updates(2, props, Some("test2")).is_err()); // different layer is error
        let e = g.edge(1, 2).unwrap();
        e.add_updates(2, props, Some("test2")).unwrap(); // non-restricted edge view can create new layers
        let layered_views = e.explode_layers().collect_vec();
        for ev in layered_views {
            let layer = ev.layer_name().unwrap();
            assert!(ev.add_updates(1, props, Some("test")).is_err()); // restricted edge view cannot create updates in different layer
            ev.add_updates(1, [("test2", layer)], None).unwrap() // this will add an update to the same layer as the view (not the default layer)
        }
        let e1_w = e1.window(0, 1);
        assert_eq!(
            e1.properties().as_map(),
            props
                .into_iter()
                .map(|(k, v)| (ArcStr::from(k), v.into_prop()))
                .chain([(ArcStr::from("test2"), "_default".into_prop())])
                .collect()
        );
        assert_eq!(
            e.layer("test2").unwrap().properties().as_map(),
            props
                .into_iter()
                .map(|(k, v)| (ArcStr::from(k), v.into_prop()))
                .chain([(ArcStr::from("test2"), "test2".into_prop())])
                .collect()
        );
        assert_eq!(e1_w.properties().as_map(), HashMap::default())
    }

    #[test]
    fn test_constant_property_additions() {
        let g = Graph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, Some("test")).unwrap();
        assert!(e
            .add_constant_properties([("test1", "test1")], None)
            .is_ok()); // adds properties to layer `"test"`
        assert!(e
            .add_constant_properties([("test", "test")], Some("test2"))
            .is_err()); // cannot add properties to a different layer
        e.add_constant_properties([("test", "test")], Some("test"))
            .unwrap(); // layer is consistent
        assert_eq!(e.properties().get("test"), Some("test".into()));
        assert_eq!(e.properties().get("test1"), Some("test1".into()));
    }

    #[test]
    fn test_constant_property_updates() {
        let g = Graph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, Some("test")).unwrap();
        assert!(e
            .add_constant_properties([("test1", "test1")], None)
            .is_ok()); // adds properties to layer `"test"`
        assert!(e
            .update_constant_properties([("test1", "test2")], None)
            .is_ok());
        assert_eq!(e.properties().get("test1"), Some("test2".into()));
    }

    #[test]
    fn test_layers_earliest_time() {
        let g = Graph::new();
        let e = g.add_edge(1, 1, 2, NO_PROPS, Some("test")).unwrap();
        assert_eq!(e.earliest_time(), Some(1));
    }
}
