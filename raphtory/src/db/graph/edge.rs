//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use chrono::{DateTime, Utc};

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::timeindex::AsTime,
        utils::{errors::GraphError, time::IntoTime},
        PropType,
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps},
                time_from_input, CollectProperties, TryIntoInputTime,
            },
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{
                internal::{OneHopFilter, Static},
                BaseEdgeViewOps, IntoDynBoxed, StaticGraphViewOps,
            },
        },
        graph::{edges::Edges, node::NodeView},
    },
    prelude::*,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

/// A view of an edge in the graph.
#[derive(Copy, Clone)]
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

impl<G: Clone, GH: Clone> EdgeView<&G, &GH> {
    pub fn cloned(&self) -> EdgeView<G, GH> {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let edge = self.edge;
        EdgeView {
            base_graph,
            graph,
            edge,
        }
    }
}

impl<G, GH> EdgeView<G, GH> {
    pub fn as_ref(&self) -> EdgeView<&G, &GH> {
        let graph = &self.graph;
        let base_graph = &self.base_graph;
        let edge = self.edge;
        EdgeView {
            base_graph,
            graph,
            edge,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgeView<G, GH> {
    pub(crate) fn new_filtered(base_graph: G, graph: GH, edge: EdgeRef) -> Self {
        Self {
            base_graph,
            graph,
            edge,
        }
    }

    #[allow(dead_code)]
    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids().constrain_from_edge(self.edge)
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
        let t = time_from_input(&self.graph, t)?;
        let layer = self.resolve_layer(layer, true)?;
        self.graph
            .internal_delete_existing_edge(t, self.edge.pid(), layer)
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> ResetFilter<'graph>
    for EdgeView<G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for EdgeView<G, GH>
{
    type BaseGraph = G;
    type Graph = GH;

    type ValueType<T>
        = T
    where
        T: 'graph;
    type PropType = Self;
    type Nodes = NodeView<G, G>;
    type Exploded = Edges<'graph, G, GH>;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        op(&self.graph, self.edge)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        Properties::new(self.clone())
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let vid = op(&self.graph, self.edge);
        NodeView::new_internal(self.base_graph.clone(), vid)
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let graph1 = self.graph.clone();
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let edge = self.edge;
        let edges = Arc::new(move || op(&graph1, edge).into_dyn_boxed());
        Edges {
            graph,
            base_graph,
            edges,
        }
    }
}

impl<G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> EdgeView<G, G> {
    fn get_valid_layers(graph: &G) -> Vec<String> {
        graph.unique_layers().map(|l| l.0.to_string()).collect()
    }

    fn resolve_layer(&self, layer: Option<&str>, create: bool) -> Result<usize, GraphError> {
        match layer {
            Some(name) => match self.edge.layer() {
                Some(l_id) => self
                    .graph
                    .get_layer_id(name)
                    .filter(|&id| id == l_id)
                    .ok_or_else(|| {
                        GraphError::invalid_layer(
                            name.to_owned(),
                            Self::get_valid_layers(&self.graph),
                        )
                    }),
                None => {
                    if create {
                        Ok(self.graph.resolve_layer(layer)?.inner())
                    } else {
                        self.graph
                            .get_layer_id(name)
                            .ok_or(GraphError::invalid_layer(
                                name.to_owned(),
                                Self::get_valid_layers(&self.graph),
                            ))
                    }
                }
            },
            None => Ok(self.edge.layer().unwrap_or(0)),
        }
    }

    /// Add constant properties for the edge
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
        let input_layer_id = self.resolve_layer(layer, false)?;
        if !self
            .graph
            .core_edge(self.edge.pid())
            .has_layer(&LayerIds::One(input_layer_id))
        {
            return Err(GraphError::InvalidEdgeLayer {
                layer: layer.unwrap_or("_default").to_string(),
                src: self.src().name(),
                dst: self.dst().name(),
            });
        }
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self.graph.resolve_edge_property(name, dtype, true)?.inner())
        })?;

        self.graph.internal_add_constant_edge_properties(
            self.edge.pid(),
            input_layer_id,
            &properties,
        )
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let input_layer_id = self.resolve_layer(layer, false)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self.graph.resolve_edge_property(name, dtype, true)?.inner())
        })?;

        self.graph.internal_update_constant_edge_properties(
            self.edge.pid(),
            input_layer_id,
            &properties,
        )
    }

    pub fn add_updates<C: CollectProperties, T: TryIntoInputTime>(
        &self,
        time: T,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let t = time_from_input(&self.graph, time)?;
        let layer_id = self.resolve_layer(layer, true)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_edge_property(name, dtype, false)?
                .inner())
        })?;

        self.graph
            .internal_add_edge_update(t, self.edge.pid(), &properties, layer_id)?;
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
        self.graph
            .edge_meta()
            .const_prop_meta()
            .get_name(id)
            .clone()
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph
            .const_edge_prop_ids(self.edge, self.graph.layer_ids().clone())
    }

    fn const_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        let reverse_map = self.graph.edge_meta().const_prop_meta().get_keys();
        Box::new(self.const_prop_ids().map(move |id| reverse_map[id].clone()))
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph
            .get_const_edge_prop(self.edge, id, self.graph.layer_ids().clone())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> TemporalPropertyViewOps
    for EdgeView<G, GH>
{
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph
            .temporal_edge_prop_hist(self.edge, id, self.layer_ids())
            .into_iter()
            .map(|(t, _)| t.t())
            .collect()
    }
    fn temporal_history_date_time(&self, id: usize) -> Option<Vec<DateTime<Utc>>> {
        self.graph
            .temporal_edge_prop_hist(self.edge, id, self.layer_ids())
            .into_iter()
            .map(|(t, _)| t.dt())
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        let layer_ids = self.layer_ids();
        self.graph
            .temporal_edge_prop_hist(self.edge, id, layer_ids)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn temporal_values_iter(&self, id: usize) -> Box<dyn Iterator<Item = Prop> + '_> {
        let layer_ids = self.layer_ids();
        Box::new(
            self.graph
                .temporal_edge_prop_hist(self.edge, id, layer_ids)
                .into_iter()
                .map(|(_, v)| v),
        )
    }

    fn temporal_history_iter(&self, id: usize) -> Box<dyn Iterator<Item = i64> + '_> {
        Box::new(
            self.graph
                .temporal_edge_prop_hist(self.edge, id, self.layer_ids())
                .into_iter()
                .map(|(t, _)| t.t()),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> TemporalPropertiesOps
    for EdgeView<G, GH>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        let layer_ids = self.layer_ids();
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_id(name)
            .filter(move |id| self.graph.has_temporal_edge_prop(self.edge, *id, layer_ids))
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_name(id)
            .clone()
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        let layer_ids = self.layer_ids();
        Box::new(
            self.graph
                .temporal_edge_prop_ids(self.edge, layer_ids.clone())
                .filter(move |id| {
                    self.graph
                        .has_temporal_edge_prop(self.edge, *id, layer_ids.clone())
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
        write!(f, "EdgeView({:?}, {:?})", self.src().id(), self.dst().id())
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
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EdgeView<G, GHH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        EdgeView::new_filtered(self.base_graph.clone(), filtered_graph, self.edge)
    }
}

#[cfg(test)]
mod test_edge {
    use crate::{
        core::IntoPropMap, db::api::view::time::TimeOps, prelude::*, test_storage,
        test_utils::test_graph,
    };
    use itertools::Itertools;
    use raphtory_api::core::storage::arc_str::ArcStr;
    use std::collections::HashMap;

    #[test]
    fn test_properties() {
        let graph = Graph::new();
        let props = [(ArcStr::from("test"), "test".into_prop())];
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(2, 1, 2, props.clone(), None).unwrap();
        test_storage!(&graph, |graph| {
            let e1 = graph.edge(1, 2).unwrap();
            let e1_w = graph.window(0, 1).edge(1, 2).unwrap();
            assert_eq!(
                HashMap::from_iter(e1.properties().as_vec()),
                props.clone().into()
            );
            assert!(e1_w.properties().as_vec().is_empty())
        });
    }

    #[test]
    fn test_constant_properties() {
        let graph = Graph::new();
        graph
            .add_edge(1, 1, 2, NO_PROPS, Some("layer 1"))
            .unwrap()
            .add_constant_properties([("test_prop", "test_val")], Some("layer 1"))
            .unwrap();
        graph
            .add_edge(1, 2, 3, NO_PROPS, Some("layer 2"))
            .unwrap()
            .add_constant_properties([("test_prop", "test_val")], Some("layer 2"))
            .unwrap();

        // FIXME: #18 constant prop for edges
        test_graph(&graph, |graph| {
            assert_eq!(
                graph
                    .edge(1, 2)
                    .unwrap()
                    .properties()
                    .constant()
                    .get("test_prop"),
                Some([("layer 1", "test_val")].into_prop_map())
            );
            assert_eq!(
                graph
                    .edge(2, 3)
                    .unwrap()
                    .properties()
                    .constant()
                    .get("test_prop"),
                Some([("layer 2", "test_val")].into_prop_map())
            );
            for e in graph.edges() {
                for ee in e.explode() {
                    assert_eq!(
                        ee.properties().constant().get("test_prop"),
                        Some("test_val".into())
                    )
                }
            }
        });
    }

    #[test]
    fn test_property_additions() {
        let graph = Graph::new();
        let props = [("test", "test")];
        let e1 = graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e1.add_updates(2, props, None).unwrap(); // same layer works
        assert!(e1.add_updates(2, props, Some("test2")).is_err()); // different layer is error
        let e = graph.edge(1, 2).unwrap();
        e.add_updates(2, props, Some("test2")).unwrap(); // non-restricted edge view can create new layers
        let layered_views = e.explode_layers().into_iter().collect_vec();
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
            e.layers("test2").unwrap().properties().as_map(),
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
