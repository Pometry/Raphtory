//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        utils::{iter::GenLockedIter, time::IntoTime},
    },
    db::{
        api::{
            mutation::{time_from_input, CollectProperties, TryIntoInputTime},
            properties::{
                internal::{ConstantPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            view::{
                internal::{EdgeTimeSemanticsOps, OneHopFilter, Static},
                BaseEdgeViewOps, BoxableGraphView, BoxedLIter, DynamicGraph, IntoDynBoxed,
                IntoDynamic, StaticGraphViewOps,
            },
        },
        graph::{edges::Edges, node::NodeView, views::layer_graph::LayeredGraph},
    },
    errors::{into_graph_err, GraphError},
    prelude::*,
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::properties::prop::PropType,
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
};
use raphtory_core::entities::graph::tgraph::InvalidLayer;
use raphtory_storage::{
    graph::edges::edge_storage_ops::EdgeStorageOps,
    mutation::{
        addition_ops::InternalAdditionOps, deletion_ops::InternalDeletionOps,
        property_addition_ops::InternalPropertyAdditionOps,
    },
};
use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    iter,
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

pub(crate) fn edge_valid_layer<G: BoxableGraphView + Clone>(graph: &G, e: EdgeRef) -> bool {
    match e.layer() {
        None => true,
        Some(layer) => graph.layer_ids().contains(&layer),
    }
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

impl<G: IntoDynamic, GH: IntoDynamic> EdgeView<G, GH> {
    pub fn into_dynamic(self) -> EdgeView<DynamicGraph, DynamicGraph> {
        let base_graph = self.base_graph.into_dynamic();
        let graph = self.graph.into_dynamic();
        EdgeView {
            base_graph,
            graph,
            edge: self.edge,
        }
    }
}

impl<G: BoxableGraphView + Clone, GH: BoxableGraphView + Clone> EdgeView<G, GH> {
    pub(crate) fn new_filtered(base_graph: G, graph: GH, edge: EdgeRef) -> Self {
        Self {
            base_graph,
            graph,
            edge,
        }
    }

    pub fn deletions_hist(&self) -> BoxedLIter<(TimeIndexEntry, usize)> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(g, e) {
            let time_semantics = g.edge_time_semantics();
            let edge = g.core_edge(e.pid());
            match e.time() {
                Some(t) => {
                    let layer = e.layer().expect("exploded edge should have layer");
                    time_semantics
                        .edge_exploded_deletion(edge.as_ref(), g, t, layer)
                        .map(move |t| (t, layer))
                        .into_iter()
                        .into_dyn_boxed()
                }
                None => match e.layer() {
                    None => GenLockedIter::from(edge, move |edge| {
                        time_semantics
                            .edge_deletion_history(edge.as_ref(), g, g.layer_ids())
                            .into_dyn_boxed()
                    })
                    .into_dyn_boxed(),
                    Some(layer) => {
                        if self.graph.layer_ids().contains(&layer) {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_deletion_history(edge.as_ref(), g, layer_ids)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        } else {
                            iter::empty().into_dyn_boxed()
                        }
                    }
                },
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }
}

impl<
        G: StaticGraphViewOps
            + InternalAdditionOps<Error = GraphError>
            + InternalPropertyAdditionOps<Error = GraphError>
            + InternalDeletionOps<Error = GraphError>,
    > EdgeView<G, G>
{
    pub fn delete<T: IntoTime>(&self, t: T, layer: Option<&str>) -> Result<(), GraphError> {
        let t = time_from_input(&self.graph, t)?;
        let layer = self.resolve_layer(layer, true)?;
        self.graph
            .internal_delete_existing_edge(t, self.edge.pid(), layer)?;
        Ok(())
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
        self.id() == other.id() && self.edge.time() == other.edge.time()
    }
}

impl<
        'graph_1,
        'graph_2,
        G1: GraphViewOps<'graph_1>,
        GH1: GraphViewOps<'graph_1>,
        G2: GraphViewOps<'graph_2>,
        GH2: GraphViewOps<'graph_2>,
    > PartialOrd<EdgeView<G2, GH2>> for EdgeView<G1, GH1>
{
    fn partial_cmp(&self, other: &EdgeView<G2, GH2>) -> Option<Ordering> {
        Some(
            self.id()
                .cmp(&other.id())
                .then(self.edge.time().cmp(&other.edge.time())),
        )
    }
}

impl<'graph_1, 'graph_2, G1: GraphViewOps<'graph_1>, GH1: GraphViewOps<'graph_1>> Ord
    for EdgeView<G1, GH1>
{
    fn cmp(&self, other: &EdgeView<G1, GH1>) -> Ordering {
        self.id()
            .cmp(&other.id())
            .then(self.edge.time().cmp(&other.edge.time()))
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
    type Nodes = NodeView<'graph, G, G>;
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
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
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

impl<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> EdgeView<G, G> {
    fn resolve_layer(&self, layer: Option<&str>, create: bool) -> Result<usize, GraphError> {
        let layer_id = match layer {
            Some(name) => match self.edge.layer() {
                Some(l_id) => self
                    .graph
                    .get_layer_id(name)
                    .filter(|&id| id == l_id)
                    .ok_or_else(|| {
                        InvalidLayer::new(
                            name.into(),
                            vec![self.graph.get_layer_name(l_id).to_string()],
                        )
                    })?,
                None => {
                    if create {
                        self.graph
                            .resolve_layer(layer)
                            .map_err(into_graph_err)?
                            .inner()
                    } else {
                        self.graph.get_layer_id(name).ok_or_else(|| {
                            InvalidLayer::new(
                                name.into(),
                                self.graph.unique_layers().map_into().collect(),
                            )
                        })?
                    }
                }
            },
            None => {
                let layer = self.edge.layer();
                match layer {
                    Some(l_id) => l_id,
                    None => self
                        .graph
                        .get_default_layer_id()
                        .ok_or_else(|| GraphError::no_default_layer(&self.graph))?,
                }
            }
        };
        Ok(layer_id)
    }

    /// Add constant properties for the edge
    ///
    /// # Arguments
    ///
    /// * `properties` - Property key-value pairs to add
    /// * `layer` - The layer to which properties should be added. If the edge view is restricted to a
    ///             single layer, 'None' will add the properties to that layer and 'Some("name")'
    ///             fails unless the layer matches the edge view. If the edge view is not restricted
    ///             to a single layer, 'None' sets the properties on the default layer and 'Some("name")'
    ///             sets the properties on layer '"name"' and fails if that layer doesn't exist.
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        properties: C,
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
        let properties: Vec<(usize, Prop)> = properties.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_edge_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;

        self.graph
            .internal_add_constant_edge_properties(self.edge.pid(), input_layer_id, &properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let input_layer_id = self.resolve_layer(layer, false).map_err(into_graph_err)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_edge_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;

        self.graph
            .internal_update_constant_edge_properties(self.edge.pid(), input_layer_id, &properties)
            .map_err(into_graph_err)?;
        Ok(())
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
                .resolve_edge_property(name, dtype, false)
                .map_err(into_graph_err)?
                .inner())
        })?;

        self.graph
            .internal_add_edge_update(t, self.edge.pid(), &properties, layer_id)
            .map_err(into_graph_err)?;
        Ok(())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> ConstantPropertiesOps
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

    fn const_prop_ids(&self) -> BoxedLIter<usize> {
        Box::new(0..self.graph.edge_meta().const_prop_meta().len())
    }

    fn const_prop_keys(&self) -> BoxedLIter<ArcStr> {
        let reverse_map = self.graph.edge_meta().const_prop_meta().get_keys();
        Box::new(self.const_prop_ids().map(move |id| reverse_map[id].clone()))
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            match self.edge.layer() {
                None => time_semantics.constant_edge_prop(
                    self.graph.core_edge(self.edge.pid()).as_ref(),
                    &self.graph,
                    id,
                ),
                Some(layer) => time_semantics.constant_edge_prop(
                    self.graph.core_edge(self.edge.pid()).as_ref(),
                    LayeredGraph::new(&self.graph, LayerIds::One(layer)),
                    id,
                ),
            }
        } else {
            None
        }
    }
}

impl<G: BoxableGraphView + Clone, GH: BoxableGraphView + Clone> TemporalPropertyViewOps
    for EdgeView<G, GH>
{
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            let edge = self.graph.core_edge(self.edge.pid());
            match self.edge.time() {
                None => match self.edge.layer() {
                    None => time_semantics
                        .temporal_edge_prop_hist_rev(
                            edge.as_ref(),
                            &self.graph,
                            self.graph.layer_ids(),
                            id,
                        )
                        .next(),
                    Some(layer) => time_semantics
                        .temporal_edge_prop_hist_rev(
                            edge.as_ref(),
                            &self.graph,
                            &LayerIds::One(layer),
                            id,
                        )
                        .next(),
                }
                .map(|(_, _, v)| v),
                Some(t) => {
                    let layer = self.edge.layer().expect("exploded edge should have layer");
                    time_semantics.temporal_edge_prop_exploded(
                        edge.as_ref(),
                        &self.graph,
                        id,
                        t,
                        layer,
                    )
                }
            }
        } else {
            None
        }
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            let edge = self.graph.core_edge(self.edge.pid());
            let graph = &self.graph;
            match self.edge.time() {
                None => match self.edge.layer() {
                    None => GenLockedIter::from(edge, move |edge| {
                        time_semantics
                            .temporal_edge_prop_hist(edge.as_ref(), graph, graph.layer_ids(), id)
                            .into_dyn_boxed()
                    })
                    .into_dyn_boxed(),
                    Some(layer) => {
                        let layer_ids = LayerIds::One(layer);
                        GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                            time_semantics
                                .temporal_edge_prop_hist(edge.as_ref(), graph, layer_ids, id)
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed()
                    }
                }
                .map(|(t, _, v)| (t, v))
                .into_dyn_boxed(),
                Some(t) => {
                    let layer = self.edge.layer().expect("Exploded edge should have layer");
                    time_semantics
                        .temporal_edge_prop_exploded(edge.as_ref(), &self.graph, id, t, layer)
                        .map(|v| (t, v))
                        .into_iter()
                        .into_dyn_boxed()
                }
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            let edge = self.graph.core_edge(self.edge.pid());
            let graph = &self.graph;
            match self.edge.time() {
                None => match self.edge.layer() {
                    None => GenLockedIter::from(edge, move |edge| {
                        time_semantics
                            .temporal_edge_prop_hist_rev(
                                edge.as_ref(),
                                graph,
                                graph.layer_ids(),
                                id,
                            )
                            .into_dyn_boxed()
                    })
                    .into_dyn_boxed(),
                    Some(layer) => {
                        let layer_ids = LayerIds::One(layer);
                        GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                            time_semantics
                                .temporal_edge_prop_hist_rev(edge.as_ref(), graph, layer_ids, id)
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed()
                    }
                }
                .map(|(t, _, v)| (t, v))
                .into_dyn_boxed(),
                Some(t) => {
                    let layer = self.edge.layer().expect("Exploded edge should have layer");
                    time_semantics
                        .temporal_edge_prop_exploded(edge.as_ref(), &self.graph, id, t, layer)
                        .map(|v| (t, v))
                        .into_iter()
                        .into_dyn_boxed()
                }
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            let edge = self.graph.core_edge(self.edge.pid());

            match self.edge.time() {
                None => match self.edge.layer() {
                    None => time_semantics.temporal_edge_prop_last_at(
                        edge.as_ref(),
                        &self.graph,
                        id,
                        TimeIndexEntry::start(t),
                    ),
                    Some(layer) => time_semantics.temporal_edge_prop_last_at(
                        edge.as_ref(),
                        LayeredGraph::new(&self.graph, LayerIds::One(layer)),
                        id,
                        TimeIndexEntry::start(t),
                    ),
                },
                Some(ti) => {
                    let layer = self.edge.layer().expect("Exploded edge should have layer");
                    time_semantics.temporal_edge_prop_exploded_last_at(
                        edge.as_ref(),
                        &self.graph,
                        ti,
                        layer,
                        id,
                        TimeIndexEntry::start(t),
                    )
                }
            }
        } else {
            None
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> TemporalPropertiesOps
    for EdgeView<G, GH>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.edge_meta().temporal_prop_meta().get_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_name(id)
            .clone()
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(0..self.graph.edge_meta().temporal_prop_meta().len())
    }

    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        let reverse_map = self.graph.edge_meta().temporal_prop_meta().get_keys();
        Box::new(
            self.temporal_prop_ids()
                .map(move |id| reverse_map[id].clone()),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Eq for EdgeView<G, GH> {}

impl<'graph, G1: GraphViewOps<'graph>, GH1: GraphViewOps<'graph>> Hash for EdgeView<G1, GH1> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
        self.edge.time().hash(state);
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Debug for EdgeView<G, GH> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeView")
            .field("src", &self.src().id())
            .field("dst", &self.dst().id())
            .field("time", &self.time().ok())
            .field("layer", &self.layer_name().ok())
            .field("properties", &self.properties())
            .finish()
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
    use crate::{db::api::view::time::TimeOps, prelude::*, test_storage, test_utils::test_graph};
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
        assert_eq!(e.edge.layer(), Some(0));
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
