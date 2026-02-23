//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between vertices in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        utils::iter::GenLockedIter,
    },
    db::{
        api::{
            mutation::{time_from_input, time_from_input_session},
            properties::{
                internal::{
                    InternalMetadataOps, InternalTemporalPropertiesOps,
                    InternalTemporalPropertyViewOps,
                },
                Metadata, Properties,
            },
            view::{
                internal::{EdgeTimeSemanticsOps, GraphView, InternalFilter, Static},
                BaseEdgeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
                StaticGraphViewOps,
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
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew, timeindex::EventTime},
    utils::time::TryIntoInputTime,
};
use raphtory_core::entities::{graph::tgraph::InvalidLayer, nodes::node_ref::NodeRef};
use raphtory_storage::{
    graph::edges::edge_storage_ops::EdgeStorageOps,
    mutation::{
        addition_ops::{EdgeWriteLock, InternalAdditionOps},
        deletion_ops::InternalDeletionOps,
        durability_ops::DurabilityOps,
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
use storage::wal::{GraphWalOps, WalOps};

/// A view of an edge in the graph.
#[derive(Copy, Clone)]
pub struct EdgeView<G> {
    /// A view of an edge in the graph.
    pub graph: G,
    /// A reference to the edge.
    pub edge: EdgeRef,
}

pub(crate) fn edge_valid_layer<G: GraphView>(graph: &G, e: EdgeRef) -> bool {
    match e.layer() {
        None => true,
        Some(layer) => graph.layer_ids().contains(&layer),
    }
}

impl<G> Static for EdgeView<G> {}

impl<'graph, G: GraphViewOps<'graph>> EdgeView<G> {
    pub fn new(graph: G, edge: EdgeRef) -> Self {
        Self { graph, edge }
    }
}

impl<G: Clone> EdgeView<&G> {
    pub fn cloned(&self) -> EdgeView<G> {
        EdgeView {
            graph: self.graph.clone(),
            edge: self.edge,
        }
    }
}

impl<G> EdgeView<G> {
    pub fn as_ref(&self) -> EdgeView<&G> {
        EdgeView {
            graph: &self.graph,
            edge: self.edge,
        }
    }
}

impl<G: IntoDynamic> EdgeView<G> {
    pub fn into_dynamic(self) -> EdgeView<DynamicGraph> {
        EdgeView {
            graph: self.graph.into_dynamic(),
            edge: self.edge,
        }
    }
}

impl<G: GraphView> EdgeView<G> {
    pub(crate) fn new_filtered(graph: G, edge: EdgeRef) -> Self {
        Self { graph, edge }
    }

    pub fn deletions_hist(&self) -> BoxedLIter<'_, (EventTime, usize)> {
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
    > EdgeView<G>
{
    pub fn delete<T: TryIntoInputTime>(&self, t: T, layer: Option<&str>) -> Result<(), GraphError> {
        let t = time_from_input(&self.graph, t)?;
        let layer = self.resolve_layer(layer, true)?;
        self.graph
            .internal_delete_existing_edge(t, self.edge.pid(), layer)?;
        Ok(())
    }
}

impl<'graph_1, 'graph_2, G1: GraphViewOps<'graph_1>, G2: GraphViewOps<'graph_2>>
    PartialEq<EdgeView<G2>> for EdgeView<G1>
{
    fn eq(&self, other: &EdgeView<G2>) -> bool {
        self.id() == other.id() && self.edge.time() == other.edge.time()
    }
}

impl<'graph_1, 'graph_2, G1: GraphViewOps<'graph_1>, G2: GraphViewOps<'graph_2>>
    PartialOrd<EdgeView<G2>> for EdgeView<G1>
{
    fn partial_cmp(&self, other: &EdgeView<G2>) -> Option<Ordering> {
        Some(
            self.id()
                .cmp(&other.id())
                .then(self.edge.time().cmp(&other.edge.time())),
        )
    }
}

impl<'graph_1, G1: GraphViewOps<'graph_1>> Ord for EdgeView<G1> {
    fn cmp(&self, other: &EdgeView<G1>) -> Ordering {
        self.id()
            .cmp(&other.id())
            .then(self.edge.time().cmp(&other.edge.time()))
    }
}

impl<'graph, G: GraphViewOps<'graph>> BaseEdgeViewOps<'graph> for EdgeView<G> {
    type Graph = G;

    type ValueType<T>
        = T
    where
        T: 'graph;
    type PropType = Self;
    type Nodes = NodeView<'graph, G>;
    type Exploded = Edges<'graph, G>;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        op(&self.graph, self.edge)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        Properties::new(self.clone())
    }

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        Metadata::new(self.clone())
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let vid = op(&self.graph, self.edge);
        NodeView::new_internal(self.graph.clone(), vid)
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let graph1 = self.graph.clone();
        let base_graph = self.graph.clone();
        let edge = self.edge;
        let edges = Arc::new(move || op(&graph1, edge).into_dyn_boxed());
        Edges { base_graph, edges }
    }
}

impl<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> EdgeView<G> {
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

    fn resolve_and_check_layer_for_metadata(
        &self,
        layer: Option<&str>,
    ) -> Result<usize, GraphError> {
        let create = false;
        let layer_id = self.resolve_layer(layer, create)?;

        if self
            .graph
            .core_edge(self.edge.pid())
            .has_layer(&LayerIds::One(layer_id))
        {
            Ok(layer_id)
        } else {
            Err(GraphError::InvalidEdgeLayer {
                layer: layer.unwrap_or("_default").to_string(),
                src: self.src().name(),
                dst: self.dst().name(),
            })
        }
    }

    /// Add metadata for the edge
    ///
    /// # Arguments
    ///
    /// * `properties` - Property key-value pairs to add
    /// * `layer` - The layer to which properties should be added. If the edge view is restricted to a
    ///             single layer, 'None' will add the properties to that layer and 'Some("name")'
    ///             fails unless the layer matches the edge view. If the edge view is not restricted
    ///             to a single layer, 'None' sets the properties on the default layer and 'Some("name")'
    ///             sets the properties on layer '"name"' and fails if that layer doesn't exist.
    ///
    /// Returns:
    ///     Ok(()) if metadata added successfully.
    ///     Err(GraphError) if the operation fails.
    pub fn add_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        properties: impl IntoIterator<Item = (PN, P)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let is_update = false;
        self.add_metadata_impl(properties, layer, is_update)
    }

    pub fn update_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        props: impl IntoIterator<Item = (PN, P)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let is_update = true;
        self.add_metadata_impl(props, layer, is_update)
    }

    /// Adds metadata properties to the edge.
    ///
    /// When `is_update` is true, existing properties are updated, otherwise
    /// an error is returned if the property already exists.
    fn add_metadata_impl<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        properties: impl IntoIterator<Item = (PN, P)>,
        layer: Option<&str>,
        is_update: bool,
    ) -> Result<(), GraphError> {
        let input_layer_id = self.resolve_and_check_layer_for_metadata(layer)?;
        let transaction_manager = self.graph.core_graph().transaction_manager()?;
        let wal = self.graph.core_graph().wal()?;
        let transaction_id = transaction_manager.begin_transaction();

        let props_with_status = self.graph.core_graph().validate_props_with_status(
            true,
            self.graph.edge_meta(),
            properties.into_iter().map(|(n, p)| (n, p.into())),
        )?;

        let props_for_wal = props_with_status
            .iter()
            .map(|maybe_new| {
                let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
                (prop_name.as_ref(), *prop_id, prop.clone())
            })
            .collect::<Vec<_>>();

        let props = props_with_status
            .iter()
            .map(|maybe_new| {
                let (_, prop_id, prop) = maybe_new.as_ref().inner();
                (*prop_id, prop.clone())
            })
            .collect::<Vec<_>>();

        let eid = self.edge.pid();

        let mut writer = if is_update {
            self.graph
                .internal_update_edge_metadata(eid, input_layer_id, props)
                .map_err(into_graph_err)?
        } else {
            self.graph
                .internal_add_edge_metadata(eid, input_layer_id, props)
                .map_err(into_graph_err)?
        };

        let lsn = wal.log_add_edge_metadata(transaction_id, eid, input_layer_id, props_for_wal)?;

        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }

    pub fn add_updates<
        T: TryIntoInputTime,
        PN: AsRef<str>,
        PI: Into<Prop>,
        PII: IntoIterator<Item = (PN, PI)>,
    >(
        &self,
        time: T,
        props: PII,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let transaction_manager = self.graph.core_graph().transaction_manager()?;
        let wal = self.graph.core_graph().wal()?;
        let transaction_id = transaction_manager.begin_transaction();
        let session = self.graph.write_session().map_err(|err| err.into())?;

        let t = time_from_input_session(&session, time)?;
        let layer_id = self.resolve_layer(layer, true)?;

        let props_with_status = self
            .graph
            .validate_props_with_status(
                false,
                self.graph.edge_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;

        let src = self.src().node;
        let src_name = None;
        let dst = self.dst().node;
        let dst_name = None;
        let edge_id = self.edge.pid();

        let mut writer = self
            .graph
            .atomic_add_edge(NodeRef::Internal(src), NodeRef::Internal(dst), Some(edge_id))
            .map_err(into_graph_err)?;

        let props_for_wal = props_with_status
            .iter()
            .map(|maybe_new| {
                let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
                (prop_name.as_ref(), *prop_id, prop.clone())
            })
            .collect::<Vec<_>>();

       let lsn = wal
            .log_add_edge(
                transaction_id,
                t,
                src_name,
                src,
                dst_name,
                dst,
                edge_id,
                layer,
                layer_id,
                props_for_wal,
            )
            .map_err(into_graph_err)?;

        writer.internal_add_update(t, layer_id, props);

        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalMetadataOps for EdgeView<G> {
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.graph.edge_meta().metadata_mapper().get_id(name)
    }

    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.graph
            .edge_meta()
            .metadata_mapper()
            .get_name(id)
            .clone()
    }

    fn metadata_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph
            .edge_meta()
            .metadata_mapper()
            .ids()
            .into_dyn_boxed()
    }

    fn metadata_keys(&self) -> BoxedLIter<'_, ArcStr> {
        self.graph
            .edge_meta()
            .metadata_mapper()
            .keys()
            .into_iter()
            .into_dyn_boxed()
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            match self.edge.layer() {
                None => time_semantics.edge_metadata(
                    self.graph.core_edge(self.edge.pid()).as_ref(),
                    &self.graph,
                    id,
                ),
                Some(layer) => time_semantics.edge_metadata(
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

impl<G: GraphView> InternalTemporalPropertyViewOps for EdgeView<G> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
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

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
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

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
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

    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        if edge_valid_layer(&self.graph, self.edge) {
            let time_semantics = self.graph.edge_time_semantics();
            let edge = self.graph.core_edge(self.edge.pid());

            match self.edge.time() {
                None => match self.edge.layer() {
                    None => {
                        time_semantics.temporal_edge_prop_last_at(edge.as_ref(), &self.graph, id, t)
                    }
                    Some(layer) => time_semantics.temporal_edge_prop_last_at(
                        edge.as_ref(),
                        LayeredGraph::new(&self.graph, LayerIds::One(layer)),
                        id,
                        t,
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
                        t,
                    )
                }
            }
        } else {
            None
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertiesOps for EdgeView<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.edge_meta().temporal_prop_mapper().get_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .get_name(id)
            .clone()
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .ids()
            .into_dyn_boxed()
    }

    fn temporal_prop_keys(&self) -> BoxedLIter<'_, ArcStr> {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .keys()
            .into_iter()
            .into_dyn_boxed()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Eq for EdgeView<G> {}

impl<'graph, G1: GraphViewOps<'graph>> Hash for EdgeView<G1> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
        self.edge.time().hash(state);
    }
}

impl<'graph, G: GraphViewOps<'graph>> Debug for EdgeView<G> {
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

impl<G> From<EdgeView<G>> for EdgeRef {
    fn from(value: EdgeView<G>) -> Self {
        value.edge
    }
}

impl<'graph, Current> InternalFilter<'graph> for EdgeView<Current>
where
    Current: GraphViewOps<'graph>,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = EdgeView<Next>;

    fn base_graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        EdgeView::new_filtered(filtered_graph, self.edge)
    }
}
