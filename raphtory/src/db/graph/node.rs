//! Defines the `Node` struct, which represents a node in the graph.
use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::node_ref::{AsNodeRef, NodeRef},
            VID,
        },
        utils::iter::GenLockedIter,
    },
    db::{
        api::{
            mutation::time_from_input_session,
            properties::internal::{
                InternalMetadataOps, InternalTemporalPropertiesOps,
                InternalTemporalPropertyViewOps, NodePropertySchemaOps,
            },
            state::ops::ArrowNodeOp,
            view::{
                internal::{
                    DynGraphArc, GraphTimeSemanticsOps, GraphView, InternalFilter,
                    NodeTimeSemanticsOps, Static,
                },
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::{edges::Edges, path::PathFromNode},
    },
    errors::{into_graph_err, GraphError},
    prelude::*,
};
use raphtory_api::core::{
    entities::{
        properties::{meta::STATIC_GRAPH_LAYER_ID, prop::PropType},
        LayerId, ELID,
    },
    storage::{arc_str::ArcStr, timeindex::EventTime},
    utils::time::TryIntoInputTime,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    durability_ops::DurabilityOps,
    graph::graph::GraphStorage,
    mutation::{
        addition_ops::{
            GraphMutationOps, MetadataMutationOps, NodeWriteLock, PropertyMutationOps, SetLsn,
        },
        MutationError,
    },
};
use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};
use storage::wal::{GraphWalOps, WalOps};

/// View of a Node in a Graph
#[derive(Copy, Clone)]
pub struct NodeView<'graph, G> {
    pub graph: G,
    pub node: VID,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G1: CoreGraphOps, G2: CoreGraphOps> PartialEq<NodeView<'graph, G2>>
    for NodeView<'graph, G1>
{
    fn eq(&self, other: &NodeView<'graph, G2>) -> bool {
        self.graph.node_id(self.node) == other.graph.node_id(other.node)
    }
}

impl<'b, G: 'b> NodeView<'b, G> {
    pub fn as_ref<'a>(&'a self) -> NodeView<'a, &'a G>
    where
        'b: 'a,
    {
        NodeView {
            graph: &self.graph,
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'a, 'b: 'a, G: Clone + 'b> NodeView<'a, &'a G> {
    pub fn cloned(&self) -> NodeView<'b, G> {
        NodeView {
            graph: self.graph.clone(),
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'a, G: IntoDynamic> NodeView<'a, G> {
    pub fn into_dynamic(self) -> NodeView<'a, DynamicGraph> {
        NodeView {
            graph: self.graph.into_dynamic(),
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: Send + Sync> AsNodeRef for NodeView<'graph, G> {
    fn as_node_ref(&self) -> NodeRef<'_> {
        NodeRef::Internal(self.node)
    }
}

impl<'graph, G: GraphViewOps<'graph>> fmt::Debug for NodeView<'graph, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeView")
            .field("node", &self.node)
            .field("node_id", &self.id())
            .field("properties", &self.properties())
            .field("updates", &self.rows().collect::<Vec<_>>())
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> fmt::Display for NodeView<'graph, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeView {{ graph: {}{}, node: {} }}",
            self.graph.count_nodes(),
            self.graph.count_edges(),
            self.node.0
        )
    }
}

impl<'graph, G1: GraphViewOps<'graph>, G2: GraphViewOps<'graph>> PartialOrd<NodeView<'graph, G2>>
    for NodeView<'graph, G1>
{
    fn partial_cmp(&self, other: &NodeView<'graph, G2>) -> Option<std::cmp::Ordering> {
        self.id().partial_cmp(&other.id())
    }
}

impl<'graph, G: GraphViewOps<'graph>> Ord for NodeView<'graph, G> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id().cmp(&other.id())
    }
}

impl<'graph, G: GraphViewOps<'graph>> Eq for NodeView<'graph, G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeView<'graph, G> {
    /// Creates a new `NodeView` wrapping an internal node reference and a graph
    pub fn new_internal(graph: G, node: VID) -> NodeView<'graph, G> {
        NodeView {
            graph,
            node,
            _marker: PhantomData,
        }
    }

    pub fn edge_history(&self) -> impl Iterator<Item = (EventTime, ELID)> + '_ {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_edge_history(node.as_ref(), &self.graph, self.graph.layer_ids())
                .into_dyn_boxed()
        })
    }

    pub fn edge_history_rev(&self) -> impl Iterator<Item = (EventTime, ELID)> + '_ {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_edge_history_rev(node.as_ref(), &self.graph, self.graph.layer_ids())
                .into_dyn_boxed()
        })
    }

    pub fn earliest_edge_time(&self) -> Option<EventTime> {
        self.edge_history().next().map(|(t, _)| t)
    }

    pub fn latest_edge_time(&self) -> Option<EventTime> {
        self.edge_history_rev().next().map(|(t, _)| t)
    }
}

impl<'graph, Current> InternalFilter<'graph> for NodeView<'graph, Current>
where
    Current: GraphViewOps<'graph>,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = NodeView<'graph, Next>;

    fn base_graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        NodeView {
            graph: filtered_graph,
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'a, G: CoreGraphOps> Hash for NodeView<'a, G> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the graph
        "1".to_string().hash(state);
        let id = self.graph.node_id(self.node);
        // Hash the node ID
        id.hash(state);
    }
}

impl<'graph, G: CoreGraphOps + GraphTimeSemanticsOps + NodePropertySchemaOps>
    InternalTemporalPropertiesOps for NodeView<'graph, G>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.node_visible_temporal_prop_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .node_visible_temporal_prop_name(id)
            .unwrap_or_default()
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph.node_visible_temporal_prop_ids()
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertyViewOps for NodeView<'graph, G> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .node_meta()
            .temporal_prop_mapper()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        semantics
            .node_tprop_last(node.as_ref(), &self.graph, id)
            .map(|(_, v)| v)
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, |node| {
            semantics
                .node_tprop_iter(node.as_ref(), &self.graph, id)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, |node| {
            semantics
                .node_tprop_iter_rev(node.as_ref(), &self.graph, id)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        semantics
            .node_tprop_last_at(node.as_ref(), &self.graph, id, t)
            .map(|(_, v)| v)
    }
}

/// A single node update: its time, layer and the `(prop id, value)` pairs that changed.
pub type NodeUpdateRow = (EventTime, LayerId, Vec<(usize, Prop)>);

impl<'graph, G: GraphView + 'graph> NodeView<'graph, G> {
    pub fn rows<'a>(&'a self) -> BoxedLIter<'a, NodeUpdateRow>
    where
        'graph: 'a,
    {
        let prop_ids: Arc<[usize]> = self.graph.node_visible_temporal_prop_ids().collect();
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        let graph = &self.graph;
        GenLockedIter::from(node, move |node| {
            semantics
                .node_updates(node.as_ref(), graph, prop_ids.clone())
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    pub fn rows_gs<'a>(&'a self, gs: &'a GraphStorage) -> BoxedLIter<'a, NodeUpdateRow>
    where
        'graph: 'a,
    {
        let prop_ids: Arc<[usize]> = self.graph.node_visible_temporal_prop_ids().collect();
        let semantics = self.graph.node_time_semantics();
        let node = gs.core_node(self.node);
        let graph = &self.graph;
        GenLockedIter::from(node, move |node| {
            semantics
                .node_updates(node.as_ref(), graph, prop_ids.clone())
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }
}

impl<'graph, G: CoreGraphOps + NodePropertySchemaOps> InternalMetadataOps for NodeView<'graph, G> {
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.graph.node_visible_metadata_id(name)
    }

    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.graph
            .node_visible_metadata_name(id)
            .unwrap_or_default()
    }

    fn metadata_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph.node_visible_metadata_ids()
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        self.graph.node_metadata(self.node, id)
    }
}

impl<'graph, G> Static for NodeView<'graph, G> {}

impl<'graph, G: GraphViewOps<'graph>> BaseNodeViewOps<'graph> for NodeView<'graph, G> {
    type Graph = G;
    type ValueType<T>
        = T::Output
    where
        T: ArrowNodeOp + 'graph,
        T::Output: 'graph;
    type PropType = Self;
    type PathType = PathFromNode<'graph, G>;
    type Edges = Edges<'graph, G>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<F: ArrowNodeOp + 'graph>(&self, op: F) -> Self::ValueType<F> {
        let cg = self.graph.core_graph();
        op.apply(cg, self.node)
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let node = self.node;
        let edges = Arc::new(move |graph: DynGraphArc<'graph>| {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        Edges::new(self.graph.clone(), edges)
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let node = self.node;
        PathFromNode::new(
            self.graph.clone(),
            Arc::new(move |graph| {
                let cg = graph.core_graph();
                op(cg, &graph, node).into_dyn_boxed()
            }),
        )
    }
}

impl<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> NodeView<'static, G> {
    pub fn add_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        props: impl IntoIterator<Item = (PN, P)>,
    ) -> Result<(), GraphError> {
        let is_update = false;
        self.add_metadata_impl(props, is_update)
    }

    pub fn update_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        props: impl IntoIterator<Item = (PN, P)>,
    ) -> Result<(), GraphError> {
        let is_update = true;
        self.add_metadata_impl(props, is_update)
    }

    /// Adds metadata properties to the node.
    ///
    /// When `is_update` is true, existing properties are updated, otherwise
    /// an error is returned if the property already exists.
    fn add_metadata_impl<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        properties: impl IntoIterator<Item = (PN, P)>,
        is_update: bool,
    ) -> Result<(), GraphError> {
        let mutable_graph = self.graph.mutable()?;
        let transaction_manager = mutable_graph
            .transaction_manager()
            .map_err(into_graph_err)?;
        let wal = mutable_graph.wal().map_err(into_graph_err)?;
        let transaction_id = transaction_manager.begin_transaction();

        let validated_props =
            mutable_graph.validate_props(true, self.graph.node_meta(), properties)?;
        let mut writer = mutable_graph.atomic_add_node(NodeRef::Internal(self.node), None)?;
        if !is_update {
            // check no existing property values have changed
            writer.check_metadata(STATIC_GRAPH_LAYER_ID, validated_props.resolved())?;
        }
        // at this point we start modifying the graph, no failures allowed
        let (prop_names, props) = validated_props.into_props_and_names();
        mutable_graph
            .node_meta()
            .metadata_mapper()
            .mark_props_in_layer(STATIC_GRAPH_LAYER_ID, props.iter().map(|(id, _)| *id));
        let props_for_wal = prop_names
            .iter()
            .zip(props.iter())
            .map(|(prop_name, (prop_id, prop))| (prop_name.as_ref(), *prop_id, prop.clone()))
            .collect::<Vec<_>>();
        let lsn = wal
            .log_add_node_metadata(transaction_id, self.node, props_for_wal)
            .map_err(into_graph_err)?;

        writer.update_metadata(STATIC_GRAPH_LAYER_ID, props);
        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }

    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        let mutable_graph = self.graph.mutable()?;
        let vid = self.node;
        let mut writer = mutable_graph.atomic_add_node(NodeRef::Internal(vid), None)?;
        if !writer.can_set_type() {
            // Ignore if the new type is the same as the existing type, else return an error.
            self.graph
                .node_meta()
                .get_node_type_id(new_type)
                .filter(|&new_type_id| writer.get_type() == new_type_id)
                .ok_or(MutationError::NodeTypeError)?;
            return Ok(());
        }

        let transaction_manager = mutable_graph.transaction_manager()?;
        let wal = mutable_graph.wal()?;
        let transaction_id = transaction_manager.begin_transaction();

        let new_type_id = self
            .graph
            .node_meta()
            .get_or_create_node_type_id(new_type)
            .inner();

        writer.set_type(new_type_id);

        let lsn = wal.log_set_node_type(transaction_id, vid, new_type, new_type_id)?;

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
        let mutable_graph = self.graph.mutable()?;
        let transaction_manager = mutable_graph.transaction_manager()?;
        let wal = mutable_graph.wal()?;
        let transaction_id = transaction_manager.begin_transaction();
        let t = time_from_input_session(&mutable_graph, time)?;
        let validated_props =
            mutable_graph.validate_props(false, mutable_graph.node_meta(), props)?;

        let vid = self.node;
        let mut writer = mutable_graph
            .atomic_add_node(NodeRef::Internal(vid), None)
            .map_err(into_graph_err)?;

        let layer_id = match layer {
            None => STATIC_GRAPH_LAYER_ID,
            Some(_) => mutable_graph.resolve_layer(layer).inner(),
        };

        let (prop_names, props) = validated_props.into_props_and_names();
        mutable_graph
            .node_meta()
            .temporal_prop_mapper()
            .mark_props_in_layer(layer_id, props.iter().map(|(id, _)| *id));

        let props_for_wal = prop_names
            .iter()
            .zip(props.iter())
            .map(|(prop_name, (prop_id, prop))| (prop_name.as_ref(), *prop_id, prop.clone()))
            .collect::<Vec<_>>();

        let lsn = wal
            .log_add_node(
                transaction_id,
                t,
                None,
                vid,
                None,
                props_for_wal,
                layer,
                layer_id,
            )
            .map_err(into_graph_err)?;

        writer.update_properties(t, layer_id, props);

        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }
}
