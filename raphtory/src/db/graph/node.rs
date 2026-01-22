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
            mutation::{time_from_input_session, CollectProperties},
            properties::internal::{
                InternalMetadataOps, InternalTemporalPropertiesOps, InternalTemporalPropertyViewOps,
            },
            state::ops::NodeOp,
            view::{
                internal::{
                    GraphTimeSemanticsOps, GraphView, InternalFilter, NodeTimeSemanticsOps, Static,
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
    entities::{properties::prop::PropType, ELID},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, EventTime},
    },
    utils::time::TryIntoInputTime,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::graph::GraphStorage,
    mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps},
};
use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

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
                .node_edge_history(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
    }

    pub fn edge_history_rev(&self) -> impl Iterator<Item = (EventTime, ELID)> + '_ {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_edge_history_rev(node.as_ref(), &self.graph)
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

impl<'graph, G: CoreGraphOps + GraphTimeSemanticsOps> InternalTemporalPropertiesOps
    for NodeView<'graph, G>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.node_meta().temporal_prop_mapper().get_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .node_meta()
            .temporal_prop_mapper()
            .get_name(id)
            .clone()
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph
            .node_meta()
            .temporal_prop_mapper()
            .ids()
            .into_dyn_boxed()
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
        let res = semantics
            .node_tprop_iter_rev(node.as_ref(), &self.graph, id)
            .next()
            .map(|(_, v)| v);
        res
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

impl<'graph, G: GraphView + 'graph> NodeView<'graph, G> {
    pub fn rows<'a>(&'a self) -> BoxedLIter<'a, (EventTime, Vec<(usize, Prop)>)>
    where
        'graph: 'a,
    {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        let graph = &self.graph;
        GenLockedIter::from(node, move |node| {
            semantics
                .node_updates(node.as_ref(), graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }
}

impl<'graph, G: CoreGraphOps> InternalMetadataOps for NodeView<'graph, G> {
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.graph.node_meta().metadata_mapper().get_id(name)
    }

    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.graph
            .node_meta()
            .metadata_mapper()
            .get_name(id)
            .clone()
    }

    fn metadata_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph
            .node_meta()
            .metadata_mapper()
            .ids()
            .into_dyn_boxed()
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
        T: NodeOp + 'graph,
        T::Output: 'graph;
    type PropType = Self;
    type PathType = PathFromNode<'graph, G>;
    type Edges = Edges<'graph, G>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F> {
        let cg = self.graph.core_graph();
        op.apply(cg, self.node)
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let node = self.node;
        let edges = Arc::new(move || {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        Edges {
            base_graph: self.graph.clone(),
            edges,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let node = self.node;
        PathFromNode::new(self.graph.clone(), move || {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        })
    }
}

impl<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> NodeView<'static, G> {
    pub fn add_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        props: impl IntoIterator<Item = (PN, P)>,
    ) -> Result<(), GraphError> {
        let properties = self.graph.core_graph().validate_props(
            true,
            self.graph.node_meta(),
            props.into_iter().map(|(n, p)| (n, p.into())),
        )?;
        self.graph
            .internal_add_node_metadata(self.node, properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        self.graph
            .resolve_and_update_node_and_type(NodeRef::Internal(self.node), Some(new_type))
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn update_metadata<C: CollectProperties>(&self, props: C) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .write_session()
                .and_then(|s| s.resolve_node_property(name, dtype, true))
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.graph
            .internal_update_node_metadata(self.node, properties)
            .map_err(into_graph_err)?;
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
    ) -> Result<(), GraphError> {
        let session = self.graph.write_session().map_err(|err| err.into())?;
        let t = time_from_input_session(&session, time)?;
        let props = self
            .graph
            .validate_props(
                false,
                self.graph.node_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;
        let vid = self.node;
        self.graph
            .internal_add_node(t, vid, props)
            .map_err(into_graph_err)
    }
}
