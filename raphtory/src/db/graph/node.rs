//! Defines the `Node` struct, which represents a node in the graph.

use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, VID},
    db::{
        api::{
            mutation::{time_from_input, CollectProperties, TryIntoInputTime},
            properties::internal::{
                ConstantPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            view::{
                internal::{BaseFilter, GraphTimeSemanticsOps, Static},
                BaseNodeViewOps, BoxedLIter, IntoDynBoxed, StaticGraphViewOps,
            },
        },
        graph::path::PathFromNode,
    },
    prelude::*,
};

use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::iter::GenLockedIter},
    db::{
        api::{
            state::NodeOp,
            view::{
                internal::NodeTimeSemanticsOps, DynamicGraph, ExplodedEdgePropertyFilterOps,
                IntoDynamic,
            },
        },
        graph::edges::Edges,
    },
    errors::{into_graph_err, GraphError},
};
use raphtory_api::core::{
    entities::properties::prop::PropType,
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
};
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};
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
    fn as_node_ref(&self) -> NodeRef {
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
}

impl<'graph, G: GraphViewOps<'graph>> NodeView<'graph, G> {
    pub fn new_one_hop_filtered(graph: G, node: VID) -> Self {
        NodeView {
            graph,
            node,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph> for NodeView<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilterOps<'graph>
    for NodeView<'graph, G>
{
}

impl<'graph, G: GraphViewOps<'graph>> NodePropertyFilterOps<'graph> for NodeView<'graph, G> {}

impl<'graph, Current> BaseFilter<'graph> for NodeView<'graph, Current>
where
    Current: GraphViewOps<'graph>,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = NodeView<'graph, Next>;

    fn base_graph(&self) -> &Self::BaseGraph {
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

impl<'graph, G: CoreGraphOps + GraphTimeSemanticsOps> TemporalPropertiesOps
    for NodeView<'graph, G>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.node_meta().temporal_prop_meta().get_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .node_meta()
            .temporal_prop_meta()
            .get_name(id)
            .clone()
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(0..self.graph.node_meta().temporal_prop_meta().len())
    }
}

impl<'graph, G: GraphViewOps<'graph>> TemporalPropertyViewOps for NodeView<'graph, G> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .node_meta()
            .temporal_prop_meta()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        let res = semantics
            .node_tprop_iter(node.as_ref(), &self.graph, id)
            .next_back()
            .map(|(_, v)| v);
        res
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, |node| {
            semantics
                .node_tprop_iter(node.as_ref(), &self.graph, id)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, |node| {
            semantics
                .node_tprop_iter(node.as_ref(), &self.graph, id)
                .rev()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        semantics
            .node_tprop_last_at(node.as_ref(), &self.graph, id, TimeIndexEntry::end(t))
            .map(|(_, v)| v)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeView<'graph, G> {
    pub fn rows<'a>(&'a self) -> BoxedLIter<'a, (TimeIndexEntry, Vec<(usize, Prop)>)>
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

impl<'graph, G: CoreGraphOps> ConstantPropertiesOps for NodeView<'graph, G> {
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.node_meta().const_prop_meta().get_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph
            .node_meta()
            .const_prop_meta()
            .get_name(id)
            .clone()
    }

    fn const_prop_ids(&self) -> BoxedLIter<usize> {
        self.graph.constant_node_prop_ids(self.node)
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph.constant_node_prop(self.node, id)
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
    type PathType = PathFromNode<'graph, G, G>;
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
            graph: self.graph.clone(),
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
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        properties: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = properties.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_node_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.graph
            .internal_add_constant_node_properties(self.node, &properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        self.graph
            .resolve_node_and_type(NodeRef::Internal(self.node), new_type)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_node_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.graph
            .internal_update_constant_node_properties(self.node, &properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn add_updates<C: CollectProperties, T: TryIntoInputTime>(
        &self,
        time: T,
        props: C,
    ) -> Result<(), GraphError> {
        let t = time_from_input(&self.graph, time)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_node_property(name, dtype, false)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.graph
            .internal_add_node(t, self.node, &properties)
            .map_err(into_graph_err)
    }
}

#[cfg(test)]
mod node_test {
    use crate::{prelude::*, test_utils::test_graph};
    use raphtory_api::core::storage::arc_str::ArcStr;
    use std::collections::HashMap;

    #[test]
    fn test_earliest_time() {
        let graph = Graph::new();
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 1, NO_PROPS, None).unwrap();

        // FIXME: Node add without properties not showing up (Issue #46)
        test_graph(&graph, |graph| {
            let view = graph.before(2);
            assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 0);
            assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 1);

            let view = graph.before(3);
            assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 0);
            assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 2);

            let view = graph.after(0);
            assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 1);
            assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 2);

            let view = graph.after(2);
            assert_eq!(view.node(1), None);
            assert_eq!(view.node(1), None);

            let view = graph.at(1);
            assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 1);
            assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 1);
        });
    }

    #[test]
    fn test_properties() {
        let graph = Graph::new();
        let props = [("test", "test")];
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 1, props, None).unwrap();

        // FIXME: Node add without properties not showing up (Issue #46)
        test_graph(&graph, |graph| {
            let v1 = graph.node(1).unwrap();
            let v1_w = graph.window(0, 1).node(1).unwrap();
            assert_eq!(
                v1.properties().as_map(),
                [(ArcStr::from("test"), Prop::str("test"))].into()
            );
            assert_eq!(v1_w.properties().as_map(), HashMap::default())
        });
    }

    #[test]
    fn test_property_additions() {
        let graph = Graph::new();
        let props = [("test", "test")];
        let v1 = graph.add_node(0, 1, NO_PROPS, None).unwrap();
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
        let v1 = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v1.add_constant_properties([("test", "test")]).unwrap();
        assert_eq!(v1.properties().get("test"), Some("test".into()))
    }

    #[test]
    fn test_constant_property_updates() {
        let g = Graph::new();
        let v1 = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v1.add_constant_properties([("test", "test")]).unwrap();
        v1.update_constant_properties([("test", "test2")]).unwrap();
        assert_eq!(v1.properties().get("test"), Some("test2".into()))
    }

    #[test]
    fn test_string_deduplication() {
        let g = Graph::new();
        let v1 = g
            .add_node(0, 1, [("test1", "test"), ("test2", "test")], None)
            .unwrap();
        let s1 = v1.properties().get("test1").unwrap_str();
        let s2 = v1.properties().get("test2").unwrap_str();

        assert_eq!(s1.as_ptr(), s2.as_ptr())
    }
}
