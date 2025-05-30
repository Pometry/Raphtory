//! Defines the `Node` struct, which represents a node in the graph.

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, VID},
        utils::errors::GraphError,
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                time_from_input, CollectProperties, TryIntoInputTime,
            },
            properties::internal::{
                ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertiesRowView,
                TemporalPropertyViewOps,
            },
            view::{
                internal::{CoreGraphOps, OneHopFilter, Static, TimeSemantics},
                BaseNodeViewOps, BoxedLIter, IntoDynBoxed, StaticGraphViewOps,
            },
        },
        graph::path::PathFromNode,
    },
    prelude::*,
};

use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, storage::timeindex::AsTime, PropType},
    db::{
        api::{state::NodeOp, storage::graph::storage_ops::GraphStorage},
        graph::edges::Edges,
    },
};
use chrono::{DateTime, Utc};
use raphtory_api::core::storage::{arc_str::ArcStr, timeindex::TimeIndexEntry};
use std::{
    fmt,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// View of a Node in a Graph
#[derive(Copy, Clone)]
pub struct NodeView<G, GH = G> {
    pub base_graph: G,
    pub graph: GH,
    pub node: VID,
}

impl<G1: CoreGraphOps, G1H, G2: CoreGraphOps, G2H> PartialEq<NodeView<G2, G2H>>
    for NodeView<G1, G1H>
{
    fn eq(&self, other: &NodeView<G2, G2H>) -> bool {
        self.base_graph.node_id(self.node) == other.base_graph.node_id(other.node)
    }
}

impl<G, GH> NodeView<G, GH> {
    pub fn as_ref(&self) -> NodeView<&G, &GH> {
        NodeView {
            base_graph: &self.base_graph,
            graph: &self.graph,
            node: self.node,
        }
    }
}

impl<'a, G: Clone, GH: Clone> NodeView<&'a G, &'a GH> {
    pub fn cloned(&self) -> NodeView<G, GH> {
        NodeView {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            node: self.node,
        }
    }
}

impl<G: Send + Sync, GH: Send + Sync> AsNodeRef for NodeView<G, GH> {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::Internal(self.node)
    }
}

impl<'graph, G, GH: GraphViewOps<'graph> + Debug> fmt::Debug for NodeView<G, GH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeView")
            .field("node", &self.node)
            .field("graph", &self.graph as &dyn Debug)
            .finish()
    }
}

impl<'graph, G, GH: GraphViewOps<'graph>> fmt::Display for NodeView<G, GH> {
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

impl<
        'graph,
        G1: GraphViewOps<'graph>,
        G1H: GraphViewOps<'graph>,
        G2: GraphViewOps<'graph>,
        G2H: GraphViewOps<'graph>,
    > PartialOrd<NodeView<G2, G2H>> for NodeView<G1, G1H>
{
    fn partial_cmp(&self, other: &NodeView<G2, G2H>) -> Option<std::cmp::Ordering> {
        self.id().partial_cmp(&other.id())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Ord for NodeView<G, GH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id().cmp(&other.id())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Eq for NodeView<G, GH> {}

impl<'graph, G: GraphViewOps<'graph>> NodeView<G> {
    /// Creates a new `NodeView` wrapping an internal node reference and a graph
    pub fn new_internal(graph: G, node: VID) -> NodeView<G> {
        NodeView {
            base_graph: graph.clone(),
            graph,
            node,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodeView<G, GH> {
    pub fn new_one_hop_filtered(base_graph: G, graph: GH, node: VID) -> Self {
        NodeView {
            base_graph,
            graph,
            node,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph>
    for NodeView<G, GH>
{
}
// impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
//     ExplodedEdgePropertyFilterOps<'graph> for NodeView<G, GH>
// {
// }

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodePropertyFilterOps<'graph>
    for NodeView<G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for NodeView<G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = NodeView<G, GHH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        let node = self.node;
        NodeView {
            base_graph,
            graph: filtered_graph,
            node,
        }
    }
}

impl<G, GH: CoreGraphOps> Hash for NodeView<G, GH> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the graph
        "1".to_string().hash(state);
        let id = self.graph.node_id(self.node);
        // Hash the node ID
        id.hash(state);
    }
}

impl<G, GH: CoreGraphOps + TimeSemantics> TemporalPropertiesOps for NodeView<G, GH> {
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

impl<G, GH: CoreGraphOps + TimeSemantics> TemporalPropertyViewOps for NodeView<G, GH> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .node_meta()
            .temporal_prop_meta()
            .get_dtype(id)
            .unwrap()
    }
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph
            .temporal_node_prop_hist(self.node, id)
            .next_back()
            .map(|(_, v)| v.to_owned())
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph
            .temporal_node_prop_hist(self.node, id)
            .into_iter()
            .map(|(t, _)| t.t())
            .collect()
    }

    fn temporal_history_iter(&self, id: usize) -> BoxedLIter<i64> {
        Box::new(
            self.graph
                .temporal_node_prop_hist(self.node, id)
                .into_iter()
                .map(|(t, _)| t.t()),
        )
    }

    fn temporal_history_date_time(&self, id: usize) -> Option<Vec<DateTime<Utc>>> {
        self.graph
            .temporal_node_prop_hist(self.node, id)
            .into_iter()
            .map(|(t, _)| t.dt())
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph
            .temporal_node_prop_hist(self.node, id)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn temporal_values_iter(&self, id: usize) -> BoxedLIter<Prop> {
        Box::new(
            self.graph
                .temporal_node_prop_hist(self.node, id)
                .into_iter()
                .map(|(_, v)| v),
        )
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        let history = self.temporal_history(id);
        match history.binary_search(&t) {
            Ok(index) => Some(self.temporal_values(id)[index].clone()),
            Err(index) => (index > 0).then(|| self.temporal_values(id)[index - 1].clone()),
        }
    }
}

impl<G, GH: CoreGraphOps + TimeSemantics> TemporalPropertiesRowView for NodeView<G, GH> {
    fn rows(&self) -> BoxedLIter<(TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.graph.node_history_rows(self.node, None)
    }
}

impl<G: Send + Sync, GH: CoreGraphOps> ConstPropertiesOps for NodeView<G, GH> {
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

impl<G, GH> Static for NodeView<G, GH> {}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for NodeView<G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T>
        = T::Output
    where
        T: NodeOp + 'graph,
        T::Output: 'graph;
    type PropType = Self;
    type PathType = PathFromNode<'graph, G, G>;
    type Edges = Edges<'graph, G, GH>;

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
        let base_graph = self.base_graph.clone();
        let graph = self.graph.clone();
        Edges {
            base_graph,
            graph,
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
        PathFromNode::new(self.base_graph.clone(), move || {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        })
    }
}

impl<G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> NodeView<G, G> {
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        properties: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = properties.collect_properties(|name, dtype| {
            Ok(self.graph.resolve_node_property(name, dtype, true)?.inner())
        })?;
        self.graph
            .internal_add_constant_node_properties(self.node, &properties)
    }

    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        self.graph.resolve_node_and_type(self.node, new_type)?;
        Ok(())
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self.graph.resolve_node_property(name, dtype, true)?.inner())
        })?;
        self.graph
            .internal_update_constant_node_properties(self.node, &properties)
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
                .resolve_node_property(name, dtype, false)?
                .inner())
        })?;
        self.graph.internal_add_node(t, self.node, &properties)
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
