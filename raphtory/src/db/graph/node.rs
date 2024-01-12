//! Defines the `Node` struct, which represents a node in the graph.

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, LayerIds, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
        ArcStr,
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
            view::{
                internal::{CoreGraphOps, InternalLayerOps, OneHopFilter, Static, TimeSemantics},
                BaseNodeViewOps, BoxedLIter, IntoDynBoxed, Layer, StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, path::PathFromNode},
    },
    prelude::*,
};

use crate::{core::storage::timeindex::AsTime, db::graph::edges::Edges};
use chrono::{DateTime, Utc};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// View of a Node in a Graph
#[derive(Clone)]
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

impl<G, GH> From<NodeView<G, GH>> for NodeRef {
    fn from(value: NodeView<G, GH>) -> Self {
        NodeRef::Internal(value.node)
    }
}

impl<G, GH> From<&NodeView<G, GH>> for NodeRef {
    fn from(value: &NodeView<G, GH>) -> Self {
        NodeRef::Internal(value.node)
    }
}

impl<'graph, G, GH: GraphViewOps<'graph>> fmt::Debug for NodeView<G, GH> {
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
        self.node.0.partial_cmp(&other.node.0)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Ord for NodeView<G, GH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.node.0.cmp(&other.node.0)
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
        self.graph
            .node_meta()
            .temporal_prop_meta()
            .get_id(name)
            .filter(|id| self.graph.has_temporal_node_prop(self.node, *id))
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.node_meta().temporal_prop_meta().get_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(
            self.graph
                .temporal_node_prop_ids(self.node)
                .filter(|id| self.graph.has_temporal_node_prop(self.node, *id)),
        )
    }
}

impl<G, GH: TimeSemantics> TemporalPropertyViewOps for NodeView<G, GH> {
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph
            .temporal_node_prop_vec(self.node, id)
            .last()
            .map(|(_, v)| v.to_owned())
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph
            .temporal_node_prop_vec(self.node, id)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_history_date_time(&self, id: usize) -> Option<Vec<DateTime<Utc>>> {
        self.graph
            .temporal_node_prop_vec(self.node, id)
            .into_iter()
            .map(|(t, _)| t.dt())
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph
            .temporal_node_prop_vec(self.node, id)
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

impl<G, GH: CoreGraphOps> ConstPropertiesOps for NodeView<G, GH> {
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.node_meta().const_prop_meta().get_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph.node_meta().const_prop_meta().get_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
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
    type ValueType<T> = T where T: 'graph;
    type PropType = Self;
    type PathType = PathFromNode<'graph, G, G>;
    type Edges = Edges<'graph, G, GH>;

    fn map<O: 'graph, F: for<'a> Fn(&'a Self::Graph, VID) -> O>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        op(&self.graph, self.node)
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
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let node = self.node;
        let edges = Arc::new(move || op(&graph, node).into_dyn_boxed());
        let base_graph = self.base_graph.clone();
        let graph = self.graph.clone();
        Edges {
            base_graph,
            graph,
            edges,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let node = self.node;
        PathFromNode::new(self.base_graph.clone(), move || {
            op(&graph, node).into_dyn_boxed()
        })
    }
}

impl<G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> NodeView<G, G> {
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        props: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_node_property(name, dtype, true),
            |prop| self.graph.process_prop_value(prop),
        )?;
        self.graph
            .internal_add_constant_node_properties(self.node, properties)
    }

    pub fn update_constant_properties<C: CollectProperties>(
        &self,
        props: C,
    ) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_node_property(name, dtype, true),
            |prop| self.graph.process_prop_value(prop),
        )?;
        self.graph
            .internal_update_constant_node_properties(self.node, properties)
    }

    pub fn add_updates<C: CollectProperties, T: TryIntoInputTime>(
        &self,
        time: T,
        props: C,
    ) -> Result<(), GraphError> {
        let t = TimeIndexEntry::from_input(&self.graph, time)?;
        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.graph.resolve_node_property(name, dtype, false),
            |prop| self.graph.process_prop_value(prop),
        )?;
        self.graph.internal_add_node(t, self.node, properties)
    }
}

#[cfg(test)]
mod node_test {
    use crate::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_earliest_time() {
        let g = Graph::new();
        g.add_node(0, 1, NO_PROPS).unwrap();
        g.add_node(1, 1, NO_PROPS).unwrap();
        g.add_node(2, 1, NO_PROPS).unwrap();
        let view = g.before(2);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 1);

        let view = g.before(3);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 2);

        let view = g.after(0);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 1);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 2);

        let view = g.after(2);
        assert_eq!(view.node(1), None);
        assert_eq!(view.node(1), None);

        let view = g.at(1);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 1);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 1);
    }

    #[test]
    fn test_properties() {
        let g = Graph::new();
        let props = [("test", "test")];
        g.add_node(0, 1, NO_PROPS).unwrap();
        g.add_node(2, 1, props).unwrap();

        let v1 = g.node(1).unwrap();
        let v1_w = g.window(0, 1).node(1).unwrap();
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
        let v1 = g.add_node(0, 1, NO_PROPS).unwrap();
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
        let v1 = g.add_node(0, 1, NO_PROPS).unwrap();
        v1.add_constant_properties([("test", "test")]).unwrap();
        assert_eq!(v1.properties().get("test"), Some("test".into()))
    }

    #[test]
    fn test_constant_property_updates() {
        let g = Graph::new();
        let v1 = g.add_node(0, 1, NO_PROPS).unwrap();
        v1.add_constant_properties([("test", "test")]).unwrap();
        v1.update_constant_properties([("test", "test2")]).unwrap();
        assert_eq!(v1.properties().get("test"), Some("test2".into()))
    }

    #[test]
    fn test_string_deduplication() {
        let g = Graph::new();
        let v1 = g
            .add_node(0, 1, [("test1", "test"), ("test2", "test")])
            .unwrap();
        let s1 = v1.properties().get("test1").unwrap_str();
        let s2 = v1.properties().get("test2").unwrap_str();

        assert_eq!(s1.as_ptr(), s2.as_ptr())
    }
}
