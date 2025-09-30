//! Defines the `Node` struct, which represents a node in the graph.

use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, VID},
    db::{
        api::{
            mutation::{time_from_input, CollectProperties, TryIntoInputTime},
            properties::internal::{
                InternalMetadataOps, InternalTemporalPropertiesOps, InternalTemporalPropertyViewOps,
            },
            view::{
                internal::{OneHopFilter, Static},
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
                internal::{GraphView, NodeTimeSemanticsOps},
                DynamicGraph, ExplodedEdgePropertyFilterOps, IntoDynamic,
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
use raphtory_core::{entities::ELID, storage::timeindex::AsTime};
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};
use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

/// View of a Node in a Graph
#[derive(Copy, Clone)]
pub struct NodeView<'graph, G, GH = G> {
    pub base_graph: G,
    pub graph: GH,
    pub node: VID,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G1: CoreGraphOps, G1H, G2: CoreGraphOps, G2H> PartialEq<NodeView<'graph, G2, G2H>>
    for NodeView<'graph, G1, G1H>
{
    fn eq(&self, other: &NodeView<'graph, G2, G2H>) -> bool {
        self.base_graph.node_id(self.node) == other.base_graph.node_id(other.node)
    }
}

impl<'b, G: 'b, GH: 'b> NodeView<'b, G, GH> {
    pub fn as_ref<'a>(&'a self) -> NodeView<'a, &'a G, &'a GH>
    where
        'b: 'a,
    {
        NodeView {
            base_graph: &self.base_graph,
            graph: &self.graph,
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'a, 'b: 'a, G: Clone + 'b, GH: Clone + 'b> NodeView<'a, &'a G, &'a GH> {
    pub fn cloned(&self) -> NodeView<'b, G, GH> {
        NodeView {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'a, G: IntoDynamic, GH: IntoDynamic> NodeView<'a, G, GH> {
    pub fn into_dynamic(self) -> NodeView<'a, DynamicGraph, DynamicGraph> {
        let base_graph = self.base_graph.into_dynamic();
        let graph = self.graph.into_dynamic();
        NodeView {
            base_graph,
            graph,
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: Send + Sync, GH: Send + Sync> AsNodeRef for NodeView<'graph, G, GH> {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::Internal(self.node)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> fmt::Debug
    for NodeView<'graph, G, GH>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeView")
            .field("node", &self.node)
            .field("node_id", &self.id())
            .field("properties", &self.properties())
            .field("updates", &self.rows().collect::<Vec<_>>())
            .finish()
    }
}

impl<'graph, G, GH: GraphViewOps<'graph>> fmt::Display for NodeView<'graph, G, GH> {
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
    > PartialOrd<NodeView<'graph, G2, G2H>> for NodeView<'graph, G1, G1H>
{
    fn partial_cmp(&self, other: &NodeView<'graph, G2, G2H>) -> Option<std::cmp::Ordering> {
        self.id().partial_cmp(&other.id())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Ord for NodeView<'graph, G, GH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id().cmp(&other.id())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Eq for NodeView<'graph, G, GH> {}

impl<'graph, G: GraphViewOps<'graph>> NodeView<'graph, G> {
    /// Creates a new `NodeView` wrapping an internal node reference and a graph
    pub fn new_internal(graph: G, node: VID) -> NodeView<'graph, G> {
        NodeView {
            base_graph: graph.clone(),
            graph,
            node,
            _marker: PhantomData,
        }
    }

    pub fn edge_history(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + '_ {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_edge_history(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
    }

    pub fn edge_history_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + '_ {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_edge_history_rev(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
    }

    pub fn earliest_edge_time(&self) -> Option<i64> {
        self.edge_history().next().map(|(t, _)| t.t())
    }

    pub fn latest_edge_time(&self) -> Option<i64> {
        self.edge_history_rev().next().map(|(t, _)| t.t())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodeView<'graph, G, GH> {
    pub fn new_one_hop_filtered(base_graph: G, graph: GH, node: VID) -> Self {
        NodeView {
            base_graph,
            graph,
            node,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph>
    for NodeView<'graph, G, GH>
{
}
impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    ExplodedEdgePropertyFilterOps<'graph> for NodeView<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodePropertyFilterOps<'graph>
    for NodeView<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for NodeView<'graph, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = NodeView<'graph, G, GHH>;

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
            _marker: PhantomData,
        }
    }
}

impl<'a, G, GH: CoreGraphOps> Hash for NodeView<'a, G, GH> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the graph
        "1".to_string().hash(state);
        let id = self.graph.node_id(self.node);
        // Hash the node ID
        id.hash(state);
    }
}

impl<'graph, G: GraphView, GH: GraphView> InternalTemporalPropertiesOps
    for NodeView<'graph, G, GH>
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

    fn temporal_prop_ids(&self) -> BoxedLIter<usize> {
        Box::new(0..self.graph.node_meta().temporal_prop_mapper().len())
    }
}

impl<'graph, G, GH: GraphViewOps<'graph>> InternalTemporalPropertyViewOps
    for NodeView<'graph, G, GH>
{
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

impl<'graph, G, GH: GraphViewOps<'graph>> NodeView<'graph, G, GH> {
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

impl<'graph, G: Send + Sync, GH: CoreGraphOps> InternalMetadataOps for NodeView<'graph, G, GH> {
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

    fn metadata_ids(&self) -> BoxedLIter<usize> {
        Box::new(0..self.graph.node_meta().metadata_mapper().len())
        // self.graph.node_metadata_ids(self.node)
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        self.graph.node_metadata(self.node, id)
    }
}

impl<'graph, G, GH> Static for NodeView<'graph, G, GH> {}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for NodeView<'graph, G, GH>
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

impl<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> NodeView<'static, G, G> {
    pub fn add_metadata<C: CollectProperties>(&self, properties: C) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = properties.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_node_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.graph
            .internal_add_node_metadata(self.node, &properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        self.graph
            .resolve_node_and_type(NodeRef::Internal(self.node), new_type)
            .map_err(into_graph_err)?;
        Ok(())
    }

    pub fn update_metadata<C: CollectProperties>(&self, props: C) -> Result<(), GraphError> {
        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self
                .graph
                .resolve_node_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.graph
            .internal_update_node_metadata(self.node, &properties)
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
    use crate::{prelude::*, test_storage, test_utils::test_graph};
    use raphtory_api::core::storage::arc_str::ArcStr;
    use raphtory_core::storage::timeindex::AsTime;
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
    fn test_metadata_additions() {
        let g = Graph::new();
        let v1 = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v1.add_metadata([("test", "test")]).unwrap();
        assert_eq!(v1.metadata().get("test"), Some("test".into()))
    }

    #[test]
    fn test_metadata_updates() {
        let g = Graph::new();
        let v1 = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v1.add_metadata([("test", "test")]).unwrap();
        v1.update_metadata([("test", "test2")]).unwrap();
        assert_eq!(v1.metadata().get("test"), Some("test2".into()))
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

    #[test]
    fn test_edge_history_and_timestamps() {
        let graph = Graph::new();

        // Add nodes
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(0, 2, NO_PROPS, None).unwrap();
        graph.add_node(0, 3, NO_PROPS, None).unwrap();

        // Add edges at different times
        graph.add_edge(10, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(20, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(30, 2, 1, NO_PROPS, None).unwrap();
        graph.add_edge(5, 1, 3, NO_PROPS, None).unwrap(); // Earlier edge to same pair

        test_storage!(&graph, |graph| {
            let node1 = graph.node(1).unwrap();

            // Test edge_history (chronological order)
            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![5, 10, 20, 30]);

            // Test edge_history_rev (reverse chronological order)
            let history_rev: Vec<_> = node1.edge_history_rev().map(|(t, _)| t.t()).collect();
            assert_eq!(history_rev, vec![30, 20, 10, 5]);

            // Test earliest_edge_time
            assert_eq!(node1.earliest_edge_time(), Some(5));

            // Test latest_edge_time
            assert_eq!(node1.latest_edge_time(), Some(30));
        });
    }

    #[test]
    fn test_edge_timestamps_with_windows() {
        let graph = Graph::new();

        // Add nodes
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(0, 2, NO_PROPS, None).unwrap();
        graph.add_node(0, 3, NO_PROPS, None).unwrap();

        // Add edges at different times
        graph.add_edge(5, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(15, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(25, 2, 1, NO_PROPS, None).unwrap();
        graph.add_edge(35, 1, 3, NO_PROPS, None).unwrap();

        test_graph(&graph, |graph| {
            // Test window 0-20
            let windowed = graph.window(0, 20);
            let node1 = windowed.node(1).unwrap();

            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![5, 15]);

            assert_eq!(node1.earliest_edge_time(), Some(5));
            assert_eq!(node1.latest_edge_time(), Some(15));

            // Test window 10-30
            let windowed = graph.window(10, 30);
            let node1 = windowed.node(1).unwrap();

            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![15, 25]);

            assert_eq!(node1.earliest_edge_time(), Some(15));
            assert_eq!(node1.latest_edge_time(), Some(25));

            // Test window after all edges
            let windowed = graph.after(40);
            assert_eq!(windowed.node(1), None); // Node has no edges in this window
        });
    }

    #[test]
    fn test_edge_timestamps_with_layers() {
        let graph = Graph::new();

        // Add nodes
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(0, 2, NO_PROPS, None).unwrap();
        graph.add_node(0, 3, NO_PROPS, None).unwrap();

        // Add edges on different layers
        graph.add_edge(10, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(20, 1, 3, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(30, 2, 1, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(5, 1, 3, NO_PROPS, Some("layer2")).unwrap();

        test_graph(&graph, |graph| {
            // Test all layers
            let node1 = graph.node(1).unwrap();
            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![5, 10, 20, 30]);
            assert_eq!(node1.earliest_edge_time(), Some(5));
            assert_eq!(node1.latest_edge_time(), Some(30));

            // Test layer1 only
            let layer1_graph = graph.layers(vec!["layer1"]).unwrap();
            let node1_layer1 = layer1_graph.node(1).unwrap();
            let history: Vec<_> = node1_layer1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![10, 30]);
            assert_eq!(node1_layer1.earliest_edge_time(), Some(10));
            assert_eq!(node1_layer1.latest_edge_time(), Some(30));

            // Test layer2 only
            let layer2_graph = graph.layers(vec!["layer2"]).unwrap();
            let node1_layer2 = layer2_graph.node(1).unwrap();
            let history: Vec<_> = node1_layer2.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![5, 20]);
            assert_eq!(node1_layer2.earliest_edge_time(), Some(5));
            assert_eq!(node1_layer2.latest_edge_time(), Some(20));
        });
    }

    #[test]
    fn test_edge_timestamps_overlapping_windows_and_layers() {
        let graph = Graph::new();

        // Add nodes
        graph.add_node(0, 1, NO_PROPS, None).unwrap();
        graph.add_node(0, 2, NO_PROPS, None).unwrap();
        graph.add_node(0, 3, NO_PROPS, None).unwrap();
        graph.add_node(0, 4, NO_PROPS, None).unwrap();

        // Add edges with overlapping time ranges across multiple layers
        graph.add_edge(5, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(10, 1, 3, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(15, 1, 4, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(20, 2, 1, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(25, 3, 1, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(30, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(35, 1, 4, NO_PROPS, Some("layer2")).unwrap();

        test_graph(&graph, |graph| {
            // Test overlapping window (8-22) with layer1
            let windowed_layer1 = graph.window(8, 22).layers(vec!["layer1"]).unwrap();
            let node1 = windowed_layer1.node(1).unwrap();

            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![10, 20]);
            assert_eq!(node1.earliest_edge_time(), Some(10));
            assert_eq!(node1.latest_edge_time(), Some(20));

            // Test overlapping window (12-28) with layer2
            let windowed_layer2 = graph.window(12, 28).layers(vec!["layer2"]).unwrap();
            let node1 = windowed_layer2.node(1).unwrap();

            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![15, 25]);
            assert_eq!(node1.earliest_edge_time(), Some(15));
            assert_eq!(node1.latest_edge_time(), Some(25));

            // Test overlapping window (18-32) with both layers
            let windowed_both = graph
                .window(18, 32)
                .layers(vec!["layer1", "layer2"])
                .unwrap();
            let node1 = windowed_both.node(1).unwrap();

            let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
            assert_eq!(history, vec![20, 25, 30]);
            assert_eq!(node1.earliest_edge_time(), Some(20));
            assert_eq!(node1.latest_edge_time(), Some(30));

            // Test edge case: window with no edges for the node
            let empty_window = graph.window(50, 60);
            assert_eq!(empty_window.node(1), None);
        });
    }

    #[test]
    fn test_edge_timestamps_no_edges() {
        let graph = Graph::new();

        // Add a node but no edges
        graph.add_node(10, 1, NO_PROPS, None).unwrap();

        test_graph(&graph, |graph| {
            let node1 = graph.node(1).unwrap();

            // Node exists but has no edges
            assert_eq!(node1.edge_history().count(), 0);
            assert_eq!(node1.edge_history_rev().count(), 0);
            assert_eq!(node1.earliest_edge_time(), None);
            assert_eq!(node1.latest_edge_time(), None);
        });
    }
}
