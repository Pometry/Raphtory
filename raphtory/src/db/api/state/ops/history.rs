use crate::db::{
    api::{
        state::ops::{IntoDynNodeOp, NodeOp},
        view::{
            history::History,
            internal::{GraphView, NodeTimeSemanticsOps},
        },
    },
    graph::node::NodeView,
};
use raphtory_api::core::{entities::VID, storage::timeindex::EventTime};
use raphtory_storage::graph::graph::GraphStorage;
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub view: G,
}

impl<G: GraphView> NodeOp for EarliestTime<G> {
    type Output = Option<EventTime>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), &self.view)
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for EarliestTime<G> {}

#[derive(Debug, Clone)]
pub struct LatestTime<G> {
    pub(crate) view: G,
}

impl<G: GraphView> NodeOp for LatestTime<G> {
    type Output = Option<EventTime>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), &self.view)
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for LatestTime<G> {}

#[derive(Debug, Clone)]
pub struct HistoryOp<'graph, G> {
    pub(crate) graph: G,
    pub(crate) _phantom: PhantomData<&'graph G>,
}

impl<'graph, G> HistoryOp<'graph, G> {
    pub(crate) fn new(graph: G) -> Self {
        Self {
            graph,
            _phantom: PhantomData,
        }
    }
}

impl<'graph, G: GraphView + 'graph> NodeOp for HistoryOp<'graph, G> {
    type Output = History<'graph, NodeView<'graph, G>>;

    #[allow(unused_variables)]
    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        History::new(NodeView::new_internal(self.graph.clone(), node))
    }
}

// Couldn't implement NodeOpFilter for HistoryOp because the output type changes from History<NodeView<G>> to History<NodeView<GH>>.
// Instead, implemented OneHopFilter for LazyNodeState<HistoryOp> directly since the NodeOp<Output = Self::Output> bound isn't there.

#[derive(Debug, Copy, Clone)]
pub struct EdgeHistoryCount<G> {
    pub(crate) view: G,
}

impl<G: GraphView> NodeOp for EdgeHistoryCount<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        let ts = self.view.node_time_semantics();
        ts.node_edge_history_count(node.as_ref(), &self.view)
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for EdgeHistoryCount<G> {}
