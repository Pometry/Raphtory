use crate::db::{
    api::{
        state::ops::{ArrowNodeOp, IntoDynNodeOp, NodeOp},
        view::{
            history::History,
            internal::{GraphView, NodeTimeSemanticsOps},
        },
    },
    graph::node::NodeView,
};
use raphtory_api::core::{entities::VID, storage::timeindex::EventTime};
use raphtory_storage::graph::graph::GraphStorage;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EarliestTimeStruct {
    earliest_time: Option<EventTime>,
}
impl From<Option<EventTime>> for EarliestTimeStruct {
    fn from(earliest_time: Option<EventTime>) -> Self {
        EarliestTimeStruct { earliest_time }
    }
}

impl<G: GraphView> NodeOp for EarliestTime<G> {
    type Output = Option<EventTime>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), &self.view)
    }
}

impl<G: GraphView> ArrowNodeOp for EarliestTime<G> {
    type ArrowOutput = EarliestTimeStruct;
}

impl<G: GraphView + 'static> IntoDynNodeOp for EarliestTime<G> {}

#[derive(Debug, Clone)]
pub struct LatestTime<G> {
    pub(crate) view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LatestTimeStruct {
    latest_time: Option<EventTime>,
}
impl From<Option<EventTime>> for LatestTimeStruct {
    fn from(latest_time: Option<EventTime>) -> Self {
        LatestTimeStruct { latest_time }
    }
}

impl<G: GraphView> NodeOp for LatestTime<G> {
    type Output = Option<EventTime>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), &self.view)
    }
}
impl<G: GraphView> ArrowNodeOp for LatestTime<G> {
    type ArrowOutput = LatestTimeStruct;
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HistoryStruct {
    history: Vec<EventTime>,
}
impl<'graph, G: GraphView + 'graph> From<History<'graph, NodeView<'graph, G>>> for HistoryStruct {
    fn from(history: History<'graph, NodeView<'graph, G>>) -> Self {
        HistoryStruct {
            history: history.collect(),
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

impl<'graph, G: GraphView + 'graph> ArrowNodeOp for HistoryOp<'graph, G> {
    type ArrowOutput = HistoryStruct;
}

// Couldn't implement NodeOpFilter for HistoryOp because the output type changes from History<NodeView<G>> to History<NodeView<GH>>.
// Instead, implemented OneHopFilter for LazyNodeState<HistoryOp> directly since the NodeOp<Output = Self::Output> bound isn't there.

#[derive(Debug, Copy, Clone)]
pub struct EdgeHistoryCount<G> {
    pub(crate) view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EdgeHistoryCountStruct {
    edge_history_count: usize,
}
impl From<usize> for EdgeHistoryCountStruct {
    fn from(edge_history_count: usize) -> Self {
        EdgeHistoryCountStruct { edge_history_count }
    }
}

impl<G: GraphView> NodeOp for EdgeHistoryCount<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        let ts = self.view.node_time_semantics();
        ts.node_edge_history_count(node.as_ref(), &self.view)
    }
}

impl<G: GraphView> ArrowNodeOp for EdgeHistoryCount<G> {
    type ArrowOutput = EdgeHistoryCountStruct;
}

impl<G: GraphView + 'static> IntoDynNodeOp for EdgeHistoryCount<G> {}
