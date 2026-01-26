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
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EarliestTimeStruct {
    earliest_time: Option<i64>,
}

impl<G: GraphView> NodeOp for EarliestTime<G> {
    type Output = Option<EventTime>;
    type ArrowOutput = EarliestTimeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), &self.view)
    }

    // TODO(wyatt): fix this
    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        EarliestTimeStruct {
            earliest_time: self.apply(storage, node),
        }
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for EarliestTime<G> {}

#[derive(Debug, Clone)]
pub struct LatestTime<G> {
    pub(crate) view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LatestTimeStruct {
    latest_time: Option<i64>,
}

impl<G: GraphView> NodeOp for LatestTime<G> {
    type Output = Option<EventTime>;
    type ArrowOutput = LatestTimeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), &self.view)
    }

    // TODO(wyatt): fix this
    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        LatestTimeStruct {
            latest_time: self.apply(storage, node),
        }
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HistoryStruct {
    history: Vec<i64>,
}

impl<'graph, G: GraphView + 'graph> NodeOp for HistoryOp<'graph, G> {
    type Output = History<'graph, NodeView<'graph, G>>;
    type ArrowOutput = HistoryStruct;

    #[allow(unused_variables)]
    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        History::new(NodeView::new_internal(self.graph.clone(), node))
    }

    // TODO(wyatt): fix this
    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        HistoryStruct {
            history: self.apply(storage, node),
        }
    }
}

// Couldn't implement NodeOpFilter for HistoryOp because the output type changes from History<NodeView<G>> to History<NodeView<GH>>.
// Instead, implemented OneHopFilter for LazyNodeState<HistoryOp> directly since the NodeOp<Output = Self::Output> bound isn't there.

#[derive(Debug, Copy, Clone)]
pub struct EdgeHistoryCount<G> {
    pub(crate) view: G,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EdgeHistoryCountStruct {
    edge_history_count: usize,
}

impl<G: GraphView> NodeOp for EdgeHistoryCount<G> {
    type Output = usize;
    type ArrowOutput = EdgeHistoryCountStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        let ts = self.view.node_time_semantics();
        ts.node_edge_history_count(node.as_ref(), &self.view)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        EdgeHistoryCountStruct {
            edge_history_count: self.apply(storage, node),
        }
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for EdgeHistoryCount<G> {}
