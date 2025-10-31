use std::marker::PhantomData;

use crate::{
    db::{
        api::{
            state::{ops::NodeOpFilter, NodeOp},
            view::{history::History, internal::NodeTimeSemanticsOps},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::VID, storage::timeindex::EventTime};
use raphtory_storage::graph::graph::GraphStorage;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EarliestTime<G> {
    type Output = Option<EventTime>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), &self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for EarliestTime<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = EarliestTime<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        EarliestTime {
            graph: filtered_graph,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatestTime<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for LatestTime<G> {
    type Output = Option<EventTime>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), &self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for LatestTime<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = LatestTime<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        LatestTime {
            graph: filtered_graph,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistoryOp<'graph, G> {
    pub(crate) graph: G,
    pub(crate) _phantom: PhantomData<&'graph G>,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for HistoryOp<'graph, G> {
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
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EdgeHistoryCount<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        let ts = self.graph.node_time_semantics();
        ts.node_edge_history_count(node.as_ref(), &self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for EdgeHistoryCount<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph>> = EdgeHistoryCount<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph>>(&self, graph: GH) -> Self::Filtered<GH> {
        EdgeHistoryCount { graph }
    }
}
