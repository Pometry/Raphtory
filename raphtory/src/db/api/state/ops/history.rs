use crate::{
    db::api::{
        state::{ops::NodeOpFilter, NodeOp},
        view::internal::NodeTimeSemanticsOps,
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub(crate) graph: G,
}

#[derive(Serialize, Clone)]
pub struct EarliestTimeStruct {
    earliest_time: Option<i64>,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EarliestTime<G> {
    type Output = Option<i64>;
    type ArrowOutput = EarliestTimeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), &self.graph)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        EarliestTimeStruct {
            earliest_time: self.apply(storage, node),
        }
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

#[derive(Serialize, Clone)]
pub struct LatestTimeStruct {
    latest_time: Option<i64>,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for LatestTime<G> {
    type Output = Option<i64>;
    type ArrowOutput = LatestTimeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), &self.graph)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        LatestTimeStruct {
            latest_time: self.apply(storage, node),
        }
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
pub struct History<G> {
    pub(crate) graph: G,
}

#[derive(Serialize, Clone)]
pub struct HistoryStruct {
    history: Vec<i64>,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for History<G> {
    type Output = Vec<i64>;
    type ArrowOutput = HistoryStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics
            .node_history(node.as_ref(), &self.graph)
            .dedup()
            .collect()
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        HistoryStruct {
            history: self.apply(storage, node),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for History<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = History<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        History {
            graph: filtered_graph,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EdgeHistoryCount<G> {
    pub(crate) graph: G,
}

#[derive(Serialize, Clone)]
pub struct EdgeHistoryCountStruct {
    edge_history_count: usize,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EdgeHistoryCount<G> {
    type Output = usize;
    type ArrowOutput = EdgeHistoryCountStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        let ts = self.graph.node_time_semantics();
        ts.node_edge_history_count(node.as_ref(), &self.graph)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        EdgeHistoryCountStruct {
            edge_history_count: self.apply(storage, node),
        }
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
