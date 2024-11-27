use crate::{
    db::api::{
        state::{ops::NodeOpFilter, NodeOp},
        storage::graph::storage_ops::GraphStorage,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::VID, storage::timeindex::AsTime};

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EarliestTime<G> {
    type Output = Option<i64>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_earliest_time(node)
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
    type Output = Option<i64>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_latest_time(node)
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

impl<'graph, G: GraphViewOps<'graph>> NodeOp for History<G> {
    type Output = Vec<i64>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_history(node).map(|t| t.t()).collect()
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
