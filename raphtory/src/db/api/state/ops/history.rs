use crate::{
    db::api::{
        state::NodeOp, storage::graph::storage_ops::GraphStorage, view::internal::OneHopFilter,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;

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

impl<'graph, G: GraphViewOps<'graph>> OneHopFilter<'graph> for EarliestTime<G> {
    type BaseGraph = G;
    type FilteredGraph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = EarliestTime<GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.graph
    }

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
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

impl<'graph, G: GraphViewOps<'graph>> OneHopFilter<'graph> for LatestTime<G> {
    type BaseGraph = G;
    type FilteredGraph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = LatestTime<GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.graph
    }

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
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
        self.graph.node_history(node)
    }
}

impl<'graph, G: GraphViewOps<'graph>> OneHopFilter<'graph> for History<G> {
    type BaseGraph = G;
    type FilteredGraph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = History<GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.graph
    }

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        History {
            graph: filtered_graph,
        }
    }
}
