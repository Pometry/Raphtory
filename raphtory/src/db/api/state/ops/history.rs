use std::sync::Arc;
use crate::{
    db::api::{
        state::{ops::NodeOpFilter, NodeOp},
        storage::graph::storage_ops::GraphStorage,
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::core::{entities::VID, storage::timeindex::AsTime};
use crate::db::api::view::history::*;
use crate::db::graph::node::NodeView;

#[derive(Debug, Clone)]
// TODO: Change this definitely
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
pub struct HistoryOp<G> {
    pub(crate) graph: G,
    // _marker: std::marker::PhantomData<&'graph ()>,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for HistoryOp<G> {
    type Output = History<NodeView<G>>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        History::new(NodeView::new_internal(self.graph.clone(), node))
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for HistoryOp<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = HistoryOp<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        HistoryOp {
            graph: filtered_graph,
        }
    }
}
