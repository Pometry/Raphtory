use crate::db::api::view::history::*;
use crate::db::api::view::TimeIndex;
use crate::db::graph::node::NodeView;
use crate::{
    db::api::{
        state::{ops::NodeOpFilter, NodeOp},
        storage::graph::storage_ops::GraphStorage,
    },
    prelude::GraphViewOps,
};
use chrono::Utc;
use itertools::Itertools;
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use raphtory_api::core::{entities::VID, storage::timeindex::AsTime};
use std::sync::Arc;

#[derive(Debug, Clone)]
// TODO: Change this definitely
pub struct EarliestTimestamp<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EarliestTimestamp<G> {
    type Output = Option<i64>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_earliest_time(node).map(|t| t.t())
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for EarliestTimestamp<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = EarliestTimestamp<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        EarliestTimestamp {
            graph: filtered_graph,
        }
    }
}

#[derive(Debug, Clone)]
// TODO: Change this definitely
pub struct EarliestDateTime<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EarliestDateTime<G> {
    type Output = Option<chrono::DateTime<Utc>>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_earliest_time(node).map(|t| t.dt())?
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for EarliestDateTime<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = EarliestDateTime<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        EarliestDateTime {
            graph: filtered_graph,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatestTimestamp<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for LatestTimestamp<G> {
    type Output = Option<i64>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_latest_time(node).map(|t| t.t())
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for LatestTimestamp<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = LatestTimestamp<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        LatestTimestamp {
            graph: filtered_graph,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatestDateTime<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for LatestDateTime<G> {
    type Output = Option<chrono::DateTime<Utc>>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.node_latest_time(node).map(|t| t.dt())?
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for LatestDateTime<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = LatestDateTime<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        LatestDateTime {
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
