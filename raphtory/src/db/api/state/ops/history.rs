use chrono::{DateTime, Utc};
use crate::{
    db::{
        api::{
            state::{ops::NodeOpFilter, NodeOp},
            storage::graph::storage_ops::GraphStorage,
            view::{internal::NodeTimeSemanticsOps, history::History},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::core::{entities::VID, storage::timeindex::AsTime};
use raphtory_api::core::storage::timeindex::TimeError;
use raphtory_storage::graph::graph::GraphStorage;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for EarliestTime<G> {
    type Output = Option<i64>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), &self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> EarliestTime<G> {
    pub fn dt(self) -> AsDateTime<EarliestTime<G>> {
        AsDateTime{op: self}
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

impl<'graph, G: GraphViewOps<'graph>> LatestTime<G> {
    pub fn dt(self) -> AsDateTime<LatestTime<G>> {
        AsDateTime{op: self}
    }
}

#[derive(Debug, Clone)]
pub struct AsDateTime<Op> {
    pub(crate) op: Op,
}

impl<Op: NodeOp<Output=Option<i64>>> NodeOp for AsDateTime<Op> {
    type Output = Result<Option<DateTime<Utc>>, TimeError>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.op.apply(storage, node).map(|t| t.dt()).transpose()
    }
}

impl<'graph, Op: NodeOpFilter<'graph>> NodeOpFilter<'graph> for AsDateTime<Op> {
    type Graph = Op::Graph;
    type Filtered<G: GraphViewOps<'graph>> = AsDateTime<Op::Filtered<G>>;

    fn graph(&self) -> &Self::Graph {
        self.op.graph()
    }

    fn filtered<G: GraphViewOps<'graph>>(&self, graph: G) -> Self::Filtered<G> {
        AsDateTime{op: self.op.filtered(graph)}
    }
}

#[derive(Debug, Clone)]
pub struct HistoryOp<G> {
    pub(crate) graph: G,
    // _marker: std::marker::PhantomData<&'graph ()>,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for History<G> {
    type Output = Vec<i64>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.graph.node_time_semantics();
        let node = storage.core_node(node);
        semantics
            .node_history(node.as_ref(), &self.graph)
            .dedup()
            .collect()
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
