use crate::db::api::{
    state::{ops::IntoDynNodeOp, NodeOp},
    view::internal::{GraphView, NodeTimeSemanticsOps},
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EarliestTime<G> {
    pub view: G,
}

impl<G: GraphView> NodeOp for EarliestTime<G> {
    type Output = Option<i64>;

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
    type Output = Option<i64>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), &self.view)
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for LatestTime<G> {}

#[derive(Debug, Clone)]
pub struct History<G> {
    pub(crate) view: G,
}

impl<G: GraphView> NodeOp for History<G> {
    type Output = Vec<i64>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = self.view.node_time_semantics();
        let node = storage.core_node(node);
        semantics
            .node_history(node.as_ref(), &self.view)
            .dedup()
            .collect()
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for History<G> {}

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
