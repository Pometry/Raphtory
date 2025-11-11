use std::sync::Arc;
use crate::{
    db::api::{state::NodeOp, view::internal::NodeTimeSemanticsOps},
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use crate::db::api::view::internal::GraphView;

#[derive(Debug, Clone)]
pub struct EarliestTime;

impl NodeOp for EarliestTime {
    type Output = Option<i64>;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_earliest_time(node.as_ref(), view)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> where Self: Sized {
        Arc::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct LatestTime;

impl NodeOp for LatestTime {
    type Output = Option<i64>;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = view.node_time_semantics();
        let node = storage.core_node(node);
        semantics.node_latest_time(node.as_ref(), view)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> where Self: Sized {
        Arc::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct History;

impl NodeOp for History {
    type Output = Vec<i64>;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        let semantics = view.node_time_semantics();
        let node = storage.core_node(node);
        semantics
            .node_history(node.as_ref(), view)
            .dedup()
            .collect()
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> where Self: Sized {
        Arc::new(self)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EdgeHistoryCount;

impl NodeOp for EdgeHistoryCount {
    type Output = usize;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        let ts = view.node_time_semantics();
        ts.node_edge_history_count(node.as_ref(), view)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> where Self: Sized {
        Arc::new(self)
    }
}
