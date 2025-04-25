use crate::db::api::{
    storage::graph::nodes::node_ref::NodeStorageRef,
    view::{internal::NodeTimeSemanticsOps, BoxableGraphView},
};

pub enum FilterState {
    Neither,
    Both,
    BothIndependent,
    Nodes,
    Edges,
}

pub trait FilterOps {
    fn filter_node(&self, node: NodeStorageRef) -> bool;
    fn filter_state(&self) -> FilterState;
    
    fn nodes_filtered(&self) -> bool;
}

impl<G: BoxableGraphView + Clone> FilterOps for G {
    fn filter_node(&self, node: NodeStorageRef) -> bool {
        let mut res = true;
        if self.edge_history_filtered() {
            res = res && {
                let time_semantics = self.node_time_semantics();
                time_semantics.node_valid(node, self)
            }
        }
        if self.internal_nodes_filtered() {
            res = res && { self.internal_filter_node(node, self.layer_ids()) }
        }
        res
    }

    #[inline]
    fn filter_state(&self) -> FilterState {
        match (
            self.internal_nodes_filtered() || self.edge_history_filtered(),
            self.edges_filtered(),
        ) {
            (false, false) => FilterState::Neither,
            (true, false) => FilterState::Nodes,
            (false, true) => FilterState::Edges,
            (true, true) => {
                if self.edge_filter_includes_node_filter() {
                    FilterState::BothIndependent
                } else {
                    FilterState::Both
                }
            }
        }
    }

    fn nodes_filtered(&self) -> bool {
        self.internal_nodes_filtered() || self.edge_history_filtered()
    }
}
