use crate::db::api::view::{internal::NodeTimeSemanticsOps, BoxableGraphView};
use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelIterator,
};
use raphtory_storage::graph::nodes::node_ref::NodeStorageRef;

#[derive(Debug)]
pub enum FilterState {
    Neither,
    Both,
    BothIndependent,
    Nodes,
    Edges,
}

#[derive(
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    FusedIterator,
    ParallelIterator,
    IndexedParallelIterator,
)]
pub enum FilterVariants<Neither, Nodes, Edges, Both> {
    Neither(Neither),
    Nodes(Nodes),
    Edges(Edges),
    Both(Both),
}

pub trait FilterOps {
    fn filter_node(&self, node: NodeStorageRef) -> bool;
    fn filter_state(&self) -> FilterState;

    fn nodes_filtered(&self) -> bool;

    fn node_list_trusted(&self) -> bool;
}

impl<G: BoxableGraphView + Clone> FilterOps for G {
    #[inline]
    fn filter_node(&self, node: NodeStorageRef) -> bool {
        if self.nodes_filtered() {
            let time_semantics = self.node_time_semantics();
            self.internal_filter_node(node, self.layer_ids())
                && time_semantics.node_valid(node, self)
        } else {
            true
        }
    }

    #[inline]
    fn filter_state(&self) -> FilterState {
        match (self.internal_nodes_filtered(), self.edges_filtered()) {
            (false, false) => FilterState::Neither,
            (true, false) => FilterState::Nodes,
            (false, true) => {
                if self.edge_history_filtered() {
                    FilterState::BothIndependent
                } else {
                    FilterState::Edges
                }
            }
            (true, true) => {
                if self.edge_and_node_filter_independent() {
                    FilterState::BothIndependent
                } else {
                    FilterState::Both
                }
            }
        }
    }

    #[inline]
    fn nodes_filtered(&self) -> bool {
        self.internal_nodes_filtered() || self.edge_history_filtered()
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        self.internal_node_list_trusted() && !self.edge_history_filtered()
    }
}
