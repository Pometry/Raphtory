use crate::db::api::view::{
    internal::{EdgeTimeSemanticsOps, NodeTimeSemanticsOps},
    BoxableGraphView,
};
use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelIterator,
};
use raphtory_storage::graph::{
    edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
    nodes::node_ref::NodeStorageRef,
};

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
    fn filters_independent(&self) -> bool;

    fn filtered(&self) -> bool;

    fn node_list_trusted(&self) -> bool;

    fn filter_edge(&self, edge: EdgeStorageRef) -> bool;

    fn edge_list_trusted(&self) -> bool;
}

impl<G: BoxableGraphView + Clone> FilterOps for G {
    #[inline]
    fn filter_node(&self, node: NodeStorageRef) -> bool {
        if self.filtered() {
            let time_semantics = self.node_time_semantics();
            self.internal_filter_node(node, self.layer_ids())
                && time_semantics.node_valid(node, self)
        } else {
            true
        }
    }

    #[inline]
    fn filter_state(&self) -> FilterState {
        match (
            self.internal_nodes_filtered(),
            self.internal_edge_filtered()
                || self.internal_edge_layer_filtered()
                || self.internal_exploded_edge_filtered(),
        ) {
            (false, false) => FilterState::Neither,
            (true, false) => FilterState::Nodes,
            (false, true) => FilterState::Edges,
            (true, true) => {
                if self.filters_independent() {
                    FilterState::BothIndependent
                } else {
                    FilterState::Both
                }
            }
        }
    }

    fn filters_independent(&self) -> bool {
        self.edge_filter_includes_node_filter()
            && self.node_filter_includes_edge_filter()
            && self.node_filter_includes_edge_layer_filter()
            && self.node_filter_includes_exploded_edge_filter()
    }

    #[inline]
    fn filtered(&self) -> bool {
        self.internal_nodes_filtered()
            || self.internal_edge_filtered()
            || self.internal_edge_layer_filtered()
            || self.internal_exploded_edge_filtered()
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        self.internal_node_list_trusted() && self.filters_independent()
    }

    fn filter_edge(&self, edge: EdgeStorageRef) -> bool {
        self.internal_filter_edge(edge, self.layer_ids()) && {
            let time_semantics = self.edge_time_semantics();
            edge.layer_ids_iter(self.layer_ids()).any(|layer_id| {
                self.internal_filter_edge_layer(edge, layer_id)
                    && time_semantics.include_edge(edge, self, layer_id)
            })
        }
    }

    fn edge_list_trusted(&self) -> bool {
        self.internal_edge_list_trusted() && self.filters_independent()
    }
}
