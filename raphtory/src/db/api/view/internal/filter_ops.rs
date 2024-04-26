use crate::db::api::view::internal::{EdgeFilterOps, NodeFilterOps};

pub enum FilterState {
    Neither,
    Both,
    BothIndependent,
    Nodes,
    Edges,
}

pub trait FilterOps {
    fn filter_state(&self) -> FilterState;
}

impl<G: NodeFilterOps + EdgeFilterOps> FilterOps for G {
    #[inline]
    fn filter_state(&self) -> FilterState {
        match (self.nodes_filtered(), self.edges_filtered()) {
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
}
