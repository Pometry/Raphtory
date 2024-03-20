use crate::db::api::view::internal::{EdgeFilterOps, NodeFilterOps};

pub enum FilterState {
    Neither,
    Both,
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
            (true, true) => FilterState::Both,
        }
    }
}
