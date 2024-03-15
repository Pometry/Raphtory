use crate::{core::entities::graph::tgraph::InnerTemporalGraph, db::api::view::internal::{EdgeFilter, EdgeFilterOps}};

impl<const N: usize> EdgeFilterOps for InnerTemporalGraph<N> {
    #[inline]
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        None
    }
}
