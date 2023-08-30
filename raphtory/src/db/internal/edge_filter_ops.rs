use crate::db::{
    api::view::internal::{EdgeFilter, EdgeFilterOps},
    graph::graph::InternalGraph,
};

impl EdgeFilterOps for InternalGraph {
    #[inline]
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        None
    }
}
