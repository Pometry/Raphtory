use crate::db::{
    api::view::internal::{ArcEdgeFilter, EdgeFilterOps},
    graph::graph::InternalGraph,
};

impl EdgeFilterOps for InternalGraph {
    #[inline]
    fn edge_filter(&self) -> Option<ArcEdgeFilter> {
        None
    }
}
