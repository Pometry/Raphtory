use crate::db::api::view::internal::{EdgeFilter, EdgeFilterOps};

use super::ArrowGraph;

impl EdgeFilterOps for ArrowGraph {
    #[doc = " Return the optional edge filter for the graph"]
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        None
    }
}
