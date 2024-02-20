use crate::db::api::view::internal::{EdgeFilter, EdgeFilterOps};

use super::Graph2;

impl EdgeFilterOps for Graph2 {
    #[doc = " Return the optional edge filter for the graph"]
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        None
    }
}
