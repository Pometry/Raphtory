use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::{
        api::view::internal::{EdgeFilter, EdgeFilterOps},
        graph::graph::InternalGraph,
    },
};

impl EdgeFilterOps for InternalGraph {
    fn edge_filter(&self) -> Option<EdgeFilter> {
        None
    }
}
