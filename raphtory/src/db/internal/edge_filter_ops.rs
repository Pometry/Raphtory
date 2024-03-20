use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::{
        api::view::internal::{EdgeFilter, EdgeFilterOps},
        graph::graph::InternalGraph,
    },
};

impl EdgeFilterOps for InternalGraph {
    fn edges_filtered(&self) -> bool {
        false
    }

    fn edge_list_trusted(&self) -> bool {
        true
    }

    fn filter_edge(&self, _edge: &EdgeStore, _layer_ids: &LayerIds) -> bool {
        true
    }
}
