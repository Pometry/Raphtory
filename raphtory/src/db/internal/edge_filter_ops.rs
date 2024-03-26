use crate::{
    core::entities::graph::tgraph::InnerTemporalGraph,
    db::api::view::internal::{EdgeFilter, EdgeFilterOps},
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::{api::view::internal::EdgeFilterOps, graph::graph::InternalGraph},
};

impl<const N: usize> EdgeFilterOps for InnerTemporalGraph<N> {
    fn edges_filtered(&self) -> bool {
        false
    }

    fn edge_list_trusted(&self) -> bool {
        true
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn filter_edge(&self, _edge: &EdgeStore, _layer_ids: &LayerIds) -> bool {
        true
    }
}
