use crate::{
    db::{api::view::internal::OneHopFilter, graph::views::filter::internal::CreateEdgeFilter},
    errors::GraphError,
    prelude::GraphViewOps,
};

pub trait EdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_edges<F: CreateEdgeFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::EdgeFiltered<'graph, Self::FilteredGraph>>, GraphError> {
        Ok(self.one_hop_filtered(filter.create_edge_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph> for G {}
