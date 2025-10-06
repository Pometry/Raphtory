use crate::{
    db::{api::view::internal::OneHopFilter, graph::views::filter::internal::CreateNodeFilter},
    errors::GraphError,
    prelude::GraphViewOps,
};

pub trait NodePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_nodes<F: CreateNodeFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::NodeFiltered<'graph, Self::FilteredGraph>>, GraphError> {
        Ok(self.one_hop_filtered(filter.create_node_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodePropertyFilterOps<'graph> for G {}
