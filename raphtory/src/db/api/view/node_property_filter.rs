use crate::core::utils::errors::GraphError;
use crate::db::api::view::internal::OneHopFilter;
use crate::db::graph::views::property_filter::internal::{InternalEdgeFilterOps, InternalNodePropertyFilterOps};
use crate::prelude::GraphViewOps;

pub trait NodePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_nodes<F: InternalNodePropertyFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::NodePropertyFiltered<'graph, Self::FilteredGraph>>, GraphError> {
        Ok(self.one_hop_filtered(filter.create_edge_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: OneHopFilter<'graph>> NodePropertyFilterOps<'graph> for G {}