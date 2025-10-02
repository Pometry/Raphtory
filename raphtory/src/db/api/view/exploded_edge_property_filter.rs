use crate::{
    db::{
        api::view::internal::OneHopFilter, graph::views::filter::internal::CreateExplodedEdgeFilter,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};

pub trait ExplodedEdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_exploded_edges<F: CreateExplodedEdgeFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::ExplodedEdgeFiltered<'graph, Self::FilteredGraph>>, GraphError>
    {
        let graph = filter.create_exploded_edge_filter(self.current_filter().clone())?;
        Ok(self.one_hop_filtered(graph))
    }
}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilterOps<'graph> for G {}
