use crate::{
    db::{
        api::view::internal::{BaseFilter, OneHopFilter},
        graph::views::filter::internal::CreateFilter,
    },
    errors::GraphError,
};

pub trait BaseFilterOps<'graph>: BaseFilter<'graph> {
    fn filter<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::EntityFiltered<'graph, Self::BaseGraph>>, GraphError> {
        Ok(self.apply_filter(filter.create_filter(self.base_graph().clone())?))
    }
}

pub trait IterFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_iter<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::OneHopFiltered<F::EntityFiltered<'graph, Self::OneHopGraph>>, GraphError>
    {
        Ok(self.apply_one_hop_filter(filter.create_filter(self.one_hop_graph().clone())?))
    }
}

impl<'graph, T: BaseFilter<'graph>> BaseFilterOps<'graph> for T {}
impl<'graph, T: OneHopFilter<'graph>> IterFilterOps<'graph> for T {}
