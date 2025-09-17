use crate::{
    db::{
        api::view::internal::{BaseFilter, IterFilter},
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

pub trait IterFilterOps<'graph>: IterFilter<'graph> {
    fn filter_iter<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F::EntityFiltered<'graph, Self::IterGraph>>, GraphError> {
        Ok(self.apply_iter_filter(filter.create_filter(self.iter_graph().clone())?))
    }
}

impl<'graph, T: BaseFilter<'graph>> BaseFilterOps<'graph> for T {}
impl<'graph, T: IterFilter<'graph>> IterFilterOps<'graph> for T {}
