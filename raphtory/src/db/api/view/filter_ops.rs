use crate::{
    db::{
        api::view::internal::{Filter, IterFilter},
        graph::views::filter::internal::CreateFilter,
    },
    errors::GraphError,
};

pub trait BaseFilterOps<'graph>: Filter<'graph> {
    fn filter<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::EntityFiltered<'graph, Self::Graph>>, GraphError> {
        Ok(self.apply_filter(filter.create_filter(self.base_graph().clone())?))
    }
}

pub trait IterFilterOps<'graph>: IterFilter<'graph> {
    fn select<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F::EntityFiltered<'graph, Self::IterGraph>>, GraphError> {
        Ok(self.apply_iter_filter(filter.create_filter(self.iter_graph().clone())?))
    }
}

impl<'graph, T: Filter<'graph>> BaseFilterOps<'graph> for T {}
impl<'graph, T: IterFilter<'graph>> IterFilterOps<'graph> for T {}
