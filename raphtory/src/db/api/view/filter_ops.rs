use crate::{
    db::api::view::internal::{InternalFilter, InternalSelect},
    errors::GraphError,
};
use crate::db::graph::views::filter::CreateFilter;

pub trait Filter<'graph>: InternalFilter<'graph> {
    fn filter<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::EntityFiltered<'graph, Self::Graph>>, GraphError> {
        Ok(self.apply_filter(filter.create_filter(self.base_graph().clone())?))
    }
}

pub trait Select<'graph>: InternalSelect<'graph> {
    fn select<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F::EntityFiltered<'graph, Self::IterGraph>>, GraphError> {
        Ok(self.apply_iter_filter(filter.create_filter(self.iter_graph().clone())?))
    }
}

impl<'graph, T: InternalFilter<'graph>> Filter<'graph> for T {}
impl<'graph, T: InternalSelect<'graph>> Select<'graph> for T {}
