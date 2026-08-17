use crate::{
    db::{
        api::view::internal::{InternalFilter, InternalNodeSelect},
        graph::views::filter::CreateFilter,
    },
    errors::GraphError,
};

pub trait Filter<'graph>: InternalFilter<'graph> {
    fn filter<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<
        Self::Filtered<F::EntityFiltered<'graph, F::FilteredGraph<'graph, Self::Graph>>>,
        GraphError,
    > {
        let fg = filter.filter_graph_view(self.base_graph().clone())?;
        Ok(self.apply_filter(filter.create_filter(fg)?))
    }
}

pub trait Select<'graph>: 'graph {
    type IterFiltered<Filter: CreateFilter + 'graph>: Select<'graph>;
    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F>, GraphError>;
}

impl<'graph, T: InternalNodeSelect<'graph> + 'graph> Select<'graph> for T {
    type IterFiltered<Filter: CreateFilter + 'graph> =
        <T as InternalNodeSelect<'graph>>::IterFiltered<
            Filter::NodeFilter<'graph, Filter::FilteredGraph<'graph, T::IterGraph>>,
        >;

    fn select<F: CreateFilter>(&self, filter: F) -> Result<Self::IterFiltered<F>, GraphError> {
        let fg = filter.filter_graph_view(self.iter_graph().clone())?;
        Ok(self.apply_iter_filter(filter.create_node_filter(fg)?))
    }
}

impl<'graph, T: InternalFilter<'graph>> Filter<'graph> for T {}
