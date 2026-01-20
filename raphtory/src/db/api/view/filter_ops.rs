use crate::{
    db::{
        api::view::internal::{InternalEdgeSelect, InternalFilter, InternalNodeSelect},
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

pub trait NodeSelect<'graph>: InternalNodeSelect<'graph> {
    fn select<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<
        Self::IterFiltered<F::NodeFilter<'graph, F::FilteredGraph<'graph, Self::IterGraph>>>,
        GraphError,
    > {
        let fg = filter.filter_graph_view(self.iter_graph().clone())?;
        Ok(self.apply_iter_filter(filter.create_node_filter(fg)?))
    }
}

pub trait EdgeSelect<'graph>: InternalEdgeSelect<'graph> {
    fn select<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<
        Self::IterFiltered<F::EntityFiltered<'graph, F::FilteredGraph<'graph, Self::IterGraph>>>,
        GraphError,
    > {
        let fg = filter.filter_graph_view(self.iter_graph().clone())?;
        Ok(self.apply_iter_filter(filter.create_filter(fg)?))
    }
}

impl<'graph, T: InternalFilter<'graph>> Filter<'graph> for T {}
impl<'graph, T: InternalNodeSelect<'graph>> NodeSelect<'graph> for T {}
impl<'graph, T: InternalEdgeSelect<'graph>> EdgeSelect<'graph> for T {}
