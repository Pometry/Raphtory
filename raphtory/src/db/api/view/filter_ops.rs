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
    ) -> Result<Self::Filtered<F::EntityFiltered<'graph, Self::Graph>>, GraphError> {
        Ok(self.apply_filter(filter.create_filter(self.base_graph().clone())?))
    }
}

pub trait NodeSelect<'graph>: InternalNodeSelect<'graph> {
    fn select<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F::NodeFilter<'graph, Self::IterGraph>>, GraphError> {
        Ok(self.apply_iter_filter(filter.create_node_filter(self.iter_graph().clone())?))
    }
}

pub trait EdgeSelect<'graph>: InternalEdgeSelect<'graph> {
    fn select<F: CreateFilter>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F::EntityFiltered<'graph, Self::IterGraph>>, GraphError> {
        Ok(self.apply_iter_filter(filter.create_filter(self.iter_graph().clone())?))
    }
}

impl<'graph, T: InternalFilter<'graph>> Filter<'graph> for T {}
impl<'graph, T: InternalNodeSelect<'graph>> NodeSelect<'graph> for T {}
impl<'graph, T: InternalEdgeSelect<'graph>> EdgeSelect<'graph> for T {}
