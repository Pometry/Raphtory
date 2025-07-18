use crate::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, AsEdgeFilter,
        AsNodeFilter, TryAsEdgeFilter, TryAsNodeFilter,
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFilter<T>(pub(crate) T);

impl<T: Display> Display for NotFilter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT({})", self.0)
    }
}

impl<T: AsNodeFilter> AsNodeFilter for NotFilter<T> {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Not(Box::new(self.0.as_node_filter()))
    }
}

impl<T: TryAsNodeFilter> TryAsNodeFilter for NotFilter<T> {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Not(Box::new(
            self.0.try_as_node_filter()?,
        )))
    }
}

impl<T: AsEdgeFilter> AsEdgeFilter for NotFilter<T> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Not(Box::new(self.0.as_edge_filter()))
    }
}

impl<T: TryAsEdgeFilter> TryAsEdgeFilter for NotFilter<T> {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Not(Box::new(
            self.0.try_as_edge_filter()?,
        )))
    }
}
