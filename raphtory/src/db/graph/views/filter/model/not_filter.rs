use crate::{
    db::graph::views::filter::model::{
        edge_filter::{CompositeEdgeFilter, CompositeExplodedEdgeFilter},
        node_filter::CompositeNodeFilter,
        TryAsCompositeFilter,
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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for NotFilter<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Not(Box::new(
            self.0.try_as_composite_node_filter()?,
        )))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Not(Box::new(
            self.0.try_as_composite_edge_filter()?,
        )))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Not(Box::new(
            self.0.try_as_composite_exploded_edge_filter()?,
        )))
    }
}
