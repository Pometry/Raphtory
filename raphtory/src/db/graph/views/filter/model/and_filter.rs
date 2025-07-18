use crate::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, AsEdgeFilter,
        AsNodeFilter, TryAsEdgeFilter, TryAsNodeFilter,
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L: AsNodeFilter, R: AsNodeFilter> AsNodeFilter for AndFilter<L, R> {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::And(
            Box::new(self.left.as_node_filter()),
            Box::new(self.right.as_node_filter()),
        )
    }
}

impl<L: TryAsNodeFilter, R: TryAsNodeFilter> TryAsNodeFilter for AndFilter<L, R> {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::And(
            Box::new(self.left.try_as_node_filter()?),
            Box::new(self.right.try_as_node_filter()?),
        ))
    }
}

impl<L: AsEdgeFilter, R: AsEdgeFilter> AsEdgeFilter for AndFilter<L, R> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::And(
            Box::new(self.left.as_edge_filter()),
            Box::new(self.right.as_edge_filter()),
        )
    }
}

impl<L: TryAsEdgeFilter, R: TryAsEdgeFilter> TryAsEdgeFilter for AndFilter<L, R> {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::And(
            Box::new(self.left.try_as_edge_filter()?),
            Box::new(self.right.try_as_edge_filter()?),
        ))
    }
}
