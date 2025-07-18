use crate::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, AsEdgeFilter,
        AsNodeFilter, TryAsEdgeFilter, TryAsNodeFilter,
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for OrFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

impl<L: AsNodeFilter, R: AsNodeFilter> AsNodeFilter for OrFilter<L, R> {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Or(
            Box::new(self.left.as_node_filter()),
            Box::new(self.right.as_node_filter()),
        )
    }
}

impl<L: TryAsNodeFilter, R: TryAsNodeFilter> TryAsNodeFilter for OrFilter<L, R> {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Or(
            Box::new(self.left.try_as_node_filter()?),
            Box::new(self.right.try_as_node_filter()?),
        ))
    }
}

impl<L: AsEdgeFilter, R: AsEdgeFilter> AsEdgeFilter for OrFilter<L, R> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Or(
            Box::new(self.left.as_edge_filter()),
            Box::new(self.right.as_edge_filter()),
        )
    }
}

impl<L: TryAsEdgeFilter, R: TryAsEdgeFilter> TryAsEdgeFilter for OrFilter<L, R> {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Or(
            Box::new(self.left.try_as_edge_filter()?),
            Box::new(self.right.try_as_edge_filter()?),
        ))
    }
}
