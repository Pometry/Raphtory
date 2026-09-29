use crate::{
    db::{
        api::{
            state::{ops::NodeFilterOp, NodeOp},
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                edge_expr::ops::EdgeExistsOp,
                expr::{FilterExpr, ToFilterExpr},
                ComposableFilter, DynCreateFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

/// The filter that keeps what `T` drops.
///
/// A typed filter negates through its tree: the compiler pushes the `not`
/// down to the entity predicates, so a negated node predicate is a node
/// predicate and the same entity rules apply either way round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFilter<T>(pub T);

impl<T: Display> Display for NotFilter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT({})", self.0)
    }
}

impl<T> ComposableFilter for NotFilter<T> {}

impl<T: ToFilterExpr> ToFilterExpr for NotFilter<T> {
    fn to_filter_expr(&self) -> FilterExpr {
        FilterExpr::Not(Box::new(self.0.to_filter_expr()))
    }
}

/// An erased filter over in-process node state has no tree to push the `not`
/// through; it is a node predicate, so its negation is the negated node op.
impl CreateFilter for NotFilter<Arc<dyn DynCreateFilter>> {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(Arc::new(NodeFilteredGraph::new(graph, filter)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(Arc::new(
            self.0
                .create_dyn_node_filter(graph.into_dyn_graph_arc())?
                .not(),
        ))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(Arc::new(EdgeExistsOp::new(
            self.create_graph_filter(graph)?,
        )))
    }
}
