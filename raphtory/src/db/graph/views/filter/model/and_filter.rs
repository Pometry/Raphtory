use crate::{
    db::{
        api::{
            state::{
                ops::{filter::AndOp, NodeFilterOp},
                NodeOp,
            },
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            model::{
                edge_expr::ops::AndEdgeOp,
                expr::{FilterExpr, ToFilterExpr},
                ComposableFilter, DynFilter,
            },
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: ToFilterExpr, R: ToFilterExpr> ToFilterExpr for AndFilter<L, R> {
    fn to_filter_expr(&self) -> FilterExpr {
        FilterExpr::And(vec![
            self.left.to_filter_expr(),
            self.right.to_filter_expr(),
        ])
    }
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for AndFilter<L, R> {}

/// The `and` of two erased filters, the join the tree compiler builds once it
/// has split a filter into its node and edge answers. A typed `and` compiles
/// through its tree instead (see `compile_through_tree!`), so it gets that split.
impl CreateFilter for AndFilter<DynFilter, DynFilter> {
    type FilteredGraph<'graph, G>
        = AndFilteredGraph<G, DynGraphArc<'graph>, DynGraphArc<'graph>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = AndOp<Arc<dyn NodeOp<Output = bool> + 'graph>, Arc<dyn NodeOp<Output = bool> + 'graph>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = AndEdgeOp<DynEdgeFilter<'graph>, DynEdgeFilter<'graph>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let left = self.left.create_graph_filter(graph.clone())?;
        let right = self.right.create_graph_filter(graph.clone())?;
        Ok(AndFilteredGraph::new(graph, left, right))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph)?;
        Ok(left.and(right))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let left = self.left.create_edge_filter(graph.clone())?;
        let right = self.right.create_edge_filter(graph)?;
        Ok(AndEdgeOp { left, right })
    }
}
