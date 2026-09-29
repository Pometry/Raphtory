use crate::{
    db::{
        api::{
            state::{
                ops::{filter::OrOp, NodeFilterOp},
                NodeOp,
            },
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                edge_expr::ops::OrEdgeOp,
                expr::{FilterExpr, ToFilterExpr},
                ComposableFilter, DynFilter,
            },
            or_filtered_graph::OrFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: ToFilterExpr, R: ToFilterExpr> ToFilterExpr for OrFilter<L, R> {
    fn to_filter_expr(&self) -> FilterExpr {
        FilterExpr::Or(vec![
            self.left.to_filter_expr(),
            self.right.to_filter_expr(),
        ])
    }
}

impl<L: Display, R: Display> Display for OrFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for OrFilter<L, R> {}

/// The `or` of two compiled legs of one question's answer, as the tree
/// compiler builds it. A typed `or` compiles through its tree instead (see
/// `compile_through_tree!`).
impl CreateFilter for OrFilter<DynFilter, DynFilter> {
    type FilteredGraph<'graph, G>
        = OrFilteredGraph<G, DynGraphArc<'graph>, DynGraphArc<'graph>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = OrOp<Arc<dyn NodeOp<Output = bool> + 'graph>, Arc<dyn NodeOp<Output = bool> + 'graph>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = OrEdgeOp<DynEdgeFilter<'graph>, DynEdgeFilter<'graph>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let left = self.left.create_graph_filter(graph.clone())?;
        let right = self.right.create_graph_filter(graph.clone())?;
        Ok(OrFilteredGraph { graph, left, right })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph)?;
        Ok(left.or(right))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let left = self.left.create_edge_filter(graph.clone())?;
        let right = self.right.create_edge_filter(graph)?;
        Ok(OrEdgeOp { left, right })
    }
}
