use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, Const, GraphView},
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph, model::edge_expr::ops::EdgeExistsOp,
            CreateFilter,
        },
    },
    errors::GraphError,
    python::graph::node_state::ops::filter::NO_FILTER,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GraphFilter;

impl std::fmt::Display for GraphFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRAPH")
    }
}

impl CreateFilter for GraphFilter {
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = NodeExistsOp<G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = EdgeExistsOp<G>
    where
        Self: 'graph,
        G: GraphView + 'graph;
    type PathEdgeFilter<'graph, G, S>
        = EdgeExprFilteredGraph<S, EdgeExistsOp<G>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        S: GraphView + 'graph;
    type PathNodeFilter<'graph, G, S>
        = Const<bool>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        S: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(graph))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(EdgeExistsOp::new(graph))
    }

    fn create_path_filter<'graph, G: GraphView + 'graph, S: GraphView + 'graph>(
        self,
        graph: G,
        select: S,
    ) -> Result<
        (
            Self::PathEdgeFilter<'graph, G, S>,
            Self::PathNodeFilter<'graph, G, S>,
        ),
        GraphError,
    > {
        // this is not a no-op, filters might have been applied to the path
        Ok((
            EdgeExprFilteredGraph::new(select, EdgeExistsOp::new(graph)),
            NO_FILTER,
        ))
    }
}
