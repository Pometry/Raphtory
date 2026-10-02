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
                answer::{all_of, compose, Answer, FilterAnswer, Question},
                edge_expr::ops::AndEdgeOp,
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

/// A leg that leaves a question open is dropped; the rest answer together.
impl<L: FilterAnswer, R: FilterAnswer> FilterAnswer for AndFilter<L, R> {
    fn answer(&self, question: Question, negated: bool) -> Result<Option<Answer>, GraphError> {
        all_of(
            [
                self.left.answer(question, negated),
                self.right.answer(question, negated),
            ],
            negated,
        )
    }
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for AndFilter<L, R> {}

/// A typed `and` compiles by answering the two questions over its legs.
impl<L, R> CreateFilter for AndFilter<L, R>
where
    L: FilterAnswer + Clone + Send + Sync + 'static,
    R: FilterAnswer + Clone + Send + Sync + 'static,
{
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
        compose(&self)?.create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        compose(&self)?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        compose(&self)?.create_edge_filter(graph)
    }
}

/// The `and` of two compiled filters, as the tree compiler builds it (the
/// node and edge answers, the legs of one answer, or the predicates beside a
/// view).
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
