use crate::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                answer::{compose, Answer, FilterAnswer, Question},
                ComposableFilter,
            },
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

/// The filter that keeps what `T` drops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFilter<T>(pub T);

impl<T: Display> Display for NotFilter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT({})", self.0)
    }
}

impl<T> ComposableFilter for NotFilter<T> {}

/// The opposite of the inner answer, pushed down to the leaves: `not` never
/// flips an answer the inner filter did not give.
impl<T: FilterAnswer> FilterAnswer for NotFilter<T> {
    fn answer(&self, question: Question, negated: bool) -> Result<Option<Answer>, GraphError> {
        self.0.answer(question, !negated)
    }
}

/// A typed `not` compiles by answering the two questions, negated, over its
/// inner filter: a negated node predicate is a node predicate and the same
/// entity rules apply either way round.
impl<T> CreateFilter for NotFilter<T>
where
    T: FilterAnswer + Clone + Send + Sync + 'static,
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
