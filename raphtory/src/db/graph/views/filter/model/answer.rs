//! The two questions a filter answers, and how combinations answer them.
//!
//! A filtered graph answers two questions, which nodes stay and which edges
//! stay, and every filter answers both: a node predicate answers the first
//! directly and the second by "both ends stayed", an edge predicate the
//! reverse. `and`, `or` and `not` combine the direct answers question by
//! question, so `name == "b" | name == "c"` keeps the edge b→c, `not` never
//! flips an answer the filter did not give, and the per-node and per-edge
//! forms of a filter agree with its graph.
//!
//! [`FilterAnswer`] is that rule as a trait. A predicate answers for its own
//! entity, and a filter tree implements it over its own nodes, so the rule
//! exists once and the tree is data, not the owner of the rule.

use crate::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                and_filter::AndFilter, edge_expr::ops::EdgeExistsOp, graph_filter::GraphFilter,
                or_filter::OrFilter, DynCreateFilter, EntityMarker,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use std::sync::Arc;

/// One of the two questions a filtered graph answers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Question {
    Nodes,
    Edges,
}

/// A compiled answer to one question: a filter over that question's entities only.
pub type Answer = Arc<dyn DynCreateFilter>;

/// How a filter answers each of the two questions.
pub trait FilterAnswer {
    /// This filter's direct answer to `question`, compiled; `None` when the
    /// filter leaves the question open. With `negated`, the opposite answer,
    /// pushed down to the leaves: a negated predicate is a predicate on the
    /// same entity, and a combination negates leg by leg.
    fn answer(&self, question: Question, negated: bool) -> Result<Option<Answer>, GraphError>;
}

/// The `and` of answers: a leg that leaves the question open is dropped, and
/// the rest combine. Negated, the legs are negated and combine with `or`.
pub(crate) fn all_of(
    legs: impl IntoIterator<Item = Result<Option<Answer>, GraphError>>,
    negated: bool,
) -> Result<Option<Answer>, GraphError> {
    // A leg that leaves the question open is dropped; the first error wins.
    let mut answers: Vec<Answer> = legs
        .into_iter()
        .filter_map(Result::transpose)
        .collect::<Result<_, _>>()?;
    Ok(match answers.len() {
        0 => None,
        1 => answers.pop(),
        _ if negated => Some(combine(answers.into_iter().map(Ok), "or", or_of)?),
        _ => Some(combine(answers.into_iter().map(Ok), "and", and_of)?),
    })
}

/// The `or` of answers: every leg must answer, or the question stays open.
/// Negated, the legs are negated and combine with `and`.
pub(crate) fn any_of(
    legs: impl IntoIterator<Item = Result<Option<Answer>, GraphError>>,
    negated: bool,
) -> Result<Option<Answer>, GraphError> {
    let Some(mut answers) = legs.into_iter().collect::<Result<Option<Vec<_>>, _>>()? else {
        return Ok(None);
    };
    Ok(Some(if answers.len() == 1 {
        answers.pop().unwrap()
    } else if negated {
        combine(answers.into_iter().map(Ok), "and", and_of)?
    } else {
        combine(answers.into_iter().map(Ok), "or", or_of)?
    }))
}

fn and_of(left: Answer, right: Answer) -> Answer {
    Arc::new(AndFilter { left, right })
}

fn or_of(left: Answer, right: Answer) -> Answer {
    Arc::new(OrFilter { left, right })
}

/// Fold compiled operands pairwise, left to right. An empty list has no
/// meaning either way (`and` of nothing is not "everything", `or` of nothing
/// is not "nothing" the caller asked for), so it is refused.
pub(crate) fn needs_operand(name: &str) -> GraphError {
    GraphError::InvalidFilter(format!("`{name}` needs at least one operand"))
}

pub(crate) fn combine(
    mut compiled: impl Iterator<Item = Result<Answer, GraphError>>,
    name: &str,
    join: impl Fn(Answer, Answer) -> Answer,
) -> Result<Answer, GraphError> {
    let first = compiled.next().ok_or_else(|| needs_operand(name))??;
    compiled.try_fold(first, |acc, next| Ok(join(acc, next?)))
}

/// The filter a whole answer to both questions is: the node answer and the
/// edge answer side by side, one of them alone, or the unfiltered graph when
/// nothing constrains either.
pub fn compose<F: FilterAnswer + ?Sized>(filter: &F) -> Result<Answer, GraphError> {
    let nodes = filter
        .answer(Question::Nodes, false)?
        .map(|nodes| Arc::new(NodeAnswer(nodes)) as Answer);
    let edges = filter.answer(Question::Edges, false)?;
    Ok(match (nodes, edges) {
        (Some(nodes), Some(edges)) => and_of(nodes, edges),
        (Some(answer), None) | (None, Some(answer)) => answer,
        (None, None) => Arc::new(GraphFilter),
    })
}

/// The node answer as a filter: the nodes its node test keeps, and the edges
/// whose ends it keeps both. The legs of a combined node answer each keep
/// their own edges, and an `or` of those would drop an edge whose ends pass
/// different legs, so the answer's edges come from its node test instead.
#[derive(Clone)]
struct NodeAnswer(Answer);

impl CreateFilter for NodeAnswer {
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
        self.0.create_dyn_graph_filter(graph.into_dyn_graph_arc())
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.0.create_dyn_node_filter(graph.into_dyn_graph_arc())
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let nodes = self
            .0
            .create_dyn_node_filter(graph.clone().into_dyn_graph_arc())?;
        Ok(Arc::new(EdgeExistsOp::new(NodeFilteredGraph::new(
            graph, nodes,
        ))))
    }
}

/// A yes/no value's own question: a node value answers for nodes, an edge or
/// exploded-edge value for edges, and a constant for neither.
pub(crate) fn question_of(entity: EntityMarker) -> Result<Question, GraphError> {
    match entity {
        EntityMarker::Node => Ok(Question::Nodes),
        EntityMarker::Edge | EntityMarker::ExplodedEdge => Ok(Question::Edges),
        EntityMarker::Const => Err(GraphError::InvalidFilter(
            "a constant is not a filter".to_owned(),
        )),
    }
}
